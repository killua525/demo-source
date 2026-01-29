package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/olivere/elastic/v7"
)

// --- 配置对象 ---
type Config struct {
	MySQLDSN string
	ESUrl    string
	Total    int
	Batch    int
}

// --- 实体对象 ---
type Order struct {
	OrderID    string  `json:"order_id"`
	CustomerID string  `json:"customer_id"`
	Amount     float64 `json:"amount"`
	CreateTime string  `json:"create_time"`
}

var cfg Config

func init() {
	// 命令行参数解析
	flag.StringVar(&cfg.MySQLDSN, "mysql", "root:123456@tcp(127.0.0.1:3306)/test_db?charset=utf8mb4&parseTime=True", "MySQL连接串")
	flag.StringVar(&cfg.ESUrl, "es", "http://127.0.0.1:9200", "ES地址")
	// 默认跑 20万数据做演示，正式跑可以指定 -total 20000000
	flag.IntVar(&cfg.Total, "total", 200000, "总数据量")
	flag.IntVar(&cfg.Batch, "batch", 2000, "批量插入的大小")
	flag.Parse()
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// 1. 初始化 MySQL
	db, err := sql.Open("mysql", cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("MySQL connect failed: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)

	// 2. 初始化 ES
	esClient, err := elastic.NewClient(
		elastic.SetURL(cfg.ESUrl),
		elastic.SetSniff(false), // 单节点建议关闭
	)
	if err != nil {
		log.Fatalf("ES connect failed: %v", err)
	}

	// 3. 初始化 Schema (表结构 + 索引配置)
	initSchema(db, esClient)

	// 4. 数据加载 (Producer-Consumer 模型)
	fmt.Printf(">>> [Phase 1] 开始加载 %d 条数据...\n", cfg.Total)
	start := time.Now()
	loadData(db, esClient)
	fmt.Printf(">>> 数据加载完成，耗时: %v\n\n", time.Since(start))

	// 5. 开始基准测试
	// 测试不同数据规模下的性能
	queryLevels := []int{10000, 100000}
	if cfg.Total >= 1000000 {
		queryLevels = append(queryLevels, 1000000)
	}
	// 如果总数够大，最后测试全量
	if cfg.Total > 1000000 {
		queryLevels = append(queryLevels, cfg.Total)
	}

	fmt.Println(">>> [Phase 2] 开始查询性能测试...")
	for _, limit := range queryLevels {
		fmt.Printf("\n--- 测试数据规模: %d ---\n", limit)

		// 场景 A: MySQL 查询 + 应用层求和
		benchmarkMySQL(db, limit)

		// 场景 B: ES 原生聚合 (Scaled Float) - 推荐
		// 注意：聚合通常针对全量或Query过滤后的数据，这里我们用 MatchAll 模拟全量
		if limit == cfg.Total {
			benchmarkESNativeAgg(esClient)
		}

		// 场景 C: ES 脚本聚合 (BigDecimal) - 特殊需求
		// 注意: 这个功能需要 ES 支持 script_metric aggregation
		// 如果编译失败可以注释掉
		// if limit == cfg.Total {
		// 	benchmarkESScriptAgg(esClient)
		// }
	}
}

// --- 初始化逻辑 ---
func initSchema(db *sql.DB, es *elastic.Client) {
	// MySQL DDL: 使用 DECIMAL 保证金额精确
	_, err := db.Exec(`CREATE TABLE IF NOT EXISTS customer_orders (
		order_id VARCHAR(64) PRIMARY KEY,
		customer_id VARCHAR(64),
		amount DECIMAL(10, 2),
		create_time DATETIME,
		KEY idx_amt (amount)
	)`)
	if err != nil {
		log.Fatalf("MySQL init table failed: %v", err)
	}
	// db.Exec("TRUNCATE TABLE customer_orders") // 可选：清空旧数据

	// ES Mapping: 
	// 1. 设置 max_result_window 为 1000w (虽然聚合不需要这个，但为了满足你的要求)
	// 2. 设置 amount 为 scaled_float，因子 100
	mapping := `{
		"settings": {
			"number_of_shards": 3,
			"number_of_replicas": 0,
			"max_result_window": 10000000
		},
		"mappings": {
			"properties": {
				"order_id": { "type": "keyword" },
				"customer_id": { "type": "keyword" },
				"amount": { "type": "scaled_float", "scaling_factor": 100 },
				"create_time": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" }
			}
		}
	}`
	ctx := context.Background()
	exists, _ := es.IndexExists("customer_orders").Do(ctx)
	if exists {
		es.DeleteIndex("customer_orders").Do(ctx)
	}
	_, err = es.CreateIndex("customer_orders").BodyString(mapping).Do(ctx)
	if err != nil {
		log.Fatalf("ES create index failed: %v", err)
	}
	fmt.Println(">>> Schema 初始化完毕 (MySQL Table + ES Index with max_result_window=1000w)")
}

// --- 数据加载 (并发) ---
func loadData(db *sql.DB, es *elastic.Client) {
	var wg sync.WaitGroup
	// 缓冲通道，防止内存溢出
	dataChan := make(chan []Order, 100)

	// 生产者 Goroutine
	go func() {
		defer close(dataChan)
		batchData := make([]Order, 0, cfg.Batch)
		for i := 0; i < cfg.Total; i++ {
			order := Order{
				OrderID:    fmt.Sprintf("ORD-%d-%d", time.Now().UnixNano(), i),
				CustomerID: fmt.Sprintf("CUST-%d", rand.Intn(100000)),
				Amount:     float64(rand.Intn(100000)) / 100.0, // 随机金额
				CreateTime: time.Now().Format("2006-01-02 15:04:05"),
			}
			batchData = append(batchData, order)
			if len(batchData) >= cfg.Batch {
				// Copy data to avoid race condition on slice reuse
				tmp := make([]Order, len(batchData))
				copy(tmp, batchData)
				dataChan <- tmp
				batchData = make([]Order, 0, cfg.Batch)
			}
		}
		if len(batchData) > 0 {
			dataChan <- batchData
		}
	}()

	// 消费者 Goroutines (5个并发)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for batch := range dataChan {
				writeMySQL(db, batch)
				writeES(es, batch)
			}
		}()
	}
	wg.Wait()
	
	// 强制刷新 ES，确保数据立即可查
	es.Refresh("customer_orders").Do(context.Background())
}

func writeMySQL(db *sql.DB, orders []Order) {
	if len(orders) == 0 { return }
	sqlStr := "INSERT INTO customer_orders (order_id, customer_id, amount, create_time) VALUES "
	vals := []interface{}{}
	placeholders := make([]string, 0, len(orders))
	for _, o := range orders {
		placeholders = append(placeholders, "(?, ?, ?, ?)")
		vals = append(vals, o.OrderID, o.CustomerID, o.Amount, o.CreateTime)
	}
	sqlStr += strings.Join(placeholders, ",")
	_, err := db.Exec(sqlStr, vals...)
	if err != nil { log.Printf("MySQL Write Error: %v", err) }
}

func writeES(es *elastic.Client, orders []Order) {
	if len(orders) == 0 { return }
	bulk := es.Bulk().Index("customer_orders")
	for _, o := range orders {
		bulk.Add(elastic.NewBulkIndexRequest().Doc(o))
	}
	_, err := bulk.Do(context.Background())
	if err != nil { log.Printf("ES Write Error: %v", err) }
}

// --- 基准测试: MySQL ---
func benchmarkMySQL(db *sql.DB, limit int) {
	start := time.Now()
	// 模拟应用层拉取数据求和
	rows, err := db.Query("SELECT amount FROM customer_orders LIMIT ?", limit)
	if err != nil {
		log.Printf("MySQL Query Error: %v", err)
		return
	}
	defer rows.Close()

	var sum float64
	var amount float64
	for rows.Next() {
		rows.Scan(&amount)
		sum += amount
	}
	fmt.Printf("[MySQL ] Limit=%-8d | Type=RowScan | Time=%-10v | Sum=%.2f\n", limit, time.Since(start), sum)
}

// --- 基准测试: ES 原生聚合 (Scaled Float) ---
func benchmarkESNativeAgg(es *elastic.Client) {
	start := time.Now()
	ctx := context.Background()

	// Sum Aggregation: 极其高效，使用 Doc Values
	sumAgg := elastic.NewSumAggregation().Field("amount")

	res, err := es.Search().
		Index("customer_orders").
		Query(elastic.NewMatchAllQuery()).
		Size(0). // 不返回 Hits
		Aggregation("total_amount", sumAgg).
		Do(ctx)

	if err != nil {
		log.Printf("ES Native Agg Error: %v", err)
		return
	}

	aggRes, _ := res.Aggregations.Sum("total_amount")
	fmt.Printf("[ES    ] Limit=ALL      | Type=Native  | Time=%-10v | Sum=%.2f (Scaled Float)\n", time.Since(start), *aggRes.Value)
}

// --- 基准测试: ES 脚本聚合 (BigDecimal) - 暂时禁用 ---
/*
func benchmarkESScriptAgg(es *elastic.Client) {
	start := time.Now()
	ctx := context.Background()

	// Painless Script for BigDecimal
	// Init: 初始化状态
	initScript := elastic.NewScript("state.total = BigDecimal.ZERO")
	// Map: 遍历文档 (doc['amount'].value 取出的是已经除以因子后的浮点值)
	mapScript := elastic.NewScript("state.total = state.total.add(BigDecimal.valueOf(doc['amount'].value))")
	// Combine: 分片内汇总
	combineScript := elastic.NewScript("return state.total")
	// Reduce: 跨分片汇总
	reduceScript := elastic.NewScript(`
		BigDecimal sum = BigDecimal.ZERO; 
		for (s in states) { if (s != null) { sum = sum.add(s); } } 
		return sum
	`)

	scriptAgg := elastic.NewScriptMetricAggregation().
		InitScript(initScript).
		MapScript(mapScript).
		CombineScript(combineScript).
		ReduceScript(reduceScript.String())

	res, err := es.Search().
		Index("customer_orders").
		Query(elastic.NewMatchAllQuery()).
		Size(0).
		Aggregation("bd_sum", scriptAgg).
		Do(ctx)

	if err != nil {
		log.Printf("ES Script Agg Error: %v", err)
		return
	}

	aggRes, _ := res.Aggregations.ScriptMetric("bd_sum")
	// 注意：script metric 返回的 Value 是 interface{}，通常打印出来看即可
	fmt.Printf("[ES    ] Limit=ALL      | Type=Script  | Time=%-10v | Sum=%v (BigDecimal)\n", time.Since(start), aggRes.Value)
}
*/