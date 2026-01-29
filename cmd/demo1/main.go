package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/olivere/elastic/v7"
	"gopkg.in/yaml.v2"
)

// 版本信息，通过编译时 ldflags 注入
var (
	Version   = "dev"
	CommitID  = "unknown"
	BuildTime = "unknown"
)

// --- 配置文件结构 ---
type ConfigFile struct {
	MySQL struct {
		DSN string `yaml:"dsn"`
	} `yaml:"mysql"`
	Elasticsearch struct {
		URL      string `yaml:"url"`
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"elasticsearch"`
	Data struct {
		Total int `yaml:"total"`
		Batch int `yaml:"batch"`
	} `yaml:"data"`
}

// --- 配置对象 ---
type Config struct {
	MySQLDSN    string
	ESUrl       string
	ESUser      string
	ESPassword  string
	Total       int
	Batch       int
	Mode        string // all(默认), mysql, es
	Reload      bool   // 是否重新加载数据（默认false：若数据存在则不加载）
	QueryLevels []int  // 测试数据规模（如 1000,100000,1000000）
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
	// 设置默认值
	cfg.MySQLDSN = "root:123456@tcp(127.0.0.1:3306)/test_db?charset=utf8mb4&parseTime=True"
	cfg.ESUrl = "http://127.0.0.1:9200"
	cfg.Total = 200000
	cfg.Batch = 2000

	// 命令行参数解析（使用临时变量来检测用户是否显式设置了参数）
	showVersion := flag.Bool("version", false, "显示版本信息")
	configFile := flag.String("config", "", "配置文件路径 (config.yaml)")
	mysqlFlag := flag.String("mysql", "", "MySQL连接串")
	esFlag := flag.String("es", "", "ES地址")
	esuserFlag := flag.String("esuser", "", "ES用户名（可选）")
	espassFlag := flag.String("espass", "", "ES密码（可选）")
	modeFlag := flag.String("mode", "", "运行模式: all(默认), mysql, es")
	totalFlag := flag.Int("total", 0, "总数据量")
	batchFlag := flag.Int("batch", 0, "批量插入的大小")
	reloadFlag := flag.Bool("reload", false, "是否强制重新加载数据")
	queryLevelsFlag := flag.String("querylevels", "", "测试数据规模，用逗号分隔 (如 1000,100000,1000000)")
	flag.Parse()

	// 如果请求显示版本信息
	if *showVersion {
		printVersion()
		os.Exit(0)
	}

	// 优先级：默认值 → 配置文件 → 命令行参数
	// 1. 先从配置文件加载（如果存在），覆盖默认值
	if *configFile != "" {
		loadConfigFile(*configFile)
	} else if _, err := os.Stat("config.yaml"); err == nil {
		// 如果没有指定但存在默认 config.yaml 文件，使用它
		loadConfigFile("config.yaml")
	}

	// 2. 再从命令行参数加载（如果用户显式指定了），覆盖配置文件和默认值
	if *mysqlFlag != "" {
		cfg.MySQLDSN = *mysqlFlag
	}
	if *esFlag != "" {
		cfg.ESUrl = *esFlag
	}
	if *esuserFlag != "" {
		cfg.ESUser = *esuserFlag
	}
	if *espassFlag != "" {
		cfg.ESPassword = *espassFlag
	}
	if *modeFlag != "" {
		cfg.Mode = *modeFlag
	}
	if *totalFlag != 0 {
		cfg.Total = *totalFlag
	}
	if *batchFlag != 0 {
		cfg.Batch = *batchFlag
	}
	if *reloadFlag {
		cfg.Reload = *reloadFlag
	}
	// 解析 QueryLevels 参数
	if *queryLevelsFlag != "" {
		levels := strings.Split(*queryLevelsFlag, ",")
		for _, level := range levels {
			level = strings.TrimSpace(level)
			if val, err := strconv.Atoi(level); err == nil && val > 0 {
				cfg.QueryLevels = append(cfg.QueryLevels, val)
			}
		}
	}
}

// 打印版本信息
func printVersion() {
	fmt.Printf("demo1 - MySQL & Elasticsearch 性能对标测试工具\n")
	fmt.Printf("Version: %s\n", Version)
	fmt.Printf("Commit:  %s\n", CommitID)
	fmt.Printf("Built:   %s\n", BuildTime)
}

// 从配置文件加载配置
func loadConfigFile(filePath string) {
	data, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatalf("Failed to read config file: %v", err)
	}

	var cfgFile ConfigFile
	err = yaml.Unmarshal(data, &cfgFile)
	if err != nil {
		log.Fatalf("Failed to parse config file: %v", err)
	}

	// 只有在非空时才覆盖默认值
	if cfgFile.MySQL.DSN != "" {
		cfg.MySQLDSN = cfgFile.MySQL.DSN
	}
	if cfgFile.Elasticsearch.URL != "" {
		cfg.ESUrl = cfgFile.Elasticsearch.URL
	}
	if cfgFile.Elasticsearch.Username != "" {
		cfg.ESUser = cfgFile.Elasticsearch.Username
	}
	if cfgFile.Elasticsearch.Password != "" {
		cfg.ESPassword = cfgFile.Elasticsearch.Password
	}
	if cfgFile.Data.Total > 0 {
		cfg.Total = cfgFile.Data.Total
	}
	if cfgFile.Data.Batch > 0 {
		cfg.Batch = cfgFile.Data.Batch
	}

	fmt.Printf(">>> 已从配置文件加载配置: %s\n", filePath)
}

func main() {
	rand.Seed(time.Now().UnixNano())

	var db *sql.DB
	var esClient *elastic.Client

	// 1. 根据 mode 参数有条件地初始化 MySQL
	if cfg.Mode == "mysql" || cfg.Mode == "all" {
		var err error
		db, err = sql.Open("mysql", cfg.MySQLDSN)
		if err != nil {
			log.Fatalf("MySQL connect failed: %v", err)
		}
		defer db.Close()
		db.SetMaxOpenConns(20)
		db.SetMaxIdleConns(10)
		fmt.Println(">>> MySQL 连接成功")
	}

	// 2. 根据 mode 参数有条件地初始化 ES
	if cfg.Mode == "es" || cfg.Mode == "all" {
		esClientOpts := []elastic.ClientOptionFunc{
			elastic.SetURL(cfg.ESUrl),
			elastic.SetSniff(false), // 单节点建议关闭
		}
		// 如果提供了用户名和密码，添加认证
		if cfg.ESUser != "" && cfg.ESPassword != "" {
			esClientOpts = append(esClientOpts, elastic.SetBasicAuth(cfg.ESUser, cfg.ESPassword))
		}
		var err error
		esClient, err = elastic.NewClient(esClientOpts...)
		if err != nil {
			log.Fatalf("ES connect failed: %v", err)
		}
		fmt.Println(">>> Elasticsearch 连接成功")
	}

	// 3. 检查数据是否已存在，如果不需要 reload 则跳过加载
	shouldLoadData := cfg.Reload || !dataExists(db, esClient)

	// 4. 初始化 Schema (表结构 + 索引配置)
	// 只有当需要加载数据时才初始化 Schema（会删除并重建）
	if shouldLoadData {
		initSchema(db, esClient)
	}
	if shouldLoadData {
		// 数据加载 (Producer-Consumer 模型)
		fmt.Printf(">>> [Phase 1] 开始加载 %d 条数据...\n", cfg.Total)
		start := time.Now()
		loadData(db, esClient)
		fmt.Printf(">>> 数据加载完成，耗时: %v\n\n", time.Since(start))
	} else {
		fmt.Println(">>> [Phase 1] 数据已存在，跳过数据加载（若需重新加载，请使用 -reload 参数）\n")
	}
	// 测试不同数据规模下的性能
	queryLevels := cfg.QueryLevels
	// 如果没有通过参数指定，使用默认规模
	if len(queryLevels) == 0 {
		queryLevels = []int{10000, 100000}
		if cfg.Total >= 1000000 {
			queryLevels = append(queryLevels, 1000000)
		}
		// 如果总数够大，最后测试全量
		if cfg.Total > 1000000 {
			queryLevels = append(queryLevels, cfg.Total)
		}
	}

	fmt.Println(">>> [Phase 2] 开始查询性能测试...")
	for _, limit := range queryLevels {
		fmt.Printf("\n--- 测试数据规模: %d ---\n", limit)

		var wg sync.WaitGroup

		// 场景 A: MySQL 查询 + 应用层求和（在 mysql 或 all 模式下运行）
		if cfg.Mode == "mysql" || cfg.Mode == "all" {
			wg.Add(1)
			go func(l int) {
				defer wg.Done()
				benchmarkMySQL(db, l)
			}(limit)
		}

		// 场景 B & C: ES 聚合（仅在 es 或 all 模式，并且通常在全量时运行）
		if (cfg.Mode == "es" || cfg.Mode == "all") && limit == cfg.Total {
			// 原生聚合
			wg.Add(1)
			go func() {
				defer wg.Done()
				benchmarkESNativeAgg(esClient)
			}()

			// 脚本聚合
			wg.Add(1)
			go func() {
				defer wg.Done()
				benchmarkESScriptAgg(esClient)
			}()
		}

		// 等待本次规模的所有测试完成再进入下一个规模
		wg.Wait()
	}
}

// --- 初始化逻辑 ---
func dataExists(db *sql.DB, es *elastic.Client) bool {
	ctx := context.Background()
	hasData := true

	// 检查 MySQL 数据是否存在
	if cfg.Mode == "mysql" || cfg.Mode == "all" {
		if db != nil {
			// 先检查表是否存在（通过 information_schema）
			var tableExists int
			err := db.QueryRow(`
				SELECT COUNT(*) FROM information_schema.TABLES 
				WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'customer_orders'
			`).Scan(&tableExists)

			if err != nil {
				fmt.Printf(">>> [检查数据] MySQL 检查表出错: %v\n", err)
				hasData = false
			} else if tableExists == 0 {
				fmt.Println(">>> [检查数据] MySQL 表 customer_orders 不存在")
				hasData = false
			} else {
				// 表存在，再检查数据量
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM customer_orders").Scan(&count)
				if err != nil {
					fmt.Printf(">>> [检查数据] MySQL 统计数据出错: %v\n", err)
					hasData = false
				} else if count == 0 {
					fmt.Println(">>> [检查数据] MySQL 表 customer_orders 存在，但数据为空")
					hasData = false
				} else {
					fmt.Printf(">>> [检查数据] MySQL 表 customer_orders 已存在，当前数据量: %d 条\n", count)
				}
			}
		}
	}

	// 检查 ES 数据是否存在
	if cfg.Mode == "es" || cfg.Mode == "all" {
		if es != nil {
			// 先检查索引是否存在
			exists, err := es.IndexExists("customer_orders").Do(ctx)
			if err != nil {
				fmt.Printf(">>> [检查数据] Elasticsearch 检查索引出错: %v\n", err)
				hasData = false
			} else if !exists {
				fmt.Println(">>> [检查数据] Elasticsearch 索引 customer_orders 不存在")
				hasData = false
			} else {
				// 索引存在，再检查数据量
				count, err := es.Count("customer_orders").Do(ctx)
				if err != nil {
					fmt.Printf(">>> [检查数据] Elasticsearch 统计数据出错: %v\n", err)
					hasData = false
				} else if count == 0 {
					fmt.Println(">>> [检查数据] Elasticsearch 索引 customer_orders 存在，但数据为空")
					hasData = false
				} else {
					fmt.Printf(">>> [检查数据] Elasticsearch 索引 customer_orders 已存在，当前数据量: %d 条\n", count)
				}
			}
		}
	}

	return hasData
}

// --- 初始化逻辑 ---
func initSchema(db *sql.DB, es *elastic.Client) {
	if cfg.Mode == "mysql" || cfg.Mode == "all" {
		// MySQL DDL: 使用 DECIMAL 保证金额精确（幂等操作）
		// 如果表存在，CREATE TABLE IF NOT EXISTS 不会删除现有数据
		if db != nil {
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
			fmt.Println(">>> MySQL 表初始化完成（如果表已存在则保留现有数据）")
		}
	}

	if cfg.Mode == "es" || cfg.Mode == "all" {
		// ES Mapping:
		// 1. 设置 max_result_window 为 1000w
		// 2. 设置 amount 为 scaled_float，因子 100
		// 注意：只有在 reload 模式下才会删除重建索引
		if es != nil {
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
				// 索引已存在
				if cfg.Reload {
					// reload 模式下才删除重建
					fmt.Println(">>> Elasticsearch 检测到索引存在，由于启用了 reload 参数，将删除并重建索引")
					es.DeleteIndex("customer_orders").Do(ctx)
					_, err := es.CreateIndex("customer_orders").BodyString(mapping).Do(ctx)
					if err != nil {
						log.Fatalf("ES create index failed: %v", err)
					}
					fmt.Println(">>> Elasticsearch 索引重建完成")
				} else {
					fmt.Println(">>> Elasticsearch 索引初始化完成（索引已存在，保留现有数据）")
				}
			} else {
				// 索引不存在，创建新索引
				_, err := es.CreateIndex("customer_orders").BodyString(mapping).Do(ctx)
				if err != nil {
					log.Fatalf("ES create index failed: %v", err)
				}
				fmt.Println(">>> Elasticsearch 索引初始化完成（新建索引）")
			}
		}
	}

	if cfg.Mode == "all" {
		fmt.Println(">>> Schema 初始化完毕 (MySQL Table + ES Index)")
	}
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
				// 根据 mode 参数决定是否写入 MySQL 或 ES
				if cfg.Mode == "mysql" || cfg.Mode == "all" {
					writeMySQL(db, batch)
				}
				if cfg.Mode == "es" || cfg.Mode == "all" {
					writeES(es, batch)
				}
			}
		}()
	}
	wg.Wait()

	// 只有 ES 模式或 all 模式才需要刷新 ES
	if cfg.Mode == "es" || cfg.Mode == "all" {
		// 强制刷新 ES，确保数据立即可查
		es.Refresh("customer_orders").Do(context.Background())
	}
}

func writeMySQL(db *sql.DB, orders []Order) {
	if len(orders) == 0 {
		return
	}
	sqlStr := "INSERT INTO customer_orders (order_id, customer_id, amount, create_time) VALUES "
	vals := []interface{}{}
	placeholders := make([]string, 0, len(orders))
	for _, o := range orders {
		placeholders = append(placeholders, "(?, ?, ?, ?)")
		vals = append(vals, o.OrderID, o.CustomerID, o.Amount, o.CreateTime)
	}
	sqlStr += strings.Join(placeholders, ",")
	_, err := db.Exec(sqlStr, vals...)
	if err != nil {
		log.Printf("MySQL Write Error: %v", err)
	}
}

func writeES(es *elastic.Client, orders []Order) {
	if len(orders) == 0 {
		return
	}
	bulk := es.Bulk().Index("customer_orders")
	for _, o := range orders {
		bulk.Add(elastic.NewBulkIndexRequest().Doc(o))
	}
	_, err := bulk.Do(context.Background())
	if err != nil {
		log.Printf("ES Write Error: %v", err)
	}
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

// --- 基准测试: ES 脚本聚合 (使用原生 JSON) ---
func benchmarkESScriptAgg(es *elastic.Client) {
	start := time.Now()
	ctx := context.Background()

	// 使用 BigDecimal 确保精度不丢失（ES7 兼容的 Painless 脚本）
	// 注意：Elasticsearch 中脚本聚合的正确类型是 scripted_metric
	query := map[string]interface{}{
		"aggs": map[string]interface{}{
			"bd_sum": map[string]interface{}{
				"scripted_metric": map[string]interface{}{
					// 初始化为 BigDecimal.ZERO（使用完全限定类名以确保 Painless 能识别）
					"init_script": "state.total = new java.math.BigDecimal('0')",
					// 将每个值转换为 BigDecimal 并累加（保证精度）
					"map_script": "state.total = state.total.add(new java.math.BigDecimal(String.valueOf(doc['amount'].value)))",
					// 分片内返回字符串形式的 BigDecimal（避免序列化为复杂对象）
					"combine_script": "return state.total.toPlainString();",
					// 跨分片汇总，接收每个分片的字符串表示并用 BigDecimal 累加，最后返回字符串
					"reduce_script": `
							java.math.BigDecimal sum = new java.math.BigDecimal('0');
							for (t in states) {
								if (t != null) {
									sum = sum.add(new java.math.BigDecimal(String.valueOf(t)));
								}
							}
							return sum.toPlainString();
						`,
				},
			},
		},
	}

	// 执行搜索请求
	searchResult, err := es.Search().
		Index("customer_orders").
		Query(elastic.NewMatchAllQuery()).
		Size(0).
		Source(query).
		Do(ctx)

	if err != nil {
		log.Printf("ES Script Agg Error: %v", err)
		return
	}

	// 提取聚合结果
	if searchResult.Aggregations == nil {
		log.Printf("ES Script Agg Error: no aggregations in response")
		return
	}

	// 从原始 JSON 数据中获取 bd_sum 结果（处理多种可能的返回类型）
	var resultValue interface{} = "N/A"
	if rawMsg, found := searchResult.Aggregations["bd_sum"]; found {
		// 打印原始响应以便调试异常情况
		var aggResult map[string]interface{}
		if err := json.Unmarshal(rawMsg, &aggResult); err != nil {
			log.Printf("ES Script Agg: failed to unmarshal aggregation raw json: %v", err)
			resultValue = string(rawMsg)
		} else {
			// 常见情况：脚本返回字符串形式的数值（我们在 reduce 中返回了字符串）
			if val, ok := aggResult["value"]; ok {
				resultValue = val
			} else if doc, ok := aggResult["bd_sum"]; ok {
				resultValue = doc
			} else {
				// 保险回退：打印整个聚合结构
				resultValue = aggResult
			}
		}
	} else {
		log.Printf("ES Script Agg Error: bd_sum not found in aggregations")
	}

	fmt.Printf("[ES    ] Limit=ALL      | Type=Script  | Time=%-10v | Sum=%v (BigDecimal String)\n", time.Since(start), resultValue)
}
