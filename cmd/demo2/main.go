package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// 配置
var (
	mysqlDSN       = "root:123456@tcp(127.0.0.1:3306)/test_db?charset=utf8mb4&parseTime=True"
	totalTables    = 200
	largeTables    = 3              // 大表数量
	largeTableRows = 5000000        // 大表行数 500w
	smallTableRows = 50000          // 小表行数 5w
	batchSize      = 5000           // 批量插入大小
	concurrency    = 10             // 并发数
	tablePrefix    = "bench_table_" // 表名前缀
)

func init() {
	flag.StringVar(&mysqlDSN, "mysql", mysqlDSN, "MySQL连接串")
	flag.IntVar(&totalTables, "tables", totalTables, "总表数量")
	flag.IntVar(&largeTables, "large", largeTables, "大表数量")
	flag.IntVar(&largeTableRows, "large-rows", largeTableRows, "大表行数")
	flag.IntVar(&smallTableRows, "small-rows", smallTableRows, "小表行数")
	flag.IntVar(&batchSize, "batch", batchSize, "批量插入大小")
	flag.IntVar(&concurrency, "concurrency", concurrency, "并发数")
	flag.Parse()
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// 如果命令行没有指定MySQL连接串，则交互式输入
	reader := bufio.NewReader(os.Stdin)
	if mysqlDSN == "root:123456@tcp(127.0.0.1:3306)/test_db?charset=utf8mb4&parseTime=True" {
		fmt.Println("========== MySQL 连接配置 ==========")
		fmt.Println("请输入MySQL连接串 (格式: user:password@tcp(host:port)/dbname)")
		fmt.Println("示例: root:123456@tcp(127.0.0.1:3306)/test_db")
		fmt.Print("连接串: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)
		if input != "" {
			// 自动添加charset参数
			if !strings.Contains(input, "charset") {
				if strings.Contains(input, "?") {
					input += "&charset=utf8mb4&parseTime=True"
				} else {
					input += "?charset=utf8mb4&parseTime=True"
				}
			}
			mysqlDSN = input
		}
		fmt.Println()
	}

	// 连接数据库
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		log.Fatalf("MySQL connect failed: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(concurrency + 5)
	db.SetMaxIdleConns(concurrency)

	// 测试连接
	if err := db.Ping(); err != nil {
		log.Fatalf("MySQL ping failed: %v", err)
	}
	fmt.Println(">>> MySQL 连接成功")

	// Phase 1: 创建表结构
	fmt.Println("\n========== Phase 1: 创建表结构 ==========")
	createTables(db)

	// Phase 2: 预置数据
	fmt.Println("\n========== Phase 2: 预置数据 ==========")
	loadData(db)

	// 等待用户输入
	fmt.Println("\n========================================")
	fmt.Println("数据预置完成！")
	fmt.Println("输入 'continue' 或 'c' 继续执行DDL操作")
	fmt.Println("  - 大表: 将pkb列设置为主键")
	fmt.Println("  - 小表: 将pkb列设置为主键")
	fmt.Println("输入 'exit' 或 'q' 退出程序")
	fmt.Println("========================================")
	for {
		fmt.Print("\n请输入命令: ")
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(strings.ToLower(input))

		switch input {
		case "continue", "c":
			// Phase 3: 执行DDL操作
			fmt.Println("\n========== Phase 3: 执行DDL操作 ==========")
			ddlStart := time.Now()
			executeDDLOperations(db)
			ddlDuration := time.Since(ddlStart)
			fmt.Printf("\n>>> DDL操作总耗时: %v\n", ddlDuration)
			fmt.Println("DDL操作完成，程序退出")
			return
		case "exit", "q":
			fmt.Println("程序退出")
			return
		default:
			fmt.Println("无效命令，请输入 'continue' 或 'exit'")
		}
	}
}

// createTables 创建所有表
func createTables(db *sql.DB) {
	fmt.Printf(">>> 开始创建 %d 张表...\n", totalTables)
	start := time.Now()

	for i := 1; i <= totalTables; i++ {
		tableName := fmt.Sprintf("%s%03d", tablePrefix, i)
		isLarge := i <= largeTables

		var createTableSQL string
		if isLarge {
			// 大表：无主键、无索引，有唯一数据列pkb
			createTableSQL = fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					pkb BIGINT NOT NULL,
					col_varchar_1 VARCHAR(100),
					col_varchar_2 VARCHAR(200),
					col_varchar_3 VARCHAR(50),
					col_int_1 INT,
					col_int_2 INT,
					col_int_3 INT,
					col_bigint_1 BIGINT,
					col_bigint_2 BIGINT,
					col_decimal_1 DECIMAL(19, 4),
					col_decimal_2 DECIMAL(15, 2),
					col_float_1 FLOAT,
					col_double_1 DOUBLE,
					col_datetime_1 DATETIME,
					col_datetime_2 DATETIME,
					col_date_1 DATE,
					col_text_1 TEXT,
					col_tinyint_1 TINYINT,
					col_smallint_1 SMALLINT,
					col_timestamp_1 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
			`, tableName)
		} else {
			// 小表：无主键、无索引，有唯一数据列pkb
			createTableSQL = fmt.Sprintf(`
				CREATE TABLE IF NOT EXISTS %s (
					pkb BIGINT NOT NULL,
					col_varchar_1 VARCHAR(100),
					col_varchar_2 VARCHAR(200),
					col_varchar_3 VARCHAR(50),
					col_int_1 INT,
					col_int_2 INT,
					col_int_3 INT,
					col_bigint_1 BIGINT,
					col_bigint_2 BIGINT,
					col_decimal_1 DECIMAL(19, 4),
					col_decimal_2 DECIMAL(15, 2),
					col_float_1 FLOAT,
					col_double_1 DOUBLE,
					col_datetime_1 DATETIME,
					col_datetime_2 DATETIME,
					col_date_1 DATE,
					col_text_1 TEXT,
					col_tinyint_1 TINYINT,
					col_smallint_1 SMALLINT,
					col_timestamp_1 TIMESTAMP DEFAULT CURRENT_TIMESTAMP
				) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
			`, tableName)
		}

		// 先删除旧表
		db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))

		_, err := db.Exec(createTableSQL)
		if err != nil {
			log.Printf("创建表 %s 失败: %v", tableName, err)
		}

		if i%50 == 0 {
			fmt.Printf(">>> 已创建 %d/%d 张表\n", i, totalTables)
		}
	}

	fmt.Printf(">>> 表结构创建完成，耗时: %v\n", time.Since(start))
}

// loadData 加载数据
func loadData(db *sql.DB) {
	start := time.Now()

	// 使用工作池模式并发加载
	type tableTask struct {
		tableName string
		rows      int
		isLarge   bool
	}

	tasks := make(chan tableTask, totalTables)
	var wg sync.WaitGroup

	// 启动工作协程
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range tasks {
				loadTableData(db, task.tableName, task.rows, task.isLarge)
			}
		}()
	}

	// 分发任务
	for i := 1; i <= totalTables; i++ {
		tableName := fmt.Sprintf("%s%03d", tablePrefix, i)
		isLarge := i <= largeTables
		rows := smallTableRows
		if isLarge {
			rows = largeTableRows
		}
		tasks <- tableTask{tableName: tableName, rows: rows, isLarge: isLarge}
	}
	close(tasks)

	wg.Wait()
	fmt.Printf("\n>>> 数据加载完成，总耗时: %v\n", time.Since(start))
}

// loadTableData 加载单表数据
func loadTableData(db *sql.DB, tableName string, totalRows int, isLarge bool) {
	start := time.Now()
	tableType := "小表"
	if isLarge {
		tableType = "大表"
	}
	fmt.Printf(">>> [%s] 开始加载 %s 数据 (%d 行)...\n", tableType, tableName, totalRows)

	loaded := 0
	for loaded < totalRows {
		batchRows := batchSize
		if loaded+batchRows > totalRows {
			batchRows = totalRows - loaded
		}

		insertBatch(db, tableName, batchRows, loaded, isLarge)
		loaded += batchRows

		// 大表每100万行打印一次进度
		if isLarge && loaded%(1000000) == 0 {
			fmt.Printf(">>> [大表] %s 已加载 %d/%d 行 (%.1f%%)\n",
				tableName, loaded, totalRows, float64(loaded)/float64(totalRows)*100)
		}
	}

	fmt.Printf(">>> [%s] %s 加载完成，行数: %d，耗时: %v\n", tableType, tableName, totalRows, time.Since(start))
}

// insertBatch 批量插入数据
func insertBatch(db *sql.DB, tableName string, rows int, offset int, isLarge bool) {
	if rows == 0 {
		return
	}

	var sqlPrefix string
	var placeholders []string
	var values []interface{}

	baseTime := time.Now()

	if isLarge {
		// 大表：包含pkb列（唯一数据）
		sqlPrefix = fmt.Sprintf(`INSERT INTO %s (
			pkb,
			col_varchar_1, col_varchar_2, col_varchar_3,
			col_int_1, col_int_2, col_int_3,
			col_bigint_1, col_bigint_2,
			col_decimal_1, col_decimal_2,
			col_float_1, col_double_1,
			col_datetime_1, col_datetime_2, col_date_1,
			col_text_1, col_tinyint_1, col_smallint_1
		) VALUES `, tableName)

		placeholders = make([]string, 0, rows)
		values = make([]interface{}, 0, rows*19)

		for i := 0; i < rows; i++ {
			placeholders = append(placeholders, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

			rowNum := offset + i
			values = append(values,
				int64(rowNum+1), // pkb (唯一值，从1开始)
				fmt.Sprintf("varchar1_%d_%d", rowNum, rand.Intn(10000)), // col_varchar_1
				fmt.Sprintf("varchar2_%d_%s", rowNum, randomString(20)), // col_varchar_2
				fmt.Sprintf("v3_%d", rowNum),                            // col_varchar_3
				rand.Intn(1000000),                                      // col_int_1
				rand.Intn(500000),                                       // col_int_2
				rand.Intn(100000),                                       // col_int_3
				rand.Int63n(10000000000),                                // col_bigint_1
				rand.Int63n(5000000000),                                 // col_bigint_2
				float64(rand.Int63n(100000000))/10000,                   // col_decimal_1
				float64(rand.Int63n(10000000))/100,                      // col_decimal_2
				rand.Float32()*10000,                                    // col_float_1
				rand.Float64()*100000,                                   // col_double_1
				baseTime.Add(-time.Duration(rand.Intn(365*24))*time.Hour).Format("2006-01-02 15:04:05"), // col_datetime_1
				baseTime.Add(-time.Duration(rand.Intn(180*24))*time.Hour).Format("2006-01-02 15:04:05"), // col_datetime_2
				baseTime.Add(-time.Duration(rand.Intn(365*24))*time.Hour).Format("2006-01-02"),          // col_date_1
				fmt.Sprintf("Text content for row %d: %s", rowNum, randomString(50)),                    // col_text_1
				rand.Intn(128),   // col_tinyint_1
				rand.Intn(32768), // col_smallint_1
			)
		}
	} else {
		// 小表：包含pkb列（唯一数据）
		sqlPrefix = fmt.Sprintf(`INSERT INTO %s (
			pkb,
			col_varchar_1, col_varchar_2, col_varchar_3,
			col_int_1, col_int_2, col_int_3,
			col_bigint_1, col_bigint_2,
			col_decimal_1, col_decimal_2,
			col_float_1, col_double_1,
			col_datetime_1, col_datetime_2, col_date_1,
			col_text_1, col_tinyint_1, col_smallint_1
		) VALUES `, tableName)

		placeholders = make([]string, 0, rows)
		values = make([]interface{}, 0, rows*19)

		for i := 0; i < rows; i++ {
			placeholders = append(placeholders, "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")

			rowNum := offset + i
			values = append(values,
				int64(rowNum+1), // pkb (唯一值，从1开始)
				fmt.Sprintf("varchar1_%d_%d", rowNum, rand.Intn(10000)), // col_varchar_1
				fmt.Sprintf("varchar2_%d_%s", rowNum, randomString(20)), // col_varchar_2
				fmt.Sprintf("v3_%d", rowNum),                            // col_varchar_3
				rand.Intn(1000000),                                      // col_int_1
				rand.Intn(500000),                                       // col_int_2
				rand.Intn(100000),                                       // col_int_3
				rand.Int63n(10000000000),                                // col_bigint_1
				rand.Int63n(5000000000),                                 // col_bigint_2
				float64(rand.Int63n(100000000))/10000,                   // col_decimal_1
				float64(rand.Int63n(10000000))/100,                      // col_decimal_2
				rand.Float32()*10000,                                    // col_float_1
				rand.Float64()*100000,                                   // col_double_1
				baseTime.Add(-time.Duration(rand.Intn(365*24))*time.Hour).Format("2006-01-02 15:04:05"), // col_datetime_1
				baseTime.Add(-time.Duration(rand.Intn(180*24))*time.Hour).Format("2006-01-02 15:04:05"), // col_datetime_2
				baseTime.Add(-time.Duration(rand.Intn(365*24))*time.Hour).Format("2006-01-02"),          // col_date_1
				fmt.Sprintf("Text content for row %d: %s", rowNum, randomString(50)),                    // col_text_1
				rand.Intn(128),   // col_tinyint_1
				rand.Intn(32768), // col_smallint_1
			)
		}
	}

	sqlStr := sqlPrefix + strings.Join(placeholders, ",")
	_, err := db.Exec(sqlStr, values...)
	if err != nil {
		log.Printf("批量插入 %s 失败: %v", tableName, err)
	}
}

// executeDDLOperations 执行DDL操作
func executeDDLOperations(db *sql.DB) {
	// 对大表执行DDL操作（添加主键）
	fmt.Println("\n========== 大表DDL操作 ==========")
	for i := 1; i <= largeTables; i++ {
		tableName := fmt.Sprintf("%s%03d", tablePrefix, i)
		fmt.Printf("\n>>> 开始处理大表 %s 的DDL操作...\n", tableName)

		// 大表只需要对pkb列添加主键
		fmt.Printf("  添加主键 (pkb)...\n")
		alterPKStart := time.Now()
		_, err := db.Exec(fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (pkb)", tableName))
		if err != nil {
			log.Printf("  添加主键失败: %v", err)
			continue
		}
		fmt.Printf("  添加主键完成，耗时: %v\n", time.Since(alterPKStart))

		fmt.Printf(">>> 大表 %s DDL操作完成\n", tableName)
	}

	// 对小表执行DDL操作（增加主键）
	fmt.Println("\n========== 小表DDL操作 ==========")
	smallTableCount := totalTables - largeTables
	fmt.Printf(">>> 开始处理 %d 张小表的DDL操作...\n", smallTableCount)

	smallTableStart := time.Now()
	successCount := 0
	failCount := 0

	for i := largeTables + 1; i <= totalTables; i++ {
		tableName := fmt.Sprintf("%s%03d", tablePrefix, i)

		// 小表只需要对pkb列添加主键
		_, err := db.Exec(fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (pkb)", tableName))
		if err != nil {
			log.Printf("  %s 添加主键失败: %v", tableName, err)
			failCount++
			continue
		}

		successCount++

		// 每50张表打印一次进度
		if successCount%50 == 0 {
			fmt.Printf(">>> 已完成 %d/%d 张小表\n", successCount, smallTableCount)
		}
	}

	fmt.Printf("\n>>> 小表DDL操作完成: 成功 %d 张，失败 %d 张，耗时: %v\n", successCount, failCount, time.Since(smallTableStart))
}

// randomString 生成随机字符串
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
