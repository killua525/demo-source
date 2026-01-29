# demo-source

[![Build Status](https://travis-ci.org/killua525/demo-source.svg?branch=master)](https://travis-ci.org/killua525/demo-source)

MySQL 和 Elasticsearch 性能对标测试工具。

## 功能特性

- 支持 MySQL 和 Elasticsearch 数据加载
- 提供多种查询性能测试场景
- 支持大规模数据处理（可配置数据量）
- 原生聚合和脚本聚合对标测试

## 安装

### 前置要求

- Go 1.21 或更高版本
- MySQL 5.7 或更高版本
- Elasticsearch 7.0 或更高版本

### 使用 go install 安装

```bash
# 从 GitHub 直接安装（最新版本）
go install github.com/killua525/demo-source/cmd/demo1@latest

# 或克隆仓库后本地安装
git clone https://github.com/killua525/demo-source.git
cd demo-source
go install -v ./cmd/demo1
```

### 使用 make 安装

```bash
git clone https://github.com/killua525/demo-source.git
cd demo-source

# 编译
make build

# 安装到 $GOPATH/bin
make install

# 查看其他命令
make help
```

## 使用

### 基本用法

```bash
# 运行程序，使用默认配置
demo1

# 显示帮助信息
demo1 -help
```

### 配置参数

```bash
demo1 -mysql "root:123456@tcp(127.0.0.1:3306)/test_db?charset=utf8mb4&parseTime=True" \
      -es "http://127.0.0.1:9200" \
      -total 200000 \
      -batch 2000
```

#### 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-mysql` | MySQL 连接字符串 | `root:123456@tcp(127.0.0.1:3306)/test_db?charset=utf8mb4&parseTime=True` |
| `-es` | Elasticsearch 服务地址 | `http://127.0.0.1:9200` |
| `-total` | 总数据量 | `200000` |
| `-batch` | 批量插入大小 | `2000` |

### demo1 工作流程

#### Phase 1: 数据加载
- 创建 MySQL 表和 Elasticsearch 索引
- 使用 Producer-Consumer 模型并发加载数据
- MySQL 使用 DECIMAL(10,2) 存储金额确保精度
- Elasticsearch 使用 scaled_float 类型，缩放因子为 100
- 支持 5 个并发消费者处理数据

#### Phase 2: 性能测试
程序会对不同数据规模运行以下三种测试场景：

**场景 A: MySQL 应用层求和**
- 拉取指定数量的行数据到应用层
- 使用浮点数累加求和
- 测试数据规模：10,000、100,000、1,000,000（若总数据量足够）

**场景 B: Elasticsearch 原生聚合（推荐）**
- 使用 Sum Aggregation 对 scaled_float 类型字段聚合
- 利用 Doc Values 高效计算
- 只针对全量数据执行

**场景 C: Elasticsearch 脚本聚合（高精度）**
- 使用 Painless 脚本和 BigDecimal 进行聚合
- 确保金额数据不丢失精度
- 支持 init_script、map_script、combine_script、reduce_script 四个阶段
- 只针对全量数据执行

### 输出示例

```
>>> [Phase 1] 开始加载 200000 条数据...
>>> 数据加载完成，耗时: 5.234s

>>> [Phase 2] 开始查询性能测试...

--- 测试数据规模: 10000 ---
[MySQL ] Limit=10000   | Type=RowScan | Time=123.45ms  | Sum=500000.00
[ES    ] Limit=ALL     | Type=Native  | Time=45.67ms   | Sum=5000000.00 (Scaled Float)
[ES    ] Limit=ALL     | Type=Script  | Time=89.01ms   | Sum=5000000 (BigDecimal)
```

### 适用场景

- **MySQL 应用层求和**：适合小规模数据，演示应用层处理的成本
- **Elasticsearch 原生聚合**：适合大规模数据，高性能推荐方案
- **Elasticsearch 脚本聚合**：需要高精度计算的金融场景

## 开发

### 代码格式化

```bash
make fmt
```

### 代码检查

```bash
make vet
```

### 运行测试

```bash
make test
```

### 下载依赖

```bash
make deps
```

### 清理构建产物

```bash
make clean
```

## 许可证

MIT
