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
# 运行程序
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
