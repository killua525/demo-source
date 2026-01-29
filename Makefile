.PHONY: build clean help test

# 项目信息
PROJECT_NAME := demo1
OUTPUT_DIR := ./bin
CMD_DIR := ./cmd/demo1

# 编译目标
BIN := $(OUTPUT_DIR)/$(PROJECT_NAME)

# Go编译参数
GO := go
GOFLAG := -v
LDFLAGS := -ldflags="-s -w"

# 默认目标
all: build

# 编译项目
build: clean
	@echo "正在编译 $(PROJECT_NAME)..."
	@mkdir -p $(OUTPUT_DIR)
	$(GO) build $(GOFLAG) $(LDFLAGS) -o $(BIN) $(CMD_DIR)
	@echo "编译完成: $(BIN)"

# 清理生成的文件
clean:
	@echo "正在清理项目..."
	@rm -rf $(OUTPUT_DIR)
	$(GO) clean -cache -testcache
	@echo "清理完成"

# 运行程序
run: build
	$(BIN)

# 格式化代码
fmt:
	@echo "正在格式化代码..."
	$(GO) fmt ./...

# 检查代码
vet:
	@echo "正在检查代码..."
	$(GO) vet ./...

# 运行测试
test:
	@echo "正在运行测试..."
	$(GO) test -v ./...

# 模块依赖
deps:
	@echo "下载依赖..."
	$(GO) mod download
	$(GO) mod tidy

# 显示帮助信息
help:
	@echo "可用命令:"
	@echo "  make build    - 编译项目"
	@echo "  make clean    - 清理生成的文件"
	@echo "  make run      - 编译并运行程序"
	@echo "  make fmt      - 格式化代码"
	@echo "  make vet      - 检查代码"
	@echo "  make test     - 运行测试"
	@echo "  make deps     - 下载依赖"
	@echo "  make help     - 显示此帮助信息"
