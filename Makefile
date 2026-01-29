.PHONY: build clean help test install

# 项目信息
PROJECT_NAME := demo1
OUTPUT_DIR := ./bin
CMD_DIR := ./cmd/demo1

# 编译目标
BIN := $(OUTPUT_DIR)/$(PROJECT_NAME)

# Go编译参数
GO := go
GOFLAG := -v

# 版本信息
VERSION ?= v1.0.0
COMMIT_ID := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
BUILD_TIME := $(shell date -u '+%Y-%m-%d %H:%M:%S' 2>/dev/null || echo "unknown")

# ldflags 配置，注入版本信息
LDFLAGS := -ldflags="-s -w \
	-X 'github.com/killua525/demo-source/cmd/demo1.Version=$(VERSION)' \
	-X 'github.com/killua525/demo-source/cmd/demo1.CommitID=$(COMMIT_ID)' \
	-X 'github.com/killua525/demo-source/cmd/demo1.BuildTime=$(BUILD_TIME)'"

# 默认目标
all: build

# 编译项目（包含版本信息）
build: clean
	@echo "正在编译 $(PROJECT_NAME)..."
	@echo "  Version: $(VERSION)"
	@echo "  Commit:  $(COMMIT_ID)"
	@echo "  Built:   $(BUILD_TIME)"
	@mkdir -p $(OUTPUT_DIR)
	$(GO) build $(GOFLAG) $(LDFLAGS) -o $(BIN) ./cmd/demo1
	@echo "编译完成: $(BIN)"

# 安装到 $GOPATH/bin（包含版本信息）
install:
	@echo "正在安装 $(PROJECT_NAME)..."
	$(GO) install $(GOFLAG) $(LDFLAGS) ./cmd/demo1
	@echo "安装完成"

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
	@echo "  make install  - 安装到 $$GOPATH/bin"
	@echo "  make clean    - 清理生成的文件"
	@echo "  make run      - 编译并运行程序"
	@echo "  make fmt      - 格式化代码"
	@echo "  make vet      - 检查代码"
	@echo "  make test     - 运行测试"
	@echo "  make deps     - 下载依赖"
	@echo "  make help     - 显示此帮助信息"
	@echo ""
	@echo "编译选项:"
	@echo "  VERSION=v1.1.0 make build  - 指定版本号编译"
