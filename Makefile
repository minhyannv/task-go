.PHONY: build test example clean help

# 默认目标
help:
	@echo "TaskGo - 轻量级异步任务执行框架"
	@echo ""
	@echo "可用命令："
	@echo "  build     - 编译项目"
	@echo "  test      - 运行测试（需要 Redis）"
	@echo "  example   - 运行基础示例（需要 Redis）"
	@echo "  clean     - 清理生成的文件"
	@echo "  deps      - 下载依赖"
	@echo "  fmt       - 格式化代码"
	@echo "  vet       - 运行 go vet"
	@echo "  lint      - 运行所有检查"
	@echo ""
	@echo "注意：测试和示例需要 Redis 服务运行在 localhost:6379"

# 编译项目
build:
	@echo "编译项目..."
	go build -v ./...

# 运行测试
test:
	@echo "运行测试..."
	@echo "请确保 Redis 服务运行在 localhost:6379"
	go test -v ./test/...

# 运行基础示例
example:
	@echo "运行基础示例..."
	@echo "请确保 Redis 服务运行在 localhost:6379"
	@echo "提示：可以复制 examples/env.example 为 examples/.env 来自定义配置"
	cd examples && go run main.go

# 运行异步队列示例（与基础示例相同）
async-example: example

# 清理
clean:
	@echo "清理生成的文件..."
	go clean -cache
	rm -f task-go

# 下载依赖
deps:
	@echo "下载依赖..."
	go mod download
	go mod tidy

# 格式化代码
fmt:
	@echo "格式化代码..."
	go fmt ./...

# 运行 go vet
vet:
	@echo "运行 go vet..."
	go vet ./...

# 运行所有检查
lint: fmt vet
	@echo "代码检查完成"

# 启动 Redis（macOS with Homebrew）
redis-start:
	@echo "启动 Redis..."
	@command -v redis-server >/dev/null 2>&1 || { echo "请先安装 Redis"; exit 1; }
	redis-server --daemonize yes

# 停止 Redis
redis-stop:
	@echo "停止 Redis..."
	@pkill redis-server || echo "Redis 进程未找到"

# 检查 Redis 状态
redis-status:
	@echo "检查 Redis 状态..."
	@redis-cli ping 2>/dev/null && echo "Redis 运行正常" || echo "Redis 未运行"

# 安装开发工具
install-tools:
	@echo "安装开发工具..."
	go install golang.org/x/tools/cmd/goimports@latest

# 全面测试（包括构建和检查）
all: deps lint build test
	@echo "所有检查完成" 