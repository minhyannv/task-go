package pkg

import (
	"context"
	"github.com/minhyannv/task-go/internal/handler_manager"
	"os"
	"strconv"
	"time"

	"github.com/minhyannv/task-go/internal/models"
	"github.com/minhyannv/task-go/internal/task_manager"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	_ "github.com/joho/godotenv/autoload"
	"github.com/minhyannv/task-go/internal/queue"
	"github.com/minhyannv/task-go/internal/task"
)

// TaskQueue 任务队列主接口
type TaskQueue struct {
	redisClient    *redis.Client
	queue          *queue.Queue
	handlerManager *handler_manager.HandlerManager
}

// Config 队列配置
type Config struct {
	RedisAddr       string           // Redis 地址
	RedisPassword   string           // Redis 密码
	RedisDB         int              // Redis 数据库
	QueueType       models.QueueType // 队列类型
	DefaultRetry    int              // 默认重试次数
	DefaultTimeout  time.Duration    // 默认超时时间
	DefaultDelay    time.Duration    // 默认任务延迟执行时间
	DefaultPriority int              // 默认任务优先级
	WorkerNumber    int              // 工作器数量
}

// NewTaskQueue 创建新的任务队列
func NewTaskQueue(ctx context.Context, logger *zap.Logger, config *Config) (*TaskQueue, error) {
	// 设置默认配置
	if config == nil {
		config = &Config{
			RedisAddr:       "localhost:6379",
			RedisPassword:   "",
			RedisDB:         0,
			DefaultRetry:    3,
			DefaultTimeout:  30 * time.Second,
			DefaultDelay:    1 * time.Second,
			DefaultPriority: 5,
			WorkerNumber:    5,
		}
	}

	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr != "" {
		config.RedisAddr = redisAddr
	}
	redisPassword := os.Getenv("REDIS_PASSWORD")
	if redisPassword != "" {
		config.RedisPassword = redisPassword
	}
	redisDB := os.Getenv("REDIS_DB")
	if redisDB != "" {
		atoi, err := strconv.Atoi(redisDB)
		if err != nil {
			return nil, err
		}
		config.RedisDB = atoi
	}
	// 创建 Redis 客户端
	redisClient := redis.NewClient(&redis.Options{
		Addr:     config.RedisAddr,
		Password: config.RedisPassword,
		DB:       config.RedisDB,
	})
	// 测试连接
	toctx, tocancel := context.WithTimeout(ctx, 5*time.Second)
	defer tocancel()

	if _, err := redisClient.Ping(toctx).Result(); err != nil {
		return nil, err
	}
	taskManager := task_manager.NewTaskManager(redisClient)
	handlerManager := handler_manager.NewHandlerManager(logger)
	queue := queue.NewQueue(ctx, logger, taskManager, handlerManager, config.QueueType, config.WorkerNumber)

	return &TaskQueue{
		redisClient:    redisClient,
		queue:          queue,
		handlerManager: handlerManager,
	}, nil
}

// RegisterHandler 注册任务处理器
func (tq *TaskQueue) RegisterHandler(taskType string, handler task.TaskHandler) error {
	return tq.handlerManager.RegisterHandler(taskType, handler)
}

// Submit 提交任务
func (tq *TaskQueue) Submit(ctx context.Context, taskType string, payload string, opts *task.TaskOptions) (string, error) {
	return tq.queue.Submit(ctx, taskType, payload, opts)
}

// Start 启动队列
func (tq *TaskQueue) Start(ctx context.Context) error {
	err := tq.queue.Start(ctx)
	if err != nil {
		return err
	}
	return nil
}

// Stop 停止任务队列
func (tq *TaskQueue) Stop() {
	tq.queue.Stop()
	tq.redisClient.Close()
}

// GetQueueStats 获取队列统计信息
func (tq *TaskQueue) GetQueueStats(ctx context.Context) (map[string]interface{}, error) {
	return tq.queue.GetStats(ctx)
}

// NewSimpleTaskQueue 创建简单队列（FIFO）
func NewSimpleTaskQueue(ctx context.Context, logger *zap.Logger, redisAddr, password string, db int) (*TaskQueue, error) {
	config := &Config{
		RedisAddr:       redisAddr,
		RedisPassword:   password,
		RedisDB:         db,
		QueueType:       models.SimpleQueue,
		DefaultRetry:    3,
		DefaultTimeout:  30 * time.Second,
		DefaultDelay:    1 * time.Second,
		DefaultPriority: 5,
		WorkerNumber:    5,
	}
	return NewTaskQueue(ctx, logger, config)
}

// NewDelayedTaskQueue 创建延迟队列
func NewDelayedTaskQueue(ctx context.Context, logger *zap.Logger, redisAddr, password string, db int) (*TaskQueue, error) {
	config := &Config{
		RedisAddr:       redisAddr,
		RedisPassword:   password,
		RedisDB:         db,
		QueueType:       models.DelayedQueue,
		DefaultRetry:    3,
		DefaultTimeout:  30 * time.Second,
		DefaultDelay:    1 * time.Second,
		DefaultPriority: 5,
		WorkerNumber:    5,
	}
	return NewTaskQueue(ctx, logger, config)
}

// NewPriorityTaskQueue 创建优先级队列
func NewPriorityTaskQueue(ctx context.Context, logger *zap.Logger, redisAddr, password string, db int) (*TaskQueue, error) {
	config := &Config{
		RedisAddr:       redisAddr,
		RedisPassword:   password,
		RedisDB:         db,
		QueueType:       models.PriorityQueue,
		DefaultRetry:    3,
		DefaultTimeout:  30 * time.Second,
		DefaultDelay:    1 * time.Second,
		DefaultPriority: 5,
		WorkerNumber:    5,
	}
	return NewTaskQueue(ctx, logger, config)
}
