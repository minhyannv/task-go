// Package taskgo 提供轻量级异步任务队列功能
package pkg

import (
	"context"
	"github.com/minhyannv/task-go/internal/models"
	"go.uber.org/zap"
	"log"
	"time"

	"github.com/minhyannv/task-go/internal/queue"
	"github.com/minhyannv/task-go/internal/redis"
	"github.com/minhyannv/task-go/internal/task"
)

// TaskQueue 任务队列主接口
type TaskQueue struct {
	queue *queue.Queue
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
func NewTaskQueue(config *Config) (*TaskQueue, error) {
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

	// 创建 Redis 客户端
	redisClient := redis.NewClient(config.RedisAddr, config.RedisPassword, config.RedisDB)

	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := redisClient.Ping(ctx); err != nil {
		return nil, err
	}

	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}
	queue := queue.NewQueue(ctx, logger, redisClient, config.QueueType)

	return &TaskQueue{
		queue: queue,
	}, nil
}

// RegisterHandler 注册任务处理器
func (tq *TaskQueue) RegisterHandler(taskType string, handler task.TaskHandler) {
	tq.queue.RegisterHandler(taskType, handler)

}

// Submit 提交任务
func (tq *TaskQueue) Submit(ctx context.Context, taskType string, payload string, opts *queue.TaskOptions) (string, error) {
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

// Stop 停止队列
func (tq *TaskQueue) Stop() {
	tq.queue.Stop()
}

// GetQueueStats 获取队列统计信息
func (tq *TaskQueue) GetQueueStats(ctx context.Context) (map[string]interface{}, error) {
	return tq.queue.GetStats(ctx)
}

// NewSimpleTaskQueue 创建简单队列（FIFO）
func NewSimpleTaskQueue(redisAddr, password string, db int) (*TaskQueue, error) {
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
	return NewTaskQueue(config)
}

// NewDelayedTaskQueue 创建延迟队列
func NewDelayedTaskQueue(redisAddr, password string, db int) (*TaskQueue, error) {
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
	return NewTaskQueue(config)
}

// NewPriorityTaskQueue 创建优先级队列
func NewPriorityTaskQueue(redisAddr, password string, db int) (*TaskQueue, error) {
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
	return NewTaskQueue(config)
}
