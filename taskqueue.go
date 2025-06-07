// Package taskgo 提供轻量级异步任务队列功能
package taskgo

import (
	"context"
	"time"

	"github.com/minhyannv/task-go/internal/queue"
	"github.com/minhyannv/task-go/internal/redis"
	"github.com/minhyannv/task-go/pkg/task"
)

// QueueType 队列类型别名
type QueueType = queue.QueueType

// 队列类型常量
const (
	SimpleQueue   = queue.SimpleQueue   // 简单队列 (FIFO)
	DelayedQueue  = queue.DelayedQueue  // 延迟队列 (时间调度)
	PriorityQueue = queue.PriorityQueue // 优先级队列 (优先级调度)
)

// TaskQueue 任务队列主接口
type TaskQueue struct {
	queue *queue.AsyncQueue
}

// Config 队列配置
type Config struct {
	RedisAddr      string        // Redis 地址
	RedisPassword  string        // Redis 密码
	RedisDB        int           // Redis 数据库
	QueueType      QueueType     // 队列类型
	DefaultRetry   int           // 默认重试次数
	DefaultTimeout time.Duration // 默认超时时间
	MaxWorkers     int           // 最大工作器数量
}

// NewTaskQueue 创建新的任务队列
func NewTaskQueue(config *Config) (*TaskQueue, error) {
	// 设置默认配置
	if config == nil {
		config = &Config{
			RedisAddr:      "localhost:6379",
			RedisPassword:  "",
			RedisDB:        0,
			QueueType:      SimpleQueue,
			DefaultRetry:   3,
			DefaultTimeout: 30 * time.Second,
			MaxWorkers:     5,
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

	// 创建队列
	queueOpts := &queue.QueueOptions{
		QueueType:      config.QueueType,
		DefaultRetry:   config.DefaultRetry,
		DefaultTimeout: config.DefaultTimeout,
		MaxWorkers:     config.MaxWorkers,
	}

	asyncQueue := queue.NewAsyncQueue(redisClient, queueOpts)

	return &TaskQueue{
		queue: asyncQueue,
	}, nil
}

// RegisterHandler 注册任务处理器
func (tq *TaskQueue) RegisterHandler(jobType string, handler task.TaskHandler) {
	tq.queue.RegisterHandler(jobType, handler)
}

// Submit 提交任务
func (tq *TaskQueue) Submit(ctx context.Context, jobType string, payload interface{}, opts *queue.JobOptions) (string, error) {
	return tq.queue.Submit(ctx, jobType, payload, opts)
}

// SubmitSimple 提交简单任务（使用默认选项）
func (tq *TaskQueue) SubmitSimple(ctx context.Context, jobType string, payload interface{}) (string, error) {
	return tq.queue.SubmitSimple(ctx, jobType, payload)
}

// SubmitWithDelay 提交延迟任务（仅延迟队列有效）
func (tq *TaskQueue) SubmitWithDelay(ctx context.Context, jobType string, payload interface{}, delay time.Duration) (string, error) {
	return tq.queue.SubmitWithDelay(ctx, jobType, payload, delay)
}

// SubmitWithPriority 提交优先级任务（仅优先级队列有效）
func (tq *TaskQueue) SubmitWithPriority(ctx context.Context, jobType string, payload interface{}, priority int) (string, error) {
	return tq.queue.SubmitWithPriority(ctx, jobType, payload, priority)
}

// Start 启动队列
func (tq *TaskQueue) Start(ctx context.Context) error {
	return tq.queue.Start(ctx)
}

// Stop 停止队列
func (tq *TaskQueue) Stop() {
	tq.queue.Stop()
}

// GetStats 获取队列统计信息
func (tq *TaskQueue) GetStats(ctx context.Context) (map[string]interface{}, error) {
	return tq.queue.GetStats(ctx)
}

// GetQueueType 获取队列类型
func (tq *TaskQueue) GetQueueType() QueueType {
	return tq.queue.GetQueueType()
}

// 便捷的构造函数

// NewSimpleTaskQueue 创建简单队列（FIFO）
func NewSimpleTaskQueue(redisAddr, password string, db int) (*TaskQueue, error) {
	config := &Config{
		RedisAddr:     redisAddr,
		RedisPassword: password,
		RedisDB:       db,
		QueueType:     SimpleQueue,
		MaxWorkers:    5,
	}
	return NewTaskQueue(config)
}

// NewDelayedTaskQueue 创建延迟队列
func NewDelayedTaskQueue(redisAddr, password string, db int) (*TaskQueue, error) {
	config := &Config{
		RedisAddr:     redisAddr,
		RedisPassword: password,
		RedisDB:       db,
		QueueType:     DelayedQueue,
		MaxWorkers:    5,
	}
	return NewTaskQueue(config)
}

// NewPriorityTaskQueue 创建优先级队列
func NewPriorityTaskQueue(redisAddr, password string, db int) (*TaskQueue, error) {
	config := &Config{
		RedisAddr:     redisAddr,
		RedisPassword: password,
		RedisDB:       db,
		QueueType:     PriorityQueue,
		MaxWorkers:    5,
	}
	return NewTaskQueue(config)
}
