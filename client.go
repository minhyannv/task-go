package taskgo

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/minhyannv/task-go/internal/config"
	"github.com/minhyannv/task-go/internal/handler_manager"
	"github.com/minhyannv/task-go/internal/models"
	"github.com/minhyannv/task-go/internal/queue"
	"github.com/minhyannv/task-go/internal/task"
	"github.com/minhyannv/task-go/internal/task_manager"
)

// Client TaskGo客户端
type Client struct {
	ctx            context.Context
	config         *config.Config
	logger         *zap.Logger
	redisClient    *redis.Client
	taskManager    *task_manager.TaskManager
	handlerManager *handler_manager.HandlerManager
	queues         map[models.QueueType]*queue.Queue
	running        bool
}

// Option 客户端选项
type Option func(*Client)

// WithConfig 设置配置
func WithConfig(cfg *config.Config) Option {
	return func(c *Client) {
		c.config = cfg
	}
}

// WithLogger 设置日志器
func WithLogger(logger *zap.Logger) Option {
	return func(c *Client) {
		c.logger = logger
	}
}

// WithRedisClient 设置Redis客户端
func WithRedisClient(redisClient *redis.Client) Option {
	return func(c *Client) {
		c.redisClient = redisClient
	}
}

// NewClient 创建新的TaskGo客户端
func NewClient(ctx context.Context, opts ...Option) (*Client, error) {
	client := &Client{
		ctx:     ctx,
		config:  config.DefaultConfig(),
		queues:  make(map[models.QueueType]*queue.Queue),
		running: false,
	}

	// 应用选项
	for _, opt := range opts {
		opt(client)
	}

	// 验证配置
	if err := client.config.Validate(); err != nil {
		return nil, fmt.Errorf("配置验证失败: %w", err)
	}

	// 设置默认日志器
	if client.logger == nil {
		logger, err := zap.NewProduction()
		if err != nil {
			return nil, fmt.Errorf("创建日志器失败: %w", err)
		}
		client.logger = logger
	}

	// 设置Redis客户端
	if client.redisClient == nil {
		client.redisClient = redis.NewClient(&redis.Options{
			Addr:     client.config.Redis.Addr,
			Password: client.config.Redis.Password,
			DB:       client.config.Redis.DB,
		})
	}

	// 测试Redis连接
	if err := client.redisClient.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("Redis连接失败: %w", err)
	}

	// 初始化组件
	client.taskManager = task_manager.NewTaskManager(client.redisClient)
	client.handlerManager = handler_manager.NewHandlerManager(client.logger)

	// 初始化队列
	client.initQueues()

	return client, nil
}

// initQueues 初始化所有类型的队列
func (c *Client) initQueues() {
	queueTypes := []models.QueueType{
		models.SimpleQueue,
		models.DelayQueue,
		models.PriorityQueue,
	}

	for _, queueType := range queueTypes {
		q := queue.NewQueue(
			c.ctx,
			c.logger,
			c.taskManager,
			c.handlerManager,
			queueType,
			c.config.Worker.Count,
		)
		c.queues[queueType] = q
	}
}

// RegisterHandler 注册任务处理器
func (c *Client) RegisterHandler(taskType string, handler task.TaskHandler) error {
	return c.handlerManager.RegisterHandler(taskType, handler)
}

// SubmitSimpleTask 提交简单任务
func (c *Client) SubmitSimpleTask(ctx context.Context, taskType string, payload string) error {
	return c.SubmitSimpleTaskWithOptions(ctx, taskType, payload, nil)
}

// SubmitDelayTask 提交延迟任务
func (c *Client) SubmitDelayTask(ctx context.Context, taskType string, payload string, delay time.Duration) error {
	return c.SubmitDelayTaskWithOptions(ctx, taskType, payload, delay, nil)
}

// SubmitPriorityTask 提交优先级任务
func (c *Client) SubmitPriorityTask(ctx context.Context, taskType string, payload string, priority int) error {
	return c.SubmitPriorityTaskWithOptions(ctx, taskType, payload, priority, nil)
}

// TaskOptions 任务选项
type TaskOptions struct {
	Retry    *int           `json:"retry,omitempty"`
	Timeout  *time.Duration `json:"timeout,omitempty"`
	Priority *int           `json:"priority,omitempty"`
	Delay    *time.Duration `json:"delay,omitempty"`
}

// SubmitSimpleTaskWithOptions 提交简单任务（带选项）
func (c *Client) SubmitSimpleTaskWithOptions(ctx context.Context, taskType string, payload string, opts *TaskOptions) error {
	q, exists := c.queues[models.SimpleQueue]
	if !exists {
		return fmt.Errorf("简单队列不存在")
	}

	delay := c.config.Task.DefaultDelay
	priority := c.config.Task.DefaultPriority

	if opts != nil {
		if opts.Delay != nil {
			delay = *opts.Delay
		}
		if opts.Priority != nil {
			priority = *opts.Priority
		}
	}

	return q.SubmitTask(ctx, taskType, payload, delay, priority)
}

// SubmitDelayTaskWithOptions 提交延迟任务（带选项）
func (c *Client) SubmitDelayTaskWithOptions(ctx context.Context, taskType string, payload string, delay time.Duration, opts *TaskOptions) error {
	q, exists := c.queues[models.DelayQueue]
	if !exists {
		return fmt.Errorf("延迟队列不存在")
	}

	priority := c.config.Task.DefaultPriority

	if opts != nil {
		if opts.Priority != nil {
			priority = *opts.Priority
		}
	}

	return q.SubmitTask(ctx, taskType, payload, delay, priority)
}

// SubmitPriorityTaskWithOptions 提交优先级任务（带选项）
func (c *Client) SubmitPriorityTaskWithOptions(ctx context.Context, taskType string, payload string, priority int, opts *TaskOptions) error {
	q, exists := c.queues[models.PriorityQueue]
	if !exists {
		return fmt.Errorf("优先级队列不存在")
	}

	delay := c.config.Task.DefaultDelay

	if opts != nil {
		if opts.Delay != nil {
			delay = *opts.Delay
		}
	}

	return q.SubmitTask(ctx, taskType, payload, delay, priority)
}

// Start 启动所有队列
func (c *Client) Start() error {
	if c.running {
		return fmt.Errorf("客户端已在运行")
	}

	c.logger.Sugar().Info("启动TaskGo客户端...")

	for queueType, q := range c.queues {
		if err := q.Start(c.ctx); err != nil {
			return fmt.Errorf("启动 %s 队列失败: %w", queueType, err)
		}
	}

	c.running = true
	c.logger.Sugar().Info("TaskGo客户端启动成功")
	return nil
}

// Stop 停止所有队列
func (c *Client) Stop() {
	if !c.running {
		return
	}

	c.logger.Sugar().Info("停止TaskGo客户端...")

	for _, q := range c.queues {
		q.Stop()
	}

	c.running = false
	c.logger.Sugar().Info("TaskGo客户端已停止")
}

// GetStats 获取统计信息
func (c *Client) GetStats(ctx context.Context) (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	for queueType, q := range c.queues {
		queueStats, err := q.GetStats(ctx)
		if err != nil {
			return nil, fmt.Errorf("获取 %s 队列统计失败: %w", queueType, err)
		}
		stats[string(queueType)] = queueStats
	}

	stats["running"] = c.running
	stats["handlers"] = c.handlerManager.ListHandlers()

	return stats, nil
}

// GetTask 获取任务详情
func (c *Client) GetTask(ctx context.Context, taskID string) (*task.Task, error) {
	return c.taskManager.GetTask(ctx, taskID)
}

// Close 关闭客户端
func (c *Client) Close() error {
	c.Stop()
	return c.taskManager.Close()
}
