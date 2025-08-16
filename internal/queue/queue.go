package queue

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/minhyannv/task-go/internal/models"
	"github.com/minhyannv/task-go/internal/worker"
	"log"
	"sync"
	"time"

	"github.com/minhyannv/task-go/internal/redis"
	"github.com/minhyannv/task-go/internal/task"
	goredis "github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// Queue 队列
type Queue struct {
	ctx       context.Context
	logger    *zap.Logger
	redis     *redis.Client
	queueType models.QueueType // 队列类型

	workers  map[string]*worker.Worker
	handlers map[string]task.TaskHandler
	running  bool
	stopCh   chan struct{}
	wg       sync.WaitGroup
	mu       sync.RWMutex

	// 配置选项
	defaultRetry    int           // 任务执行失败重试次数，默认：3
	defaultTimeout  time.Duration // 任务执行超时时间，默认：30s
	defaultDelay    time.Duration // 延迟执行时间 (仅延迟队列有效)，默认：1s
	DefaultPriority int           // 优先级 (仅优先级队列有效, 1-10, 10最高)，默认：5

	workerNumber int // 工作器数量
}

// TaskOptions 任务选项
type TaskOptions struct {
	Retry    int           // 重试次数
	Timeout  time.Duration // 超时时间
	Delay    time.Duration // 延迟执行时间 (仅延迟队列有效)
	Priority int           // 优先级 (仅优先级队列有效, 1-10, 10最高)
}

// NewQueue 创建队列
func NewQueue(ctx context.Context, logger *zap.Logger, redisClient *redis.Client, queueType models.QueueType) *Queue {
	return &Queue{
		ctx:            ctx,
		logger:         logger,
		redis:          redisClient,
		workers:        make(map[string]*worker.Worker),
		handlers:       make(map[string]task.TaskHandler),
		stopCh:         make(chan struct{}),
		defaultRetry:   3,
		defaultTimeout: 30 * time.Second,
		defaultDelay:   1 * time.Second,
		workerNumber:   10,
		queueType:      queueType,
	}
}

// RegisterHandler 注册任务处理器
func (q *Queue) RegisterHandler(taskType string, handler task.TaskHandler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[taskType] = handler
	q.logger.Sugar().Infof("已注册任务处理器: %s (队列类型: %s)", taskType, q.queueType)
	for _, worker := range q.workers {
		worker.RegisterHandler(taskType, handler)
	}
}

// Submit 提交任务到指定类型的队列
func (q *Queue) Submit(ctx context.Context, taskType string, payload string, opts *TaskOptions) (string, error) {

	// 设置默认选项
	if opts == nil {
		opts = &TaskOptions{}
	}
	if opts.Retry == 0 {
		opts.Retry = q.defaultRetry
	}
	if opts.Timeout == 0 {
		opts.Timeout = q.defaultTimeout
	}
	if opts.Delay == 0 {
		opts.Delay = q.defaultDelay
	}
	if opts.Priority == 0 {
		opts.Priority = q.DefaultPriority
	}

	// 创建任务
	t := &task.Task{
		ID:        uuid.New().String(),
		Type:      taskType,
		Payload:   payload,
		Status:    task.StatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Retry:     opts.Retry,
		Timeout:   opts.Timeout,
		Delay:     opts.Delay,
		Priority:  opts.Priority,
	}

	// 保存任务信息
	if err := q.redis.SaveTask(ctx, t); err != nil {
		return "", fmt.Errorf("保存任务失败: %w", err)
	}

	// 根据队列类型提交任务
	switch q.queueType {
	case models.SimpleQueue:
		return q.submitToSimpleQueue(ctx, t)
	case models.DelayedQueue:
		return q.submitToDelayedQueue(ctx, t, opts)
	case models.PriorityQueue:
		return q.submitToPriorityQueue(ctx, t, opts)
	default:
		return "", fmt.Errorf("不支持的队列类型: %s", q.queueType)
	}
}

// submitToSimpleQueue 提交到简单队列 (FIFO)
func (q *Queue) submitToSimpleQueue(ctx context.Context, t *task.Task) (string, error) {
	if err := q.redis.EnqueueTask(ctx, t.ID); err != nil {
		return "", fmt.Errorf("任务入队失败: %w", err)
	}
	q.logger.Sugar().Infof("简单队列任务已提交: %s", t.ID)
	return t.ID, nil
}

// submitToDelayedQueue 提交到延迟队列
func (q *Queue) submitToDelayedQueue(ctx context.Context, t *task.Task, opts *TaskOptions) (string, error) {
	var executeAt time.Time
	if opts.Delay > 0 {
		executeAt = time.Now().Add(opts.Delay)
	} else {
		executeAt = time.Now() // 立即执行
	}

	if err := q.redis.EnqueueDelayedTask(ctx, t.ID, executeAt, 5); err != nil { // 延迟队列不需要优先级
		return "", fmt.Errorf("延迟任务入队失败: %w", err)
	}

	if opts.Delay > 0 {
		q.logger.Sugar().Infof("延迟队列任务已提交: %s, 将于 %v 执行", t.ID, executeAt.Format("2006-01-02 15:04:05"))
	} else {
		q.logger.Sugar().Infof("延迟队列任务已提交: %s (立即执行)", t.ID)
	}
	return t.ID, nil
}

// submitToPriorityQueue 提交到优先级队列
func (q *Queue) submitToPriorityQueue(ctx context.Context, t *task.Task, opts *TaskOptions) (string, error) {
	priority := opts.Priority
	if priority == 0 {
		priority = 5 // 默认优先级
	}

	t.SetPriority(priority)
	if err := q.redis.UpdateTask(ctx, t); err != nil {
		q.logger.Sugar().Infof("更新任务优先级失败: %v", err)
	}

	if err := q.redis.EnqueueTaskWithPriority(ctx, t.ID, priority); err != nil {
		return "", fmt.Errorf("优先级任务入队失败: %w", err)
	}
	q.logger.Sugar().Infof("优先级队列任务已提交: %s (优先级: %d)", t.ID, priority)
	return t.ID, nil
}

// Start 启动队列
func (q *Queue) Start(ctx context.Context) error {
	q.mu.Lock()
	if q.running {
		q.mu.Unlock()
		return fmt.Errorf("队列已在运行")
	}
	q.running = true
	q.mu.Unlock()

	q.logger.Sugar().Infof("启动 %s 队列中...", q.queueType)

	// 只有延迟队列需要调度器
	if q.queueType == models.DelayedQueue {
		q.wg.Add(1)
		go q.runDelayedTaskScheduler(ctx)
	}

	// 启动工作器
	for i := 0; i < q.workerNumber; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		wk := worker.NewWorker(ctx, q.logger, workerID, q.queueType, q.redis)

		q.workers[workerID] = wk
		q.wg.Add(1)
		go func() {
			defer q.wg.Done()
			go wk.Run(ctx)
		}()
	}

	q.logger.Sugar().Infof("%s 队列已启动，工作器数量: %d", q.queueType, q.workerNumber)
	return nil
}

// Stop 停止队列
func (q *Queue) Stop() {
	q.mu.Lock()
	if !q.running {
		q.mu.Unlock()
		return
	}
	q.running = false
	q.mu.Unlock()

	q.logger.Sugar().Infof("正在停止 %s 队列...", q.queueType)

	// 停止所有工作器
	for _, worker := range q.workers {
		worker.Close()
	}

	// 发送停止信号
	close(q.stopCh)

	// 等待所有 goroutine 结束
	q.wg.Wait()

	q.logger.Sugar().Infof("%s 队列已停止", q.queueType)
}

// runDelayedTaskScheduler 运行延迟任务调度器 (仅延迟队列使用)
func (q *Queue) runDelayedTaskScheduler(ctx context.Context) {
	defer q.wg.Done()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	log.Println("延迟任务调度器已启动")

	for {
		select {
		case <-q.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			// 对于延迟队列，检查到期任务并直接执行
			if err := q.processExpiredDelayedTasks(ctx); err != nil {
				q.logger.Sugar().Infof("处理到期延迟任务失败: %v", err)
			}
		}
	}
}

// processExpiredDelayedTasks 处理到期的延迟任务
func (q *Queue) processExpiredDelayedTasks(ctx context.Context) error {
	// 获取到期的任务
	delayedKey := "task:queue:delayed"
	now := time.Now().Unix() * 1000

	tasks, err := q.redis.GetClient().ZRangeByScore(ctx, delayedKey, &goredis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%d", now),
	}).Result()

	if err != nil || len(tasks) == 0 {
		return err
	}

	// 移动到期任务供工作器处理
	pipe := q.redis.GetClient().Pipeline()
	for _, taskID := range tasks {
		// 添加到待处理列表
		pipe.LPush(ctx, "task:queue:ready", taskID)
		// 从延迟队列移除
		pipe.ZRem(ctx, delayedKey, taskID)
	}

	_, err = pipe.Exec(ctx)
	if err == nil && len(tasks) > 0 {
		q.logger.Sugar().Infof("已处理 %d 个到期延迟任务", len(tasks))
	}
	return err
}

// GetStats 获取队列统计信息
func (q *Queue) GetStats(ctx context.Context) (map[string]interface{}, error) {
	stats := map[string]interface{}{
		"queue_type": string(q.queueType),
		"workers":    len(q.workers),
		"handlers":   len(q.handlers),
		"running":    q.running,
	}

	// 根据队列类型获取不同的统计信息
	switch q.queueType {
	case models.SimpleQueue:
		length, err := q.redis.GetQueueLength(ctx)
		if err != nil {
			return nil, err
		}
		stats["queue_length"] = length

	case models.DelayedQueue:
		delayedCount, err := q.redis.GetDelayedTaskCount(ctx)
		if err != nil {
			return nil, err
		}
		stats["delayed_tasks"] = delayedCount

		// 获取ready队列长度
		readyLength, err := q.redis.GetClient().LLen(ctx, "task:queue:ready").Result()
		if err != nil {
			return nil, err
		}
		stats["ready_tasks"] = readyLength

	case models.PriorityQueue:
		priorityLength, err := q.redis.GetClient().ZCard(ctx, "task:queue:priority").Result()
		if err != nil {
			return nil, err
		}
		stats["priority_queue_length"] = priorityLength
	}

	return stats, nil
}

// GetQueueType 获取队列类型
func (q *Queue) GetQueueType() models.QueueType {
	return q.queueType
}
