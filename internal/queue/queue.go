package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/minhyannv/task-go/internal/redis"
	"github.com/minhyannv/task-go/pkg/task"

	goredis "github.com/redis/go-redis/v9"
)

// QueueType 队列类型
type QueueType string

const (
	SimpleQueue   QueueType = "simple"   // 简单队列 (FIFO)
	DelayedQueue  QueueType = "delayed"  // 延迟队列 (时间调度)
	PriorityQueue QueueType = "priority" // 优先级队列 (优先级调度)
)

// AsyncQueue 异步任务队列
type AsyncQueue struct {
	redis    *redis.Client
	workers  map[string]*QueueWorker
	handlers map[string]task.TaskHandler
	running  bool
	stopCh   chan struct{}
	wg       sync.WaitGroup
	mu       sync.RWMutex

	// 配置选项
	defaultRetry   int
	defaultTimeout time.Duration
	maxWorkers     int
	queueType      QueueType // 队列类型
}

// QueueOptions 队列配置选项
type QueueOptions struct {
	DefaultRetry   int           // 默认重试次数
	DefaultTimeout time.Duration // 默认超时时间
	MaxWorkers     int           // 最大工作器数量
	QueueType      QueueType     // 队列类型
}

// QueueWorker 队列工作器
type QueueWorker struct {
	id          string
	queue       *AsyncQueue
	concurrency int
	running     bool
	stopCh      chan struct{}
	wg          sync.WaitGroup
}

// JobOptions 任务选项
type JobOptions struct {
	Delay     time.Duration // 延迟执行时间 (仅延迟队列有效)
	Retry     int           // 重试次数
	Timeout   time.Duration // 超时时间
	Priority  int           // 优先级 (仅优先级队列有效, 1-10, 10最高)
	UniqueKey string        // 唯一键，防重复
}

// NewAsyncQueue 创建异步任务队列
func NewAsyncQueue(redisClient *redis.Client, opts *QueueOptions) *AsyncQueue {
	if opts == nil {
		opts = &QueueOptions{
			DefaultRetry:   3,
			DefaultTimeout: 30 * time.Second,
			MaxWorkers:     5,
			QueueType:      SimpleQueue, // 默认使用简单队列
		}
	}

	// 设置默认队列类型
	if opts.QueueType == "" {
		opts.QueueType = SimpleQueue
	}

	return &AsyncQueue{
		redis:          redisClient,
		workers:        make(map[string]*QueueWorker),
		handlers:       make(map[string]task.TaskHandler),
		stopCh:         make(chan struct{}),
		defaultRetry:   opts.DefaultRetry,
		defaultTimeout: opts.DefaultTimeout,
		maxWorkers:     opts.MaxWorkers,
		queueType:      opts.QueueType,
	}
}

// RegisterHandler 注册任务处理器
func (q *AsyncQueue) RegisterHandler(jobType string, handler task.TaskHandler) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.handlers[jobType] = handler
	log.Printf("已注册任务处理器: %s (队列类型: %s)", jobType, q.queueType)
}

// Submit 提交任务到指定类型的队列
func (q *AsyncQueue) Submit(ctx context.Context, jobType string, payload interface{}, opts *JobOptions) (string, error) {
	// 序列化载荷
	var payloadStr string
	switch v := payload.(type) {
	case string:
		payloadStr = v
	case []byte:
		payloadStr = string(v)
	default:
		data, err := json.Marshal(payload)
		if err != nil {
			return "", fmt.Errorf("序列化载荷失败: %w", err)
		}
		payloadStr = string(data)
	}

	// 设置默认选项
	if opts == nil {
		opts = &JobOptions{}
	}
	if opts.Retry == 0 {
		opts.Retry = q.defaultRetry
	}
	if opts.Timeout == 0 {
		opts.Timeout = q.defaultTimeout
	}

	// 创建任务
	t := task.NewTask(jobType, payloadStr)
	t.SetRetryCount(opts.Retry)
	t.SetTimeout(opts.Timeout)

	if opts.UniqueKey != "" {
		t.SetUniqueKey(opts.UniqueKey)
	}

	// 检查唯一性
	if opts.UniqueKey != "" {
		exists, err := q.redis.CheckUniqueJob(ctx, opts.UniqueKey)
		if err != nil {
			return "", fmt.Errorf("检查任务唯一性失败: %w", err)
		}
		if exists {
			return "", fmt.Errorf("任务已存在: %s", opts.UniqueKey)
		}
	}

	// 保存任务
	if err := q.redis.SaveTask(ctx, t); err != nil {
		return "", fmt.Errorf("保存任务失败: %w", err)
	}

	// 根据队列类型提交任务
	switch q.queueType {
	case SimpleQueue:
		return q.submitToSimpleQueue(ctx, t)
	case DelayedQueue:
		return q.submitToDelayedQueue(ctx, t, opts)
	case PriorityQueue:
		return q.submitToPriorityQueue(ctx, t, opts)
	default:
		return "", fmt.Errorf("不支持的队列类型: %s", q.queueType)
	}
}

// submitToSimpleQueue 提交到简单队列 (FIFO)
func (q *AsyncQueue) submitToSimpleQueue(ctx context.Context, t *task.Task) (string, error) {
	if err := q.redis.EnqueueTask(ctx, t.ID); err != nil {
		return "", fmt.Errorf("任务入队失败: %w", err)
	}
	log.Printf("简单队列任务已提交: %s", t.ID)
	return t.ID, nil
}

// submitToDelayedQueue 提交到延迟队列
func (q *AsyncQueue) submitToDelayedQueue(ctx context.Context, t *task.Task, opts *JobOptions) (string, error) {
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
		log.Printf("延迟队列任务已提交: %s, 将于 %v 执行", t.ID, executeAt.Format("2006-01-02 15:04:05"))
	} else {
		log.Printf("延迟队列任务已提交: %s (立即执行)", t.ID)
	}
	return t.ID, nil
}

// submitToPriorityQueue 提交到优先级队列
func (q *AsyncQueue) submitToPriorityQueue(ctx context.Context, t *task.Task, opts *JobOptions) (string, error) {
	priority := opts.Priority
	if priority == 0 {
		priority = 5 // 默认优先级
	}

	t.SetPriority(priority)
	if err := q.redis.UpdateTask(ctx, t); err != nil {
		log.Printf("更新任务优先级失败: %v", err)
	}

	if err := q.redis.EnqueueTaskWithPriority(ctx, t.ID, priority); err != nil {
		return "", fmt.Errorf("优先级任务入队失败: %w", err)
	}
	log.Printf("优先级队列任务已提交: %s (优先级: %d)", t.ID, priority)
	return t.ID, nil
}

// 便捷方法
func (q *AsyncQueue) SubmitSimple(ctx context.Context, jobType string, payload interface{}) (string, error) {
	return q.Submit(ctx, jobType, payload, nil)
}

func (q *AsyncQueue) SubmitWithDelay(ctx context.Context, jobType string, payload interface{}, delay time.Duration) (string, error) {
	return q.Submit(ctx, jobType, payload, &JobOptions{Delay: delay})
}

func (q *AsyncQueue) SubmitWithPriority(ctx context.Context, jobType string, payload interface{}, priority int) (string, error) {
	return q.Submit(ctx, jobType, payload, &JobOptions{Priority: priority})
}

// Start 启动队列
func (q *AsyncQueue) Start(ctx context.Context) error {
	q.mu.Lock()
	if q.running {
		q.mu.Unlock()
		return fmt.Errorf("队列已在运行")
	}
	q.running = true
	q.mu.Unlock()

	log.Printf("启动 %s 队列中...", q.queueType)

	// 只有延迟队列需要调度器
	if q.queueType == DelayedQueue {
		q.wg.Add(1)
		go q.runDelayedTaskScheduler(ctx)
	}

	// 启动工作器
	for i := 0; i < q.maxWorkers; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		worker := &QueueWorker{
			id:          workerID,
			queue:       q,
			concurrency: 1,
			stopCh:      make(chan struct{}),
		}

		q.workers[workerID] = worker
		q.wg.Add(1)
		go worker.run(ctx)
	}

	log.Printf("%s 队列已启动，工作器数量: %d", q.queueType, q.maxWorkers)
	return nil
}

// Stop 停止队列
func (q *AsyncQueue) Stop() {
	q.mu.Lock()
	if !q.running {
		q.mu.Unlock()
		return
	}
	q.running = false
	q.mu.Unlock()

	log.Printf("正在停止 %s 队列...", q.queueType)

	// 停止所有工作器
	for _, worker := range q.workers {
		close(worker.stopCh)
	}

	// 发送停止信号
	close(q.stopCh)

	// 等待所有 goroutine 结束
	q.wg.Wait()

	log.Printf("%s 队列已停止", q.queueType)
}

// runDelayedTaskScheduler 运行延迟任务调度器 (仅延迟队列使用)
func (q *AsyncQueue) runDelayedTaskScheduler(ctx context.Context) {
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
				log.Printf("处理到期延迟任务失败: %v", err)
			}
		}
	}
}

// processExpiredDelayedTasks 处理到期的延迟任务
func (q *AsyncQueue) processExpiredDelayedTasks(ctx context.Context) error {
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
		log.Printf("已处理 %d 个到期延迟任务", len(tasks))
	}
	return err
}

// 工作器运行逻辑
func (w *QueueWorker) run(ctx context.Context) {
	defer w.queue.wg.Done()

	w.running = true
	log.Printf("工作器 %s 启动 (队列类型: %s)", w.id, w.queue.queueType)

	for {
		select {
		case <-w.stopCh:
			log.Printf("工作器 %s 收到停止信号", w.id)
			return
		case <-ctx.Done():
			log.Printf("工作器 %s 收到上下文取消信号", w.id)
			return
		default:
			if err := w.processNextTask(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				// 短暂休眠避免过度轮询
				select {
				case <-time.After(time.Second):
				case <-w.stopCh:
					return
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// processNextTask 处理下一个任务
func (w *QueueWorker) processNextTask(ctx context.Context) error {
	var taskID string
	var err error

	// 根据队列类型获取任务
	switch w.queue.queueType {
	case SimpleQueue:
		taskID, err = w.queue.redis.DequeueTask(ctx, time.Second)
	case DelayedQueue:
		// 延迟队列从ready队列获取已到期的任务
		taskID, err = w.queue.redis.DequeueFromReadyQueue(ctx, time.Second)
	case PriorityQueue:
		taskID, err = w.queue.redis.DequeueTaskWithPriority(ctx, time.Second)
	default:
		return fmt.Errorf("不支持的队列类型: %s", w.queue.queueType)
	}

	if err != nil {
		if err.Error() == "redis: nil" {
			return nil // 队列为空，继续轮询
		}
		return fmt.Errorf("获取任务失败: %w", err)
	}

	// 获取任务详情
	t, err := w.queue.redis.GetTask(ctx, taskID)
	if err != nil {
		log.Printf("获取任务 %s 详情失败: %v", taskID, err)
		return nil
	}

	// 检查任务状态
	if t.Status == task.StatusStopped {
		log.Printf("任务 %s 已被停止，跳过执行", taskID)
		return nil
	}

	// 查找处理器
	w.queue.mu.RLock()
	handler, exists := w.queue.handlers[t.Name]
	w.queue.mu.RUnlock()

	if !exists {
		log.Printf("未找到任务 %s 的处理器: %s", taskID, t.Name)
		_ = w.queue.redis.UpdateTaskError(ctx, taskID, fmt.Sprintf("未找到处理器: %s", t.Name))
		return nil
	}

	// 执行任务
	return w.executeTaskWithRetry(ctx, t, handler)
}

// executeTaskWithRetry 带重试的任务执行
func (w *QueueWorker) executeTaskWithRetry(ctx context.Context, t *task.Task, handler task.TaskHandler) error {
	maxRetries := t.GetRetryCount()
	timeout := t.GetTimeout()

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			log.Printf("任务 %s 第 %d 次重试", t.ID, attempt)

			// 指数退避
			backoff := time.Duration(attempt*attempt) * time.Second
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// 执行任务
		err := w.executeTaskOnce(ctx, t, handler, timeout)
		if err == nil {
			log.Printf("任务 %s 执行成功 (队列: %s)", t.ID, w.queue.queueType)
			return nil
		}

		log.Printf("任务 %s 执行失败 (第 %d/%d 次): %v", t.ID, attempt+1, maxRetries+1, err)

		// 最后一次尝试失败
		if attempt == maxRetries {
			_ = w.queue.redis.UpdateTaskError(ctx, t.ID, err.Error())
			return fmt.Errorf("任务 %s 最终执行失败: %w", t.ID, err)
		}
	}

	return nil
}

// executeTaskOnce 执行任务一次
func (w *QueueWorker) executeTaskOnce(ctx context.Context, t *task.Task, handler task.TaskHandler, timeout time.Duration) error {
	// 更新任务状态为运行中
	if err := w.queue.redis.UpdateTaskStatus(ctx, t.ID, task.StatusRunning); err != nil {
		log.Printf("更新任务状态失败: %v", err)
	}

	// 创建带超时的上下文
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 执行任务
	done := make(chan error, 1)
	go func() {
		done <- handler(timeoutCtx, t)
	}()

	select {
	case err := <-done:
		if err != nil {
			return err
		}

		// 标记任务完成
		if updateErr := w.queue.redis.UpdateTaskStatus(ctx, t.ID, task.StatusDone); updateErr != nil {
			log.Printf("更新任务完成状态失败: %v", updateErr)
		}
		return nil

	case <-timeoutCtx.Done():
		return fmt.Errorf("任务执行超时")
	}
}

// GetStats 获取队列统计信息
func (q *AsyncQueue) GetStats(ctx context.Context) (map[string]interface{}, error) {
	stats := map[string]interface{}{
		"queue_type": string(q.queueType),
		"workers":    len(q.workers),
		"handlers":   len(q.handlers),
		"running":    q.running,
	}

	// 根据队列类型获取不同的统计信息
	switch q.queueType {
	case SimpleQueue:
		length, err := q.redis.GetQueueLength(ctx)
		if err != nil {
			return nil, err
		}
		stats["queue_length"] = length

	case DelayedQueue:
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

	case PriorityQueue:
		priorityLength, err := q.redis.GetClient().ZCard(ctx, "task:queue:priority").Result()
		if err != nil {
			return nil, err
		}
		stats["priority_queue_length"] = priorityLength
	}

	return stats, nil
}

// GetQueueType 获取队列类型
func (q *AsyncQueue) GetQueueType() QueueType {
	return q.queueType
}
