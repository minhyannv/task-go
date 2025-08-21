package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/minhyannv/task-go/internal/constants"
	"github.com/minhyannv/task-go/internal/handler_manager"
	"github.com/minhyannv/task-go/internal/models"
	"github.com/minhyannv/task-go/internal/task_manager"
	"github.com/minhyannv/task-go/internal/worker"
	"go.uber.org/zap"

	"github.com/minhyannv/task-go/internal/task"
)

// Queue 队列
type Queue struct {
	ctx            context.Context
	logger         *zap.Logger
	taskManager    *task_manager.TaskManager
	handlerManager *handler_manager.HandlerManager

	queueType models.QueueType
	workers   map[string]*worker.Worker
	running   bool
	stopCh    chan struct{}
	wg        sync.WaitGroup
	mu        sync.RWMutex

	// 配置选项
	retry           int           // 任务执行失败重试次数
	timeout         time.Duration // 任务执行超时时间
	defaultDelay    time.Duration // 延迟执行时间
	defaultPriority int           // 默认优先级

	workerNumber int // 工作器数量
}

// NewQueue 创建队列
func NewQueue(ctx context.Context, logger *zap.Logger, taskManager *task_manager.TaskManager, handlerManager *handler_manager.HandlerManager, queueType models.QueueType, workerNumber int) *Queue {
	return &Queue{
		ctx:             ctx,
		logger:          logger,
		taskManager:     taskManager,
		handlerManager:  handlerManager,
		workers:         make(map[string]*worker.Worker),
		stopCh:          make(chan struct{}),
		retry:           constants.DefaultRetryCount,
		timeout:         constants.DefaultTimeoutSec * time.Second,
		defaultDelay:    constants.DefaultDelaySec * time.Second,
		defaultPriority: constants.DefaultPriority,
		workerNumber:    workerNumber,
		queueType:       queueType,
	}
}

// SubmitTask 提交任务
func (q *Queue) SubmitTask(ctx context.Context, taskType string, payload string, delay time.Duration, priority int) error {
	// 创建任务
	t := &task.Task{
		ID:        uuid.New().String(),
		Type:      taskType,
		QueueType: q.queueType,
		Payload:   payload,
		Status:    models.StatusPending,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		Retry:     q.retry,
		Timeout:   q.timeout,
		Delay:     delay,
		Priority:  priority,
	}

	// 根据队列类型提交任务
	switch t.QueueType {
	case models.SimpleQueue:
		return q.taskManager.EnqueueSimpleTask(ctx, t)
	case models.DelayQueue:
		return q.taskManager.EnqueueDelayTask(ctx, t)
	case models.PriorityQueue:
		return q.taskManager.EnqueuePriorityTask(ctx, t)
	default:
		return fmt.Errorf("不支持的队列类型: %s", t.QueueType)
	}
}

// Start 启动队列
func (q *Queue) Start(ctx context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.running {
		return fmt.Errorf("队列已在运行")
	}
	q.running = true

	q.logger.Sugar().Infof("启动队列中...")

	// 启动工作器
	for i := 0; i < q.workerNumber; i++ {
		workerID := fmt.Sprintf("worker-%d", i+1)
		wk := worker.NewWorker(ctx, q.logger, workerID, q.queueType, q.taskManager, q.handlerManager)

		q.workers[workerID] = wk
		q.wg.Add(1)
		go func(w *worker.Worker) {
			defer q.wg.Done()
			w.Run(ctx)
		}(wk)
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
	for _, wk := range q.workers {
		wk.Stop()
	}

	// 发送停止信号
	close(q.stopCh)

	// 等待所有 goroutine 结束
	q.wg.Wait()

	q.logger.Sugar().Infof("%s 队列已停止", q.queueType)
}

// GetStats 获取队列统计信息
func (q *Queue) GetStats(ctx context.Context) (map[string]interface{}, error) {
	stats := map[string]interface{}{
		"queue_type": string(q.queueType),
		"workers":    len(q.workers),
		"handlers":   q.handlerManager.GetHandlerCount(),
		"running":    q.running,
	}

	// 根据队列类型获取不同的统计信息
	switch q.queueType {
	case models.SimpleQueue:
		length, err := q.taskManager.GetSimpleQueueLength(ctx)
		if err != nil {
			return nil, err
		}
		stats["simple_queue_length"] = length

	case models.DelayQueue:
		delayCount, err := q.taskManager.GetDelayTaskCount(ctx)
		if err != nil {
			return nil, err
		}
		stats["delay_queue_length"] = delayCount

	case models.PriorityQueue:
		priorityLength, err := q.taskManager.GetPriorityTaskCount(ctx)
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

// GetHandlerManager 获取处理器管理器
func (q *Queue) GetHandlerManager() *handler_manager.HandlerManager {
	return q.handlerManager
}
