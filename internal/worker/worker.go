package worker

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
	"sync"
	"time"

	"github.com/minhyannv/task-go/internal/handler_manager"
	"github.com/minhyannv/task-go/internal/models"
	"github.com/minhyannv/task-go/internal/task"
	"github.com/minhyannv/task-go/internal/task_manager"
	"go.uber.org/zap"
)

// Worker 队列工作器
type Worker struct {
	ctx            context.Context
	logger         *zap.Logger
	id             string
	queueType      models.QueueType
	taskManager    *task_manager.TaskManager
	handlerManager *handler_manager.HandlerManager
	mu             sync.RWMutex
	running        bool
	stopCh         chan struct{}
}

func NewWorker(ctx context.Context, logger *zap.Logger, id string, queueType models.QueueType, taskManager *task_manager.TaskManager, handlerManager *handler_manager.HandlerManager) *Worker {
	return &Worker{
		ctx:            ctx,
		logger:         logger.With(zap.String("worker_id", id)),
		id:             id,
		queueType:      queueType,
		taskManager:    taskManager,
		handlerManager: handlerManager,
		running:        false,
		stopCh:         make(chan struct{}),
	}
}

// Run 工作器运行
func (w *Worker) Run(ctx context.Context) {
	if w.running == true {
		w.logger.Sugar().Warnf("工作器: %s 已经在运行", w.id)
		return
	}
	w.running = true
	w.logger.Sugar().Infof("工作器 %s 启动 (队列类型: %s)", w.id, w.queueType)

	for {
		select {
		case <-w.stopCh:
			w.logger.Sugar().Infof("工作器 %s 收到停止信号", w.id)
			return
		case <-ctx.Done():
			w.logger.Sugar().Infof("工作器 %s 收到上下文取消信号", w.id)
			return
		default:
			if err := w.processNextTask(ctx); err != nil {
				w.logger.Sugar().Errorf("w.processNextTask error: %v", err)
				// 短暂休眠避免过度轮询
				select {
				case <-time.After(time.Second):
				case <-w.stopCh:
					w.logger.Sugar().Warnf("chan stop, return")
					return
				case <-ctx.Done():
					w.logger.Sugar().Warnf("ctx done, return")
					return
				}
			}
		}
	}
}

// processNextTask 处理下一个任务
func (w *Worker) processNextTask(ctx context.Context) error {
	var taskID string
	var err error

	// 根据队列类型获取任务
	switch w.queueType {
	case models.SimpleQueue:
		taskID, err = w.taskManager.DequeueTask(ctx, time.Second)
	case models.DelayedQueue:
		// 延迟队列从ready队列获取已到期的任务
		taskID, err = w.taskManager.DequeueFromReadyQueue(ctx, time.Second)
	case models.PriorityQueue:
		taskID, err = w.taskManager.DequeueTaskWithPriority(ctx, time.Second)
	default:
		return fmt.Errorf("不支持的队列类型: %s", w.queueType)
	}

	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil // 队列为空，继续轮询
		}
		w.logger.Sugar().Errorf("获取任务失败: %v", err)
		return fmt.Errorf("获取任务失败: %w", err)
	}

	// 获取任务详情
	t, err := w.taskManager.GetTask(ctx, taskID)
	if err != nil {
		w.logger.Sugar().Errorf("获取任务 %s 详情失败: %v", taskID, err)
		return fmt.Errorf("获取任务 %s 详情失败: %v", taskID, err)
	}
	w.logger.Sugar().Infof("获取任务信息 : %+v", t)

	// 查找处理器
	handler, exists := w.handlerManager.GetHandler(t.Type)
	if !exists {
		w.logger.Sugar().Infof("未找到任务类型 %s 的处理器: %s", taskID, t.Type)
		// 更新任务状态
		err = w.taskManager.TaskError(ctx, taskID, fmt.Sprintf("未找到处理器: %s", t.Type))
		if err != nil {
			w.logger.Sugar().Errorf("更新任务失败状态失败: %v", err)
		}
		return nil
	}

	// 执行任务
	return w.executeTaskWithRetry(ctx, t, handler)
}

// executeTaskWithRetry 带重试的任务执行
func (w *Worker) executeTaskWithRetry(ctx context.Context, t *task.Task, handler task.TaskHandler) error {
	maxRetries := t.GetRetry()
	timeout := t.GetTimeout()

	// 更新任务状态：运行中
	if err := w.taskManager.TaskRun(ctx, t.ID); err != nil {
		w.logger.Sugar().Errorf("更新任务状态失败: %v", err)
		return err
	}

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if attempt > 0 {
			w.logger.Sugar().Infof("任务 %s 第 %d 次重试", t.ID, attempt)
			// 指数退避
			backoff := time.Duration(attempt*attempt) * time.Second
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		// 执行任务
		res, err := w.executeTaskOnce(ctx, t, handler, timeout)
		if err == nil {
			// 更新任务状态：已完成
			if updateErr := w.taskManager.TaskSuccess(ctx, t.ID, res); updateErr != nil {
				w.logger.Sugar().Errorf("更新任务完成状态失败: %v", updateErr)
			}
			w.logger.Sugar().Infof("任务 %s 执行成功 (队列: %s)", t.ID, w.queueType)
			return nil
		}

		w.logger.Sugar().Errorf("任务 %s 执行失败 (第 %d/%d 次): %v", t.ID, attempt+1, maxRetries+1, err)

		// 最后一次尝试失败
		if attempt == maxRetries {
			// 更新任务状态：已失败
			err = w.taskManager.TaskError(ctx, t.ID, err.Error())
			if err != nil {
				w.logger.Sugar().Errorf("更新任务失败状态失败: %v", err)
			}
			return fmt.Errorf("任务 %s 最终执行失败: %w", t.ID, err)
		}
	}

	return nil
}

// executeTaskOnce 执行任务一次
func (w *Worker) executeTaskOnce(ctx context.Context, t *task.Task, handler task.TaskHandler, timeout time.Duration) (string, error) {
	// 创建带超时的上下文
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// 执行任务
	resChan := make(chan string, 1)
	errChan := make(chan error, 1)
	go func() {
		res, err := handler(timeoutCtx, t)
		if err != nil {
			errChan <- err
		}
		resChan <- res
	}()

	select {
	case err := <-errChan:
		return "", err
	case res := <-resChan:
		return res, nil
	case <-timeoutCtx.Done():
		return "", fmt.Errorf("任务执行超时")
	}
}

func (w *Worker) Stop() {
	close(w.stopCh)
}
