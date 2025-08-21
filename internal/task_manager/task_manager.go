package task_manager

import (
	"context"
	"fmt"
	"time"

	"github.com/minhyannv/task-go/internal/constants"
	"github.com/minhyannv/task-go/internal/task"
	"github.com/redis/go-redis/v9"
)

// TaskManager Redis 任务管理器
type TaskManager struct {
	redisClient *redis.Client
}

// NewTaskManager 创建新的任务管理器
func NewTaskManager(redisClient *redis.Client) *TaskManager {
	return &TaskManager{
		redisClient: redisClient,
	}
}

// Close 关闭 Redis 连接
func (tm *TaskManager) Close() error {
	return tm.redisClient.Close()
}

// Ping 测试 Redis 连接
func (tm *TaskManager) Ping(ctx context.Context) error {
	return tm.redisClient.Ping(ctx).Err()
}

// GetRedisClient 获取原始 Redis 客户端（用于高级操作）
func (tm *TaskManager) GetRedisClient() *redis.Client {
	return tm.redisClient
}

// === 任务生命周期管理 ===

// SaveTask 保存任务到 Redis Hash
func (tm *TaskManager) SaveTask(ctx context.Context, t *task.Task) error {
	key := tm.taskKey(t.ID)
	return tm.redisClient.HSet(ctx, key, t.ToMap()).Err()
}

// GetTask 从 Redis Hash 获取任务
func (tm *TaskManager) GetTask(ctx context.Context, taskID string) (*task.Task, error) {
	key := tm.taskKey(taskID)
	data, err := tm.redisClient.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("获取任务失败: %w", err)
	}

	if len(data) == 0 {
		return nil, fmt.Errorf("任务不存在: %s", taskID)
	}

	t := &task.Task{}
	if err := t.FromMap(data); err != nil {
		return nil, fmt.Errorf("解析任务数据失败: %w", err)
	}

	return t, nil
}

// UpdateTask 更新任务信息
func (tm *TaskManager) UpdateTask(ctx context.Context, t *task.Task) error {
	key := tm.taskKey(t.ID)
	return tm.redisClient.HSet(ctx, key, t.ToMap()).Err()
}

// DeleteTask 删除任务
func (tm *TaskManager) DeleteTask(ctx context.Context, taskID string) error {
	key := tm.taskKey(taskID)

	// 使用管道删除任务数据和可能存在的队列项
	pipe := tm.redisClient.Pipeline()
	pipe.Del(ctx, key)
	// 从所有可能的队列中移除
	pipe.LRem(ctx, constants.SimpleQueueKey, 0, taskID)
	pipe.ZRem(ctx, constants.DelayQueueKey, taskID)
	pipe.ZRem(ctx, constants.PriorityQueueKey, taskID)

	_, err := pipe.Exec(ctx)
	return err
}

// TaskExists 检查任务是否存在
func (tm *TaskManager) TaskExists(ctx context.Context, taskID string) (bool, error) {
	key := tm.taskKey(taskID)
	count, err := tm.redisClient.Exists(ctx, key).Result()
	return count > 0, err
}

// === 任务状态管理 ===

// TaskRun 标记任务开始执行
func (tm *TaskManager) TaskRun(ctx context.Context, taskID string) error {
	key := tm.taskKey(taskID)
	now := time.Now().Unix()

	return tm.redisClient.HSet(ctx, key, map[string]interface{}{
		"status":     constants.StatusRunning,
		"updated_at": now,
		"started_at": now,
	}).Err()
}

// TaskSuccess 标记任务执行成功
func (tm *TaskManager) TaskSuccess(ctx context.Context, taskID, result string) error {
	key := tm.taskKey(taskID)
	now := time.Now().Unix()

	return tm.redisClient.HSet(ctx, key, map[string]interface{}{
		"status":      constants.StatusDone,
		"result":      result,
		"updated_at":  now,
		"finished_at": now,
	}).Err()
}

// TaskError 标记任务执行失败
func (tm *TaskManager) TaskError(ctx context.Context, taskID, errorMsg string) error {
	key := tm.taskKey(taskID)
	now := time.Now().Unix()

	return tm.redisClient.HSet(ctx, key, map[string]interface{}{
		"status":      constants.StatusFailed,
		"error_msg":   errorMsg,
		"updated_at":  now,
		"finished_at": now,
	}).Err()
}

// === 简单队列操作 ===

// EnqueueSimpleTask 将任务加入简单队列
func (tm *TaskManager) EnqueueSimpleTask(ctx context.Context, t *task.Task) error {
	// 保存任务信息
	if err := tm.SaveTask(ctx, t); err != nil {
		return fmt.Errorf("保存任务失败: %w", err)
	}
	return tm.redisClient.LPush(ctx, constants.SimpleQueueKey, t.ID).Err()
}

// DequeueSimpleTask 从简单队列中取出任务（阻塞式）
func (tm *TaskManager) DequeueSimpleTask(ctx context.Context, timeout time.Duration) (string, error) {
	result, err := tm.redisClient.BRPop(ctx, timeout, constants.SimpleQueueKey).Result()
	if err != nil {
		return "", err
	}

	if len(result) < 2 {
		return "", fmt.Errorf("队列返回数据格式错误")
	}

	return result[1], nil
}

// GetSimpleQueueLength 获取简单队列长度
func (tm *TaskManager) GetSimpleQueueLength(ctx context.Context) (int64, error) {
	return tm.redisClient.LLen(ctx, constants.SimpleQueueKey).Result()
}

// === 延迟队列操作 ===

// EnqueueDelayTask 将任务加入延迟队列
func (tm *TaskManager) EnqueueDelayTask(ctx context.Context, t *task.Task) error {
	// 保存任务信息
	if err := tm.SaveTask(ctx, t); err != nil {
		return fmt.Errorf("保存任务失败: %w", err)
	}
	// 使用执行时间的秒时间戳作为分数
	executeAt := time.Now().Add(t.Delay)
	score := float64(executeAt.Unix())
	return tm.redisClient.ZAdd(ctx, constants.DelayQueueKey, redis.Z{
		Score:  score,
		Member: t.ID,
	}).Err()
}

// DequeueDelayTask 从延迟队列中取出到期的任务
func (tm *TaskManager) DequeueDelayTask(ctx context.Context, timeout time.Duration) (string, error) {
	// 使用循环检查是否有到期的任务
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		// 获取当前时间戳，只取出已到期的任务
		now := float64(time.Now().Unix())

		// 使用 ZRANGEBYSCORE 获取到期的任务（非阻塞）
		results, err := tm.redisClient.ZRangeByScoreWithScores(ctx, constants.DelayQueueKey, &redis.ZRangeBy{
			Min:   "-inf",
			Max:   fmt.Sprintf("%.0f", now),
			Count: 1,
		}).Result()

		if err != nil {
			return "", fmt.Errorf("查询延迟任务失败: %w", err)
		}

		if len(results) == 0 {
			// 没有到期任务，短暂休眠后继续检查
			select {
			case <-time.After(100 * time.Millisecond):
				continue
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}

		// 找到到期任务，尝试原子性地移除它
		taskID, ok := results[0].Member.(string)
		if !ok {
			return "", fmt.Errorf("任务ID格式错误")
		}

		// 使用 ZREM 原子性地移除任务，避免重复处理
		removed, err := tm.redisClient.ZRem(ctx, constants.DelayQueueKey, taskID).Result()
		if err != nil {
			return "", fmt.Errorf("移除延迟任务失败: %w", err)
		}

		if removed > 0 {
			// 成功移除，返回任务ID
			return taskID, nil
		}

		// 任务已被其他worker处理，继续查找下一个
		continue
	}

	// 超时，没有找到到期任务
	return "", redis.Nil
}

// GetDelayTaskCount 获取延迟任务数量
func (tm *TaskManager) GetDelayTaskCount(ctx context.Context) (int64, error) {
	return tm.redisClient.ZCard(ctx, constants.DelayQueueKey).Result()
}

// === 优先级队列操作 ===

// EnqueuePriorityTask 将任务加入优先级队列
func (tm *TaskManager) EnqueuePriorityTask(ctx context.Context, t *task.Task) error {
	// 保存任务信息
	if err := tm.SaveTask(ctx, t); err != nil {
		return fmt.Errorf("保存任务失败: %w", err)
	}

	// 使用有序集合实现优先级队列，分数越高优先级越高
	score := float64(t.Priority)
	return tm.redisClient.ZAdd(ctx, constants.PriorityQueueKey, redis.Z{
		Score:  score,
		Member: t.ID,
	}).Err()
}

// DequeuePriorityTask 从优先级队列中取出任务
func (tm *TaskManager) DequeuePriorityTask(ctx context.Context, timeout time.Duration) (string, error) {
	// 使用 BZPOPMAX 命令获取最高优先级的任务
	result, err := tm.redisClient.BZPopMax(ctx, timeout, constants.PriorityQueueKey).Result()
	if err != nil {
		return "", err
	}

	if result == nil {
		return "", fmt.Errorf("队列为空")
	}

	memberStr, ok := result.Member.(string)
	if !ok || memberStr == "" {
		return "", fmt.Errorf("队列返回数据格式错误")
	}

	return memberStr, nil
}

// GetPriorityTaskCount 获取优先级任务数量
func (tm *TaskManager) GetPriorityTaskCount(ctx context.Context) (int64, error) {
	return tm.redisClient.ZCard(ctx, constants.PriorityQueueKey).Result()
}

// taskKey 生成任务的 Redis key
func (tm *TaskManager) taskKey(taskID string) string {
	return constants.TaskPrefix + taskID
}
