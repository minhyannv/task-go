package task_manager

import (
	"context"
	"fmt"
	"github.com/minhyannv/task-go/internal/models"
	"time"

	"github.com/minhyannv/task-go/internal/task"

	"github.com/redis/go-redis/v9"
)

// TaskManager Redis 客户端封装
type TaskManager struct {
	redisClient *redis.Client
	taskPrefix  string // 任务键前缀
	queueKey    string // 队列键名
}

// NewTaskManager 创建新的 Redis 客户端
func NewTaskManager(redisClient *redis.Client) *TaskManager {
	return &TaskManager{
		redisClient: redisClient,
		taskPrefix:  "task:",
		queueKey:    "task:queue",
	}
}

// Close 关闭 Redis 连接
func (c *TaskManager) Close() error {
	return c.redisClient.Close()
}

// Ping 测试 Redis 连接
func (c *TaskManager) Ping(ctx context.Context) error {
	return c.redisClient.Ping(ctx).Err()
}

// SaveTask 保存任务到 Redis Hash
func (c *TaskManager) SaveTask(ctx context.Context, t *task.Task) error {
	key := c.taskKey(t.ID)
	return c.redisClient.HSet(ctx, key, t.ToMap()).Err()
}

// GetTask 从 Redis Hash 获取任务
func (c *TaskManager) GetTask(ctx context.Context, taskID string) (*task.Task, error) {
	key := c.taskKey(taskID)
	data, err := c.redisClient.HGetAll(ctx, key).Result()
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

// TaskRun 任务执行
func (c *TaskManager) TaskRun(ctx context.Context, taskID string) error {
	key := c.taskKey(taskID)
	now := time.Now().Unix()

	return c.redisClient.HSet(ctx, key, map[string]interface{}{
		"status":     string(models.StatusRunning),
		"updated_at": now,
		"started_at": now,
	}).Err()
}

// TaskSuccess 更新任务结果
func (c *TaskManager) TaskSuccess(ctx context.Context, taskID, result string) error {
	key := c.taskKey(taskID)
	now := time.Now().Unix()

	return c.redisClient.HSet(ctx, key, map[string]interface{}{
		"status":      string(models.StatusDone),
		"result":      result,
		"updated_at":  now,
		"finished_at": now,
	}).Err()
}

// TaskError 更新任务错误信息
func (c *TaskManager) TaskError(ctx context.Context, taskID, errorMsg string) error {
	key := c.taskKey(taskID)
	now := time.Now().Unix()

	return c.redisClient.HSet(ctx, key, map[string]interface{}{
		"status":      string(models.StatusFailed),
		"error_msg":   errorMsg,
		"updated_at":  now,
		"finished_at": now,
	}).Err()
}

// EnqueueTask 将任务加入队列
func (c *TaskManager) EnqueueTask(ctx context.Context, taskID string) error {
	return c.redisClient.LPush(ctx, c.queueKey, taskID).Err()
}

// DequeueTask 从队列中取出任务（阻塞式）
func (c *TaskManager) DequeueTask(ctx context.Context, timeout time.Duration) (string, error) {
	result, err := c.redisClient.BRPop(ctx, timeout, c.queueKey).Result()
	if err != nil {
		return "", err
	}

	if len(result) < 2 {
		return "", fmt.Errorf("队列返回数据格式错误")
	}

	return result[1], nil
}

// DeleteTask 删除任务
func (c *TaskManager) DeleteTask(ctx context.Context, taskID string) error {
	key := c.taskKey(taskID)

	// 使用管道删除任务数据和可能存在的队列项
	pipe := c.redisClient.Pipeline()
	pipe.Del(ctx, key)
	pipe.LRem(ctx, c.queueKey, 0, taskID)

	_, err := pipe.Exec(ctx)
	return err
}

// GetQueueLength 获取队列长度
func (c *TaskManager) GetQueueLength(ctx context.Context) (int64, error) {
	return c.redisClient.LLen(ctx, c.queueKey).Result()
}

// TaskExists 检查任务是否存在
func (c *TaskManager) TaskExists(ctx context.Context, taskID string) (bool, error) {
	key := c.taskKey(taskID)
	count, err := c.redisClient.Exists(ctx, key).Result()
	return count > 0, err
}

// ListTasks 列出所有任务（分页）
func (c *TaskManager) ListTasks(ctx context.Context, pattern string, cursor uint64, count int64) ([]string, uint64, error) {
	searchPattern := c.taskPrefix + pattern
	keys, nextCursor, err := c.redisClient.Scan(ctx, cursor, searchPattern, count).Result()
	if err != nil {
		return nil, 0, err
	}

	// 移除前缀，只返回任务ID
	taskIDs := make([]string, len(keys))
	for i, key := range keys {
		taskIDs[i] = key[len(c.taskPrefix):]
	}

	return taskIDs, nextCursor, nil
}

// taskKey 生成任务的 Redis 键
func (c *TaskManager) taskKey(taskID string) string {
	return c.taskPrefix + taskID
}

// GetRedisClient 获取原始 Redis 客户端（用于高级操作）
func (c *TaskManager) GetRedisClient() *redis.Client {
	return c.redisClient
}

// EnqueueTaskWithPriority 将任务加入优先级队列
func (c *TaskManager) EnqueueTaskWithPriority(ctx context.Context, taskID string, priority int) error {
	// 使用有序集合实现优先级队列，分数越高优先级越高
	score := float64(priority)
	queueKey := c.queueKey + ":priority"
	return c.redisClient.ZAdd(ctx, queueKey, redis.Z{
		Score:  score,
		Member: taskID,
	}).Err()
}

// DequeueTaskWithPriority 从优先级队列中取出任务
func (c *TaskManager) DequeueTaskWithPriority(ctx context.Context, timeout time.Duration) (string, error) {
	queueKey := c.queueKey + ":priority"

	// 使用 BZPOPMAX 命令获取最高优先级的任务
	result, err := c.redisClient.BZPopMax(ctx, timeout, queueKey).Result()
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

// EnqueueDelayedTask 将任务加入延迟队列
func (c *TaskManager) EnqueueDelayedTask(ctx context.Context, taskID string, executeAt time.Time, priority int) error {
	delayedKey := c.queueKey + ":delayed"
	// 使用执行时间的时间戳作为分数，优先级作为次要排序
	score := float64(executeAt.Unix()*1000 + int64(priority))
	return c.redisClient.ZAdd(ctx, delayedKey, redis.Z{
		Score:  score,
		Member: taskID,
	}).Err()
}

// MoveExpiredDelayedTasks 移动到期的延迟任务到执行队列
func (c *TaskManager) MoveExpiredDelayedTasks(ctx context.Context) error {
	delayedKey := c.queueKey + ":delayed"
	priorityKey := c.queueKey + ":priority"
	now := time.Now().Unix() * 1000

	// 获取所有到期的任务
	tasks, err := c.redisClient.ZRangeByScore(ctx, delayedKey, &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%d", now),
	}).Result()

	if err != nil || len(tasks) == 0 {
		return err
	}

	// 使用管道批量移动任务
	pipe := c.redisClient.Pipeline()
	for _, taskID := range tasks {
		// 获取任务优先级
		task, err := c.GetTask(ctx, taskID)
		if err != nil {
			continue
		}

		priority := task.GetPriority()

		// 移动到优先级队列
		pipe.ZAdd(ctx, priorityKey, redis.Z{
			Score:  float64(priority),
			Member: taskID,
		})

		// 从延迟队列中移除
		pipe.ZRem(ctx, delayedKey, taskID)
	}

	_, err = pipe.Exec(ctx)
	return err
}

// GetDelayedTaskCount 获取延迟任务数量
func (c *TaskManager) GetDelayedTaskCount(ctx context.Context) (int64, error) {
	delayedKey := c.queueKey + ":delayed"
	return c.redisClient.ZCard(ctx, delayedKey).Result()
}

// UpdateTask 更新任务信息
func (c *TaskManager) UpdateTask(ctx context.Context, t *task.Task) error {
	key := c.taskKey(t.ID)
	return c.redisClient.HSet(ctx, key, t.ToMap()).Err()
}

// DequeueFromReadyQueue 从ready队列中取出任务（延迟队列专用）
func (c *TaskManager) DequeueFromReadyQueue(ctx context.Context, timeout time.Duration) (string, error) {
	result, err := c.redisClient.BRPop(ctx, timeout, "task:queue:ready").Result()
	if err != nil {
		return "", err
	}

	if len(result) < 2 {
		return "", fmt.Errorf("队列返回数据格式错误")
	}

	return result[1], nil
}
