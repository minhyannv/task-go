package task

import (
	"context"
	"encoding/json"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// TaskStatus 任务状态枚举
type TaskStatus string

const (
	StatusPending TaskStatus = "pending"
	StatusRunning TaskStatus = "running"
	StatusStopped TaskStatus = "stopped"
	StatusDone    TaskStatus = "done"
	StatusFailed  TaskStatus = "failed"
)

// Task 任务结构体
type Task struct {
	ID         string     `json:"id"`          // 任务唯一标识
	Name       string     `json:"name"`        // 任务名称
	Payload    string     `json:"payload"`     // 任务载荷（JSON 字符串）
	Status     TaskStatus `json:"status"`      // 任务状态
	CreatedAt  time.Time  `json:"created_at"`  // 创建时间
	UpdatedAt  time.Time  `json:"updated_at"`  // 更新时间
	StartedAt  *time.Time `json:"started_at"`  // 开始执行时间
	FinishedAt *time.Time `json:"finished_at"` // 完成时间
	Result     string     `json:"result"`      // 执行结果
	ErrorMsg   string     `json:"error_msg"`   // 错误信息

	// 新增字段
	RetryCount int           `json:"retry_count"` // 重试次数
	Timeout    time.Duration `json:"timeout"`     // 超时时间
	Priority   int           `json:"priority"`    // 优先级
	UniqueKey  string        `json:"unique_key"`  // 唯一键
}

// TaskHandler 任务处理函数类型
type TaskHandler func(ctx context.Context, task *Task) error

// NewTask 创建新任务
func NewTask(name, payload string) *Task {
	now := time.Now()
	return &Task{
		ID:        uuid.New().String(),
		Name:      name,
		Payload:   payload,
		Status:    StatusPending,
		CreatedAt: now,
		UpdatedAt: now,
	}
}

// ToMap 将任务转换为 map，用于存储到 Redis Hash
func (t *Task) ToMap() map[string]interface{} {
	data := map[string]interface{}{
		"id":          t.ID,
		"name":        t.Name,
		"payload":     t.Payload,
		"status":      string(t.Status),
		"created_at":  t.CreatedAt.Unix(),
		"updated_at":  t.UpdatedAt.Unix(),
		"result":      t.Result,
		"error_msg":   t.ErrorMsg,
		"retry_count": t.RetryCount,
		"timeout":     int64(t.Timeout.Seconds()),
		"priority":    t.Priority,
		"unique_key":  t.UniqueKey,
	}

	if t.StartedAt != nil {
		data["started_at"] = t.StartedAt.Unix()
	}
	if t.FinishedAt != nil {
		data["finished_at"] = t.FinishedAt.Unix()
	}

	return data
}

// FromMap 从 map 构建任务对象，用于从 Redis Hash 读取
func (t *Task) FromMap(data map[string]string) error {
	t.ID = data["id"]
	t.Name = data["name"]
	t.Payload = data["payload"]
	t.Status = TaskStatus(data["status"])
	t.Result = data["result"]
	t.ErrorMsg = data["error_msg"]
	t.UniqueKey = data["unique_key"]

	// 解析数值字段
	if retryStr, ok := data["retry_count"]; ok && retryStr != "" {
		if retry, err := strconv.Atoi(retryStr); err == nil {
			t.RetryCount = retry
		}
	}

	if timeoutStr, ok := data["timeout"]; ok && timeoutStr != "" {
		if timeout, err := strconv.ParseInt(timeoutStr, 10, 64); err == nil {
			t.Timeout = time.Duration(timeout) * time.Second
		}
	}

	if priorityStr, ok := data["priority"]; ok && priorityStr != "" {
		if priority, err := strconv.Atoi(priorityStr); err == nil {
			t.Priority = priority
		}
	}

	// 解析时间戳
	if createdStr, ok := data["created_at"]; ok && createdStr != "" {
		if created, err := strconv.ParseInt(createdStr, 10, 64); err == nil {
			t.CreatedAt = time.Unix(created, 0)
		}
	}

	if updatedStr, ok := data["updated_at"]; ok && updatedStr != "" {
		if updated, err := strconv.ParseInt(updatedStr, 10, 64); err == nil {
			t.UpdatedAt = time.Unix(updated, 0)
		}
	}

	if startedStr, ok := data["started_at"]; ok && startedStr != "" {
		if started, err := strconv.ParseInt(startedStr, 10, 64); err == nil {
			startedTime := time.Unix(started, 0)
			t.StartedAt = &startedTime
		}
	}

	if finishedStr, ok := data["finished_at"]; ok && finishedStr != "" {
		if finished, err := strconv.ParseInt(finishedStr, 10, 64); err == nil {
			finishedTime := time.Unix(finished, 0)
			t.FinishedAt = &finishedTime
		}
	}

	return nil
}

// ToJSON 将任务转换为 JSON 字符串
func (t *Task) ToJSON() (string, error) {
	data, err := json.Marshal(t)
	return string(data), err
}

// UpdateStatus 更新任务状态
func (t *Task) UpdateStatus(status TaskStatus) {
	t.Status = status
	t.UpdatedAt = time.Now()

	switch status {
	case StatusRunning:
		now := time.Now()
		t.StartedAt = &now
	case StatusDone, StatusFailed:
		now := time.Now()
		t.FinishedAt = &now
	}
}

// SetResult 设置任务执行结果
func (t *Task) SetResult(result string) {
	t.Result = result
	t.UpdatedAt = time.Now()
}

// SetError 设置任务错误信息
func (t *Task) SetError(errMsg string) {
	t.ErrorMsg = errMsg
	t.UpdateStatus(StatusFailed)
}

// SetRetryCount 设置重试次数
func (t *Task) SetRetryCount(count int) {
	t.RetryCount = count
}

// GetRetryCount 获取重试次数
func (t *Task) GetRetryCount() int {
	return t.RetryCount
}

// SetTimeout 设置超时时间
func (t *Task) SetTimeout(timeout time.Duration) {
	t.Timeout = timeout
}

// GetTimeout 获取超时时间
func (t *Task) GetTimeout() time.Duration {
	if t.Timeout == 0 {
		return 30 * time.Second // 默认30秒
	}
	return t.Timeout
}

// SetPriority 设置优先级
func (t *Task) SetPriority(priority int) {
	t.Priority = priority
}

// GetPriority 获取优先级
func (t *Task) GetPriority() int {
	if t.Priority == 0 {
		return 5 // 默认优先级
	}
	return t.Priority
}

// SetUniqueKey 设置唯一键
func (t *Task) SetUniqueKey(key string) {
	t.UniqueKey = key
}

// GetUniqueKey 获取唯一键
func (t *Task) GetUniqueKey() string {
	return t.UniqueKey
}
