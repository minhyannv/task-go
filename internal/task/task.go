package task

import (
	"context"
	"encoding/json"
	"github.com/minhyannv/task-go/internal/models"
	"strconv"
	"time"
)

// TaskOptions 任务选项
type TaskOptions struct {
	Retry    int           // 重试次数
	Timeout  time.Duration // 超时时间
	Delay    time.Duration // 延迟执行时间 (仅延迟队列有效)
	Priority int           // 优先级 (仅优先级队列有效, 1-10, 10最高)
}

// Task 任务结构体
type Task struct {
	ID         string            `json:"id"`          // 任务唯一标识
	Type       string            `json:"type"`        // 任务类型
	Payload    string            `json:"payload"`     // 任务载荷（JSON 字符串）
	Status     models.TaskStatus `json:"status"`      // 任务状态
	CreatedAt  time.Time         `json:"created_at"`  // 创建时间
	UpdatedAt  time.Time         `json:"updated_at"`  // 更新时间
	StartedAt  *time.Time        `json:"started_at"`  // 开始执行时间
	FinishedAt *time.Time        `json:"finished_at"` // 完成时间
	Result     string            `json:"result"`      // 执行结果
	ErrorMsg   string            `json:"error_msg"`   // 错误信息

	Retry    int           `json:"retry_count"` // 重试次数
	Timeout  time.Duration `json:"timeout"`     // 超时时间
	Delay    time.Duration `json:"delay"`       // 延迟执行时间
	Priority int           `json:"priority"`    // 优先级
}

// TaskHandler 任务处理函数类型
type TaskHandler func(ctx context.Context, task *Task) (string, error)

// ToMap 将任务转换为 map，用于存储到 Redis Hash
func (t *Task) ToMap() map[string]interface{} {
	data := map[string]interface{}{
		"id":         t.ID,
		"type":       t.Type,
		"payload":    t.Payload,
		"status":     string(t.Status),
		"created_at": t.CreatedAt.Unix(),
		"updated_at": t.UpdatedAt.Unix(),
		"result":     t.Result,
		"error_msg":  t.ErrorMsg,
		"retry":      t.Retry,
		"timeout":    int64(t.Timeout.Seconds()),
		"delay":      int64(t.Delay.Seconds()),
		"priority":   t.Priority,
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
	t.Type = data["type"]
	t.Payload = data["payload"]
	t.Status = models.TaskStatus(data["status"])
	t.Result = data["result"]
	t.ErrorMsg = data["error_msg"]

	// 解析数值字段（字符串 -> 数字）
	if retryStr, ok := data["retry"]; ok && retryStr != "" {
		if retry, err := strconv.Atoi(retryStr); err == nil {
			t.Retry = retry
		}
	}
	if timeoutStr, ok := data["timeout"]; ok && timeoutStr != "" {
		if timeout, err := strconv.Atoi(timeoutStr); err == nil {
			t.Timeout = time.Duration(timeout) * time.Second
		}
	}
	if delayStr, ok := data["delay"]; ok && delayStr != "" {
		if delay, err := strconv.Atoi(delayStr); err == nil {
			t.Delay = time.Duration(delay) * time.Second
		}
	}
	if priorityStr, ok := data["priority"]; ok && priorityStr != "" {
		if priority, err := strconv.Atoi(priorityStr); err == nil {
			t.Priority = priority
		}
	}

	// 解析时间戳（字符串 -> int64 -> time.Time）
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

//
//// UpdateStatus 更新任务状态
//func (t *Task) UpdateStatus(status TaskStatus) {
//	t.Status = status
//	t.UpdatedAt = time.Now()
//
//	switch status {
//	case StatusRunning:
//		now := time.Now()
//		t.StartedAt = &now
//	case StatusDone, StatusFailed:
//		now := time.Now()
//		t.FinishedAt = &now
//	}
//}
//
//// SetResult 设置任务执行结果
//func (t *Task) SetResult(result string) {
//	t.Result = result
//	t.UpdatedAt = time.Now()
//}
//
//// SetError 设置任务错误信息
//func (t *Task) SetError(errMsg string) {
//	t.ErrorMsg = errMsg
//	t.UpdateStatus(StatusFailed)
//}

// SetRetry 设置重试次数
func (t *Task) SetRetry(count int) {
	t.Retry = count
}

// GetRetry 获取重试次数
func (t *Task) GetRetry() int {
	return t.Retry
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
