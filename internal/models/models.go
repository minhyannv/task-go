package models

// QueueType 队列类型
type QueueType string

const (
	SimpleQueue   QueueType = "simple"   // 简单队列 (FIFO)
	DelayQueue    QueueType = "delay"    // 延迟队列 (时间调度)
	PriorityQueue QueueType = "priority" // 优先级队列 (优先级调度)
)

// TaskStatus 任务状态枚举
type TaskStatus string

const (
	StatusPending TaskStatus = "pending"
	StatusRunning TaskStatus = "running"
	StatusDone    TaskStatus = "done"
	StatusFailed  TaskStatus = "failed"
)
