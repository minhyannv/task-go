package models

// QueueType 队列类型
type QueueType string

const (
	SimpleQueue   QueueType = "simple"   // 简单队列 (FIFO)
	DelayedQueue  QueueType = "delayed"  // 延迟队列 (时间调度)
	PriorityQueue QueueType = "priority" // 优先级队列 (优先级调度)
)
