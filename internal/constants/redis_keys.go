package constants

// Redis 键名常量
const (
	// 任务相关
	TaskPrefix = "task:"

	// 队列键名
	SimpleQueueKey   = "task:queue:simple"   // 简单队列
	DelayQueueKey    = "task:queue:delay"    // 延迟队列
	ReadyQueueKey    = "task:queue:ready"    // 就绪队列（延迟任务到期后移入）
	PriorityQueueKey = "task:queue:priority" // 优先级队列
)

// 默认配置常量
const (
	DefaultRetryCount    = 3
	DefaultTimeoutSec    = 30
	DefaultDelaySec      = 1
	DefaultPriority      = 5
	DefaultWorkerCount   = 5
	DefaultRedisAddr     = "localhost:6379"
	DefaultRedisPassword = ""
	DefaultRedisDB       = 0
)

// 任务状态常量（虽然在 models 包中也有定义，但这里作为字符串常量使用）
const (
	StatusPending = "pending"
	StatusRunning = "running"
	StatusDone    = "done"
	StatusFailed  = "failed"
)
