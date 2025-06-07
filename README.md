# TaskGo - 轻量级异步任务队列 Go SDK

🚀 一个小而美的 Go 异步任务队列框架，基于 Redis 实现，提供简洁的 API 和强大的功能。

## ✨ 特性

- 🎯 **简洁 API** - 一行代码提交任务，自动处理
- ⚡ **高性能** - 基于 Redis，支持高并发处理
- 🔄 **自动重试** - 指数退避重试机制
- ⏰ **延迟任务** - 支持任务延迟执行
- 🔐 **唯一键** - 防止重复任务提交
- 👥 **并发处理** - 多工作器并发执行
- 📊 **状态跟踪** - 完整的任务生命周期管理
- 📈 **统计监控** - 实时队列状态监控

## 🏗️ 架构设计

TaskGo 提供三种完全分离的队列类型，每种队列专注于特定的调度策略：

```
TaskGo 架构
├── 🚀 简单队列 (Simple Queue)     - FIFO 调度，Redis List
├── ⏰ 延迟队列 (Delayed Queue)    - 时间调度，Redis Sorted Set  
└── ⭐ 优先级队列 (Priority Queue) - 优先级调度，Redis Sorted Set
```

### 队列类型对比

| 队列类型 | Redis结构 | 调度方式 | 适用场景 | 时间复杂度 |
|----------|-----------|----------|----------|------------|
| **简单队列** | List | FIFO | 普通后台任务、邮件发送 | O(1) |
| **延迟队列** | Sorted Set | 时间触发 | 定时任务、延迟提醒 | O(log N) |
| **优先级队列** | Sorted Set | 优先级排序 | 紧急任务、VIP请求 | O(log N) |

## 🚀 快速开始

### 安装

```bash
go mod init your-project
go get task-go
```

### 基础使用

#### 1️⃣ 简单队列 - FIFO处理

```go
package main

import (
    "context"
    "log"
    "time"
    
    taskqueue "task-go"
    "task-go/pkg/task"
)

func main() {
    // 创建简单队列
    queue, err := taskqueue.NewSimpleTaskQueue("localhost:6379", "", 0)
    if err != nil {
        log.Fatal(err)
    }
    
    // 注册处理器
    queue.RegisterHandler("email", func(ctx context.Context, t *task.Task) error {
        log.Printf("发送邮件: %s", t.Payload)
        return nil
    })
    
    // 启动队列
    ctx := context.Background()
    go queue.Start(ctx)
    
    // 提交任务（按FIFO顺序执行）
    taskID, _ := queue.SubmitSimple(ctx, "email", `{"to":"user@example.com"}`)
    log.Printf("任务已提交: %s", taskID)
    
    time.Sleep(3 * time.Second)
    queue.Stop()
}
```

#### 2️⃣ 延迟队列 - 定时执行

```go
// 创建延迟队列
queue, _ := taskqueue.NewDelayedTaskQueue("localhost:6379", "", 1)

// 提交延迟任务
queue.SubmitWithDelay(ctx, "reminder", data, 1*time.Hour) // 1小时后执行
queue.SubmitWithDelay(ctx, "cleanup", data, 30*time.Minute) // 30分钟后执行
```

#### 3️⃣ 优先级队列 - 重要任务优先

```go
// 创建优先级队列
queue, _ := taskqueue.NewPriorityTaskQueue("localhost:6379", "", 2)

// 提交不同优先级任务
queue.SubmitWithPriority(ctx, "urgent", data, 9)  // 高优先级
queue.SubmitWithPriority(ctx, "normal", data, 5)  // 普通优先级
queue.SubmitWithPriority(ctx, "cleanup", data, 2) // 低优先级
// 执行顺序: urgent → normal → cleanup
```

### 通用配置方式

```go
config := &taskqueue.Config{
    RedisAddr:  "localhost:6379",
    QueueType:  taskqueue.SimpleQueue, // 或 DelayedQueue, PriorityQueue
    MaxWorkers: 5,
}
queue, _ := taskqueue.NewTaskQueue(config)
```

## 📚 API 参考

### 队列创建

```go
// 专用构造函数（推荐）
simpleQueue, _ := taskqueue.NewSimpleTaskQueue("localhost:6379", "", 0)
delayedQueue, _ := taskqueue.NewDelayedTaskQueue("localhost:6379", "", 1)  
priorityQueue, _ := taskqueue.NewPriorityTaskQueue("localhost:6379", "", 2)

// 通用配置
config := &taskqueue.Config{
    RedisAddr:      "localhost:6379",
    QueueType:      taskqueue.SimpleQueue,
    DefaultRetry:   3,
    DefaultTimeout: 30 * time.Second,
    MaxWorkers:     5,
}
queue, _ := taskqueue.NewTaskQueue(config)
```

### 任务提交

```go
// 简单提交
taskID, err := queue.SubmitSimple(ctx, "send_email", emailData)

// 带选项提交
taskID, err := queue.Submit(ctx, "process_data", data, &queue.JobOptions{
    Retry:     3,                    // 重试次数
    Timeout:   60 * time.Second,     // 超时时间
    Delay:     5 * time.Minute,      // 延迟时间（仅延迟队列）
    Priority:  8,                    // 优先级（仅优先级队列）
    UniqueKey: "daily-report",       // 防重复
})

// 特定队列的便捷方法
delayedQueue.SubmitWithDelay(ctx, "cleanup", data, 1*time.Hour)
priorityQueue.SubmitWithPriority(ctx, "urgent", data, 9)
```

### 处理器注册

```go
// 注册任务处理器
queue.RegisterHandler("email", func(ctx context.Context, t *task.Task) error {
    log.Printf("处理任务: %s", t.ID)
    
    // 任务处理逻辑
    time.Sleep(1 * time.Second)
    
    return nil // 返回 nil 表示成功，返回 error 表示失败
})
```

### 队列管理

```go
// 启动队列
ctx := context.Background()
go queue.Start(ctx)

// 停止队列
queue.Stop()

// 获取队列统计
stats, err := queue.GetStats(ctx)
fmt.Printf("队列类型: %s\n", stats["queue_type"])
fmt.Printf("工作器数量: %d\n", stats["workers"])
```

## 🔧 配置选项

### 队列配置

```go
config := &taskqueue.Config{
    RedisAddr:      "localhost:6379",    // Redis地址
    RedisPassword:  "",                  // Redis密码
    RedisDB:        0,                   // Redis数据库
    QueueType:      taskqueue.SimpleQueue, // 队列类型
    DefaultRetry:   3,                   // 默认重试次数
    DefaultTimeout: 30 * time.Second,    // 默认超时时间
    MaxWorkers:     5,                   // 最大工作器数量
}
```

### 任务选项

```go
jobOpts := &queue.JobOptions{
    Retry:     3,                    // 重试次数
    Timeout:   30 * time.Second,     // 超时时间
    Delay:     5 * time.Minute,      // 延迟时间（仅延迟队列）
    Priority:  8,                    // 优先级（仅优先级队列，1-10）
    UniqueKey: "unique-task-key",    // 唯一键，防重复
}
```

## 📊 任务状态

- `pending` - 待执行
- `running` - 执行中  
- `stopped` - 已停止
- `done` - 已完成
- `failed` - 执行失败

## 🧪 运行示例

```bash
# 确保 Redis 服务运行
redis-server

# 运行统一示例（包含三种队列演示）
go run examples/main.go

# 运行生产者-消费者测试
go run test/producer_consumer/main.go
```

## 📁 项目结构

```
task-go/
├── taskqueue.go                    # 主API入口
├── pkg/task/task.go               # 任务结构定义
├── internal/                       # 内部实现
│   ├── redis/client.go            # Redis客户端封装
│   └── queue/queue.go             # 三种队列实现
├── examples/                       # 使用示例
│   ├── main.go                    # 统一示例（三种队列演示）
│   └── README.md                  # 示例说明文档
├── test/                          # 测试用例
│   ├── producer_consumer/         # 生产者-消费者测试
│   ├── basic/                     # 基础功能测试
│   ├── errors/                    # 错误处理测试
│   ├── unique/                    # 唯一性测试
│   └── performance/               # 性能测试
├── README.md                      # 使用文档
├── LICENSE                        # 许可证
└── go.mod                         # Go模块定义
```

## 🌟 核心特性

- ✅ **三种队列类型完全分离** - 简单队列、延迟队列、优先级队列各司其职
- ✅ **统一接口设计** - 所有队列类型提供一致的API
- ✅ **灵活使用** - 可在同一应用中使用多种队列类型
- ✅ **高性能** - 针对不同场景使用最优的Redis数据结构
- ✅ **易于扩展** - 清晰的架构设计，便于添加新功能
- ✅ **小而美** - 专注核心功能，代码简洁易懂

## 📝 使用场景

### 简单队列适用场景
- 📧 邮件发送
- 🔄 数据同步  
- 📊 日志处理
- 🔔 普通通知

### 延迟队列适用场景
- ⏰ 定时任务
- 📅 延迟提醒
- ⌛ 订单超时处理
- 📊 定时报告

### 优先级队列适用场景
- 🚨 紧急任务处理
- 👑 VIP用户请求
- ⚡ 系统关键任务
- 📈 实时数据处理

## 🔍 技术实现

### Redis 数据结构映射

| 操作 | Redis 命令 | 数据结构 | 说明 |
|------|-----------|----------|------|
| **简单队列入队** | `LPUSH` | List | 从左侧推入任务ID |
| **简单队列出队** | `BRPOP` | List | 阻塞从右侧弹出任务ID |
| **优先级入队** | `ZADD` | Sorted Set | 以优先级为分数添加任务 |
| **优先级出队** | `BZPOPMAX` | Sorted Set | 阻塞弹出最高优先级任务 |
| **延迟任务入队** | `ZADD` | Sorted Set | 以时间戳为分数添加任务 |
| **任务存储** | `HMSET` | Hash | 存储任务元数据 |
| **唯一性检查** | `EXISTS` | String | 检查唯一键是否存在 |

### 延迟队列实现原理

延迟队列采用**两级队列设计** + **定时调度器**架构，精确控制任务的延迟执行时间：

#### 📦 两级队列结构

```
延迟队列架构图

┌─────────────────────┐    定时调度器     ┌─────────────────────┐
│   延迟队列 (ZADD)    │    (每秒扫描)     │   就绪队列 (LIST)    │
│  task:queue:delayed │ ──────────────▶  │  task:queue:ready   │
│                     │    移动到期任务    │                     │
│ Score: 时间戳+优先级  │                  │   FIFO 处理顺序     │
└─────────────────────┘                  └─────────────────────┘
         ▲                                         │
         │ 1. 提交延迟任务                         │ 2. 工作器消费
    ┌─────────┐                              ┌─────────┐
    │  用户   │                              │ Worker  │
    └─────────┘                              └─────────┘
```

#### ⏰ 工作流程说明

1. **任务提交阶段**
   ```go
   // 计算执行时间
   executeAt := time.Now().Add(delay)
   // 使用时间戳作为分数存入 Sorted Set
   score := executeAt.Unix() * 1000  // 毫秒精度
   redis.ZAdd("task:queue:delayed", score, taskID)
   ```

2. **定时调度阶段**
   ```go
   // 每秒扫描一次延迟队列
   ticker := time.NewTicker(time.Second)
   
   // 获取到期任务 (分数 <= 当前时间)
   now := time.Now().Unix() * 1000
   expiredTasks := redis.ZRangeByScore("task:queue:delayed", 0, now)
   
   // 原子操作：移动到期任务
   pipeline := redis.Pipeline()
   for _, taskID := range expiredTasks {
       pipeline.LPush("task:queue:ready", taskID)    // 加入就绪队列
       pipeline.ZRem("task:queue:delayed", taskID)   // 从延迟队列移除
   }
   pipeline.Exec()
   ```

3. **任务执行阶段**
   ```go
   // 工作器从就绪队列取任务
   taskID := redis.BRPop("task:queue:ready", timeout)
   // 执行任务处理逻辑
   executeTask(taskID)
   ```

#### 🎯 关键技术特点

- **时间精度**: 毫秒级时间戳，调度精度 ±1秒
- **原子操作**: 使用 Pipeline 确保任务移动的原子性
- **高性能**: Sorted Set 的 `O(log N)` 复杂度
- **可靠性**: Redis 持久化保证任务不丢失
- **扩展性**: 支持多实例并发调度

### 并发处理机制

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────┐
│   Worker-1  │───▶│  Priority Queue  │◀───│   Worker-2  │
│  BZPOPMAX   │    │  (Sorted Set)    │    │  BZPOPMAX   │
└─────────────┘    └──────────────────┘    └─────────────┘
       │                    ▲                       │
       │                    │                       │
       ▼                    │                       ▼
┌─────────────┐    ┌──────────────────┐    ┌─────────────┐
│  执行任务1   │    │  Delayed Queue   │    │  执行任务2   │
│             │    │  (Sorted Set)    │    │             │
└─────────────┘    └──────────────────┘    └─────────────┘
                          ▲
                          │
                   ┌─────────────┐
                   │  调度器     │
                   │  每秒检查    │
                   └─────────────┘
```

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

🎉 **TaskGo - 让异步任务处理变得简单而强大！** 🚀 