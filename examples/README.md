# TaskGo 使用示例

🚀 这是 TaskGo 的统一使用示例，展示三种队列类型的基本用法。

## 🎯 快速运行

确保 Redis 服务正在运行：

```bash
# 启动 Redis (Docker 方式)
docker run -d --name redis -p 6379:6379 redis redis-server --requirepass 123456

# 运行示例
cd examples
go run main.go
```

## 📋 示例内容

### 🚀 示例1: 简单队列 (FIFO)

**演示功能**：
- ✅ 创建简单队列
- ✅ 注册任务处理器
- ✅ 按FIFO顺序提交和处理任务
- ✅ 获取队列统计信息

**任务类型**：
- 📧 邮件发送任务
- 📦 订单处理任务

**执行顺序**: 先进先出，按提交顺序处理

### ⏰ 示例2: 延迟队列 (定时执行)

**演示功能**：
- ✅ 创建延迟队列
- ✅ 提交不同延迟时间的任务
- ✅ 按时间顺序自动执行

**任务类型**：
- ⏰ 提醒任务
- ⌛ 超时处理任务

**执行顺序**: 按时间先后，不按提交顺序

### ⭐ 示例3: 优先级队列 (优先级调度)

**演示功能**：
- ✅ 创建优先级队列
- ✅ 提交不同优先级的任务
- ✅ 按优先级高低执行

**任务类型**：
- 🚨 紧急任务 (优先级: 9)
- 📝 普通任务 (优先级: 5-6)
- 🧹 清理任务 (优先级: 2)

**执行顺序**: 优先级高的先执行，不按提交顺序

## 📊 预期输出

运行示例会看到类似以下输出：

```
🎉 TaskGo 使用示例
================

📋 示例1: 简单队列 (FIFO)
🚀 提交任务到简单队列...
✅ 邮件任务已提交: a1b2c3d4...
✅ 订单任务已提交: e5f6g7h8...
✅ 通知任务已提交: i9j0k1l2...
📧 [简单队列] 发送邮件: {"to":"user@example.com",...}
📦 [简单队列] 处理订单: {"order_id":"ORD001",...}
📧 [简单队列] 发送邮件: {"to":"admin@example.com",...}
📊 队列统计: map[queue_type:simple workers:5 ...]
✅ 简单队列示例完成

📋 示例2: 延迟队列 (定时执行)
🚀 提交延迟任务...
✅ 3秒后执行提醒任务: m3n4o5p6...
✅ 6秒后执行超时任务: q7r8s9t0...
✅ 1秒后执行通知任务: u1v2w3x4...
⏳ 等待延迟任务执行...
⏰ [延迟队列] 提醒任务: {"type":"notification",...}    # 1秒后
⏰ [延迟队列] 提醒任务: {"type":"meeting",...}        # 3秒后
⌛ [延迟队列] 超时处理: {"order_id":"ORD002",...}     # 6秒后
✅ 延迟队列示例完成

📋 示例3: 优先级队列 (优先级调度)
🚀 提交不同优先级任务...
✅ 低优先级任务已提交: y5z6a7b8... (优先级: 2)
✅ 普通优先级任务已提交: c9d0e1f2... (优先级: 5)
✅ 紧急任务已提交: g3h4i5j6... (优先级: 9)
✅ 备份任务已提交: k7l8m9n0... (优先级: 6)
📋 执行顺序应该是: 紧急任务(9) → 备份任务(6) → 普通任务(5) → 清理任务(2)
🚨 [优先级队列] 紧急任务: {"action":"security_alert",...}  # 优先级9
📝 [优先级队列] 普通任务: {"action":"backup_database",...} # 优先级6
📝 [优先级队列] 普通任务: {"action":"send_newsletter",...} # 优先级5
🧹 [优先级队列] 清理任务: {"action":"delete_old_logs",...} # 优先级2
✅ 优先级队列示例完成

🎉 所有示例完成！
```

## 🔍 代码要点

### 队列创建
```go
// 简单队列
queue, _ := taskqueue.NewSimpleTaskQueue("localhost:6379", "123456", 0)

// 延迟队列  
queue, _ := taskqueue.NewDelayedTaskQueue("localhost:6379", "123456", 1)

// 优先级队列
queue, _ := taskqueue.NewPriorityTaskQueue("localhost:6379", "123456", 2)
```

### 任务处理器注册
```go
queue.RegisterHandler("task_type", func(ctx context.Context, t *task.Task) error {
    fmt.Printf("处理任务: %s\n", t.Payload)
    return nil // 返回nil表示成功，返回error表示失败
})
```

### 任务提交
```go
// 简单队列
taskID, _ := queue.SubmitSimple(ctx, "task_type", `{"data": "value"}`)

// 延迟队列
taskID, _ := queue.SubmitWithDelay(ctx, "task_type", `{"data": "value"}`, 5*time.Second)

// 优先级队列
taskID, _ := queue.SubmitWithPriority(ctx, "task_type", `{"data": "value"}`, 8)
```

## ⚙️ 配置说明

- **Redis连接**: `localhost:6379`，密码 `123456`
- **数据库选择**: 简单队列(DB0)，延迟队列(DB1)，优先级队列(DB2)
- **优先级范围**: 1-10，数字越大优先级越高
- **延迟时间**: 支持任意 `time.Duration`

## 🛠️ 自定义示例

可以基于现有示例修改：

```go
// 修改Redis连接
redisAddr := "your-redis-host:6379"
redisPass := "your-password"

// 添加自定义任务类型
queue.RegisterHandler("my_task", func(ctx context.Context, t *task.Task) error {
    // 你的业务逻辑
    return nil
})

// 提交自定义任务
taskID, _ := queue.SubmitSimple(ctx, "my_task", `{"custom": "data"}`)
```

---

🎉 **通过这个统一示例，快速掌握 TaskGo 的核心功能！** 🚀 