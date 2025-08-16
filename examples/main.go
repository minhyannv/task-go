package main

import (
	"context"
	"fmt"
	queue2 "github.com/minhyannv/task-go/internal/queue"
	"log"
	"time"

	"github.com/minhyannv/task-go/internal/task"
	taskqueue "github.com/minhyannv/task-go/pkg"
)

func main() {
	fmt.Printf("🎉 TaskGo 使用示例\n")
	fmt.Printf("================\n\n")

	// Redis 连接信息
	redisAddr := "localhost:6379"
	redisPass := "8PKtAj9TDIuqAuAAL5w7"
	redisDB := 0

	// 示例1: 简单队列 - FIFO 处理
	fmt.Printf("📋 示例1: 简单队列 (FIFO)\n")
	runSimpleQueueExample(redisAddr, redisPass, redisDB)

	time.Sleep(2 * time.Second)

	// 示例2: 延迟队列 - 定时执行
	fmt.Printf("\n📋 示例2: 延迟队列 (定时执行)\n")
	runDelayedQueueExample(redisAddr, redisPass, redisDB+1)

	time.Sleep(2 * time.Second)

	// 示例3: 优先级队列 - 重要任务优先
	fmt.Printf("\n📋 示例3: 优先级队列 (优先级调度)\n")
	runPriorityQueueExample(redisAddr, redisPass, redisDB+2)

	fmt.Printf("\n🎉 所有示例完成！\n")
}

// 示例1: 简单队列
func runSimpleQueueExample(addr, pass string, db int) {
	// 创建简单队列
	queue, err := taskqueue.NewSimpleTaskQueue(addr, pass, db)
	if err != nil {
		log.Printf("❌ 创建简单队列失败: %v", err)
		return
	}

	// 启动队列
	ctx, cancel := context.WithCancel(context.Background())
	queue.Start(ctx)

	// 注册邮件处理器
	queue.RegisterHandler("email", func(ctx context.Context, t *task.Task) (string, error) {
		fmt.Printf("📧 [简单队列] 发送邮件: %s\n", t.Payload)
		time.Sleep(500 * time.Millisecond) // 模拟处理时间
		return t.Payload, nil
	})

	// 注册订单处理器
	queue.RegisterHandler("order", func(ctx context.Context, t *task.Task) (string, error) {
		fmt.Printf("📦 [简单队列] 处理订单: %s\n", t.Payload)
		time.Sleep(800 * time.Millisecond)
		return t.Payload, nil
	})

	// 提交任务（按 FIFO 顺序执行）
	fmt.Printf("🚀 提交任务到简单队列...\n")

	// 任务1: 邮件
	taskID1, _ := queue.Submit(ctx, "email", `{
		"to": "user@example.com",
		"subject": "欢迎注册",
		"body": "感谢您的注册！"
	}`, nil)
	fmt.Printf("✅ 邮件任务已提交: %s\n", taskID1[:8]+"...")

	// 任务2: 订单
	taskID2, _ := queue.Submit(ctx, "order", `{
		"order_id": "ORD001",
		"amount": 299.99,
		"items": ["商品A", "商品B"]
	}`, nil)
	fmt.Printf("✅ 订单任务已提交: %s\n", taskID2[:8]+"...")

	// 任务3: 邮件
	taskID3, _ := queue.Submit(ctx, "email", `{
		"to": "admin@example.com",
		"subject": "新订单通知",
		"body": "有新订单需要处理"
	}`, nil)
	fmt.Printf("✅ 通知任务已提交: %s\n", taskID3[:8]+"...")

	// 等待任务完成
	time.Sleep(5 * time.Second)

	// 获取统计信息
	stats, _ := queue.GetQueueStats(ctx)
	fmt.Printf("📊 队列统计: %+v\n", stats)

	// 停止队列
	cancel()
	queue.Stop()
	fmt.Printf("✅ 简单队列示例完成\n")
}

// 示例2: 延迟队列
func runDelayedQueueExample(addr, pass string, db int) {
	// 创建延迟队列
	queue, err := taskqueue.NewDelayedTaskQueue(addr, pass, db)
	if err != nil {
		log.Printf("❌ 创建延迟队列失败: %v", err)
		return
	}

	// 启动队列
	ctx, cancel := context.WithCancel(context.Background())
	queue.Start(ctx)

	// 注册提醒处理器
	queue.RegisterHandler("reminder", func(ctx context.Context, t *task.Task) (string, error) {
		fmt.Printf("⏰ [延迟队列] 提醒任务: %s\n", t.Payload)
		return t.Payload, nil
	})

	// 注册超时处理器
	queue.RegisterHandler("timeout", func(ctx context.Context, t *task.Task) (string, error) {
		fmt.Printf("⌛ [延迟队列] 超时处理: %s\n", t.Payload)
		return t.Payload, nil
	})

	// 提交延迟任务
	fmt.Printf("🚀 提交延迟任务...\n")

	// 任务1: 3秒后提醒
	taskID1, _ := queue.Submit(ctx, "reminder", `{
		"type": "meeting",
		"message": "会议将在10分钟后开始",
		"user": "张三"
	}`, &queue2.TaskOptions{
		Delay: 3 * time.Second,
	})
	fmt.Printf("✅ 3秒后执行提醒任务: %s\n", taskID1[:8]+"...")

	// 任务2: 6秒后超时处理
	taskID2, _ := queue.Submit(ctx, "timeout", `{
		"order_id": "ORD002",
		"action": "cancel_unpaid_order",
		"timeout": "30分钟"
	}`, &queue2.TaskOptions{
		Delay: 6 * time.Second,
	})
	fmt.Printf("✅ 6秒后执行超时任务: %s\n", taskID2[:8]+"...")

	// 任务3: 1秒后提醒（最先执行）
	taskID3, _ := queue.Submit(ctx, "reminder", `{
		"type": "notification",
		"message": "您有新消息",
		"user": "李四"
	}`, &queue2.TaskOptions{
		Delay: 1 * time.Second,
	})
	fmt.Printf("✅ 1秒后执行通知任务: %s\n", taskID3[:8]+"...")

	// 等待所有延迟任务完成
	fmt.Printf("⏳ 等待延迟任务执行...\n")
	time.Sleep(8 * time.Second)

	// 停止队列
	cancel()
	queue.Stop()
	fmt.Printf("✅ 延迟队列示例完成\n")
}

// 示例3: 优先级队列
func runPriorityQueueExample(addr, pass string, db int) {
	// 创建优先级队列
	queue, err := taskqueue.NewPriorityTaskQueue(addr, pass, db)
	if err != nil {
		log.Printf("❌ 创建优先级队列失败: %v", err)
		return
	}

	// 启动队列
	ctx, cancel := context.WithCancel(context.Background())
	queue.Start(ctx)

	// 注册紧急任务处理器
	queue.RegisterHandler("urgent", func(ctx context.Context, t *task.Task) (string, error) {
		fmt.Printf("🚨 [优先级队列] 紧急任务: %s\n", t.Payload)
		return t.Payload, nil
	})

	// 注册普通任务处理器
	queue.RegisterHandler("normal", func(ctx context.Context, t *task.Task) (string, error) {
		fmt.Printf("📝 [优先级队列] 普通任务: %s\n", t.Payload)
		return t.Payload, nil
	})

	// 注册低优先级任务处理器
	queue.RegisterHandler("cleanup", func(ctx context.Context, t *task.Task) (string, error) {
		fmt.Printf("🧹 [优先级队列] 清理任务: %s\n", t.Payload)
		return t.Payload, nil
	})

	// 提交不同优先级的任务（注意：先提交低优先级，后提交高优先级）
	fmt.Printf("🚀 提交不同优先级任务...\n")

	// 任务1: 低优先级清理任务
	taskID1, _ := queue.Submit(ctx, "cleanup", `{
		"action": "delete_old_logs",
		"days": 30,
		"size": "1GB"
	}`, &queue2.TaskOptions{
		Priority: 2,
	}) // 低优先级
	fmt.Printf("✅ 低优先级任务已提交: %s (优先级: 2)\n", taskID1[:8]+"...")

	// 任务2: 普通任务
	taskID2, _ := queue.Submit(ctx, "normal", `{
		"action": "send_newsletter",
		"recipients": 1000,
		"template": "monthly"
	}`, &queue2.TaskOptions{
		Priority: 5,
	}) // 中等优先级
	fmt.Printf("✅ 普通优先级任务已提交: %s (优先级: 5)\n", taskID2[:8]+"...")

	// 任务3: 紧急任务（最后提交但最先执行）
	taskID3, _ := queue.Submit(ctx, "urgent", `{
		"action": "security_alert",
		"level": "critical",
		"message": "检测到异常登录"
	}`, &queue2.TaskOptions{
		Priority: 9,
	}) // 高优先级
	fmt.Printf("✅ 紧急任务已提交: %s (优先级: 9)\n", taskID3[:8]+"...")

	// 再添加一个中等优先级任务
	taskID4, _ := queue.Submit(ctx, "normal", `{
		"action": "backup_database",
		"type": "incremental"
	}`, &queue2.TaskOptions{
		Priority: 6,
	}) // 中等偏高优先级
	fmt.Printf("✅ 备份任务已提交: %s (优先级: 6)\n", taskID4[:8]+"...")

	fmt.Printf("📋 执行顺序应该是: 紧急任务(9) → 备份任务(6) → 普通任务(5) → 清理任务(2)\n")

	// 等待任务完成
	time.Sleep(6 * time.Second)

	// 停止队列
	cancel()
	queue.Stop()
	fmt.Printf("✅ 优先级队列示例完成\n")
}
