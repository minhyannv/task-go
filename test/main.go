package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	taskqueue "task-go"
	"task-go/pkg/task"
)

// 测试配置
var (
	redisAddr = "localhost:6379"
	redisPass = "123456"
	redisDB   = 0
)

// 统计信息
type Stats struct {
	mu           sync.RWMutex
	produced     int
	consumed     int
	errors       int
	startTime    time.Time
	endTime      time.Time
	maxQueueLen  int
	currentQueue int
}

func (s *Stats) AddProduced() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.produced++
}

func (s *Stats) AddConsumed() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.consumed++
}

func (s *Stats) AddError() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.errors++
}

func (s *Stats) UpdateQueue(length int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentQueue = length
	if length > s.maxQueueLen {
		s.maxQueueLen = length
	}
}

func (s *Stats) GetStats() (int, int, int, time.Duration, int, int) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	duration := s.endTime.Sub(s.startTime)
	if s.endTime.IsZero() {
		duration = time.Since(s.startTime)
	}
	return s.produced, s.consumed, s.errors, duration, s.maxQueueLen, s.currentQueue
}

func (s *Stats) Start() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.startTime = time.Now()
}

func (s *Stats) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.endTime = time.Now()
}

var stats = &Stats{}

// 任务类型
type TaskType string

const (
	OrderTask TaskType = "order"
	EmailTask TaskType = "email"
	SMSTask   TaskType = "sms"
)

// 生成测试数据
func generateOrderData(id int) string {
	return fmt.Sprintf(`{
		"order_id": "ORD%06d",
		"customer_id": %d,
		"amount": %.2f,
		"items": ["商品-%d"],
		"timestamp": "%s"
	}`, id, rand.Intn(100000), rand.Float64()*1000, rand.Intn(100), time.Now().Format("15:04:05"))
}

func generateEmailData(id int) string {
	subjects := []string{"订单确认", "支付成功", "发货通知"}
	emails := []string{"user1@test.com", "user2@test.com", "admin@test.com"}
	return fmt.Sprintf(`{
		"to": "%s",
		"subject": "%s",
		"body": "邮件内容 %d",
		"timestamp": "%s"
	}`, emails[rand.Intn(len(emails))], subjects[rand.Intn(len(subjects))], id, time.Now().Format("15:04:05"))
}

func generateSMSData(id int) string {
	phones := []string{"138****8000", "139****9000", "158****8000"}
	messages := []string{"验证码：123456", "订单确认", "发货通知"}
	return fmt.Sprintf(`{
		"phone": "%s",
		"message": "%s",
		"timestamp": "%s"
	}`, phones[rand.Intn(len(phones))], messages[rand.Intn(len(messages))], time.Now().Format("15:04:05"))
}

// 注册任务处理器
func registerHandlers(queue *taskqueue.TaskQueue) {
	// 订单处理器
	queue.RegisterHandler(string(OrderTask), func(ctx context.Context, t *task.Task) error {
		fmt.Printf("📦 [消费者] 处理订单: %s\n", t.Payload)
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond) // 模拟处理时间
		stats.AddConsumed()
		return nil
	})

	// 邮件处理器
	queue.RegisterHandler(string(EmailTask), func(ctx context.Context, t *task.Task) error {
		fmt.Printf("📧 [消费者] 发送邮件: %s\n", t.Payload)
		time.Sleep(time.Duration(rand.Intn(800)) * time.Millisecond)
		stats.AddConsumed()
		return nil
	})

	// 短信处理器
	queue.RegisterHandler(string(SMSTask), func(ctx context.Context, t *task.Task) error {
		fmt.Printf("📱 [消费者] 发送短信: %s\n", t.Payload)
		time.Sleep(time.Duration(rand.Intn(600)) * time.Millisecond)
		stats.AddConsumed()
		return nil
	})
}

// 生产者协程
func producer(ctx context.Context, queue *taskqueue.TaskQueue, taskCount int, wg *sync.WaitGroup) {
	defer wg.Done()

	taskTypes := []TaskType{OrderTask, EmailTask, SMSTask}

	for i := 1; i <= taskCount; i++ {
		select {
		case <-ctx.Done():
			fmt.Printf("🛑 [生产者] 收到停止信号\n")
			return
		default:
		}

		taskType := taskTypes[rand.Intn(len(taskTypes))]
		var data string

		switch taskType {
		case OrderTask:
			data = generateOrderData(i)
		case EmailTask:
			data = generateEmailData(i)
		case SMSTask:
			data = generateSMSData(i)
		}

		taskID, err := queue.SubmitSimple(ctx, string(taskType), data)
		if err != nil {
			fmt.Printf("❌ [生产者] 提交任务失败: %v\n", err)
			stats.AddError()
			continue
		}

		fmt.Printf("✅ [生产者] [%s] 任务 %d 已提交: %s...\n", taskType, i, taskID[:8])
		stats.AddProduced()

		// 随机间隔
		time.Sleep(time.Duration(rand.Intn(2000)) * time.Millisecond)
	}

	fmt.Printf("🛑 [生产者] 生产完成，总共生产了 %d 个任务\n", taskCount)
}

// 监控协程
func monitor(ctx context.Context, queue *taskqueue.TaskQueue, wg *sync.WaitGroup) {
	defer wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Printf("🛑 [监控器] 监控结束\n")
			return
		case <-ticker.C:
			queueStats, err := queue.GetStats(ctx)
			if err == nil {
				if queueLen, ok := queueStats["queue_length"].(int); ok {
					stats.UpdateQueue(queueLen)
				}
			}

			produced, consumed, errors, duration, maxQueue, currentQueue := stats.GetStats()
			successRate := float64(consumed) / float64(produced) * 100
			if produced == 0 {
				successRate = 0
			}

			fmt.Printf("📊 [监控器] 实时状态:\n")
			fmt.Printf("   运行时间: %.1fs\n", duration.Seconds())
			fmt.Printf("   已生产: %d 个\n", produced)
			fmt.Printf("   已消费: %d 个\n", consumed)
			fmt.Printf("   积压任务: %d 个\n", produced-consumed)
			fmt.Printf("   错误数量: %d 个\n", errors)
			fmt.Printf("   队列长度: %d 个\n", currentQueue)
			fmt.Printf("   峰值队列: %d 个\n", maxQueue)
			fmt.Printf("   成功率: %.1f%%\n", successRate)
			fmt.Printf("   生产速率: %.2f tasks/sec\n", float64(produced)/duration.Seconds())
			fmt.Printf("   消费速率: %.2f tasks/sec\n", float64(consumed)/duration.Seconds())
			fmt.Printf("================================\n")
		}
	}
}

// 测试场景1: 基础生产者-消费者测试
func testBasicProducerConsumer() {
	fmt.Printf("🚀 开始基础生产者-消费者测试\n")

	// 创建简单队列
	queue, err := taskqueue.NewSimpleTaskQueue(redisAddr, redisPass, redisDB)
	if err != nil {
		log.Fatalf("创建队列失败: %v", err)
	}

	// 注册处理器
	registerHandlers(queue)

	// 启动队列
	ctx, cancel := context.WithCancel(context.Background())
	go queue.Start(ctx)

	// 重置统计
	stats = &Stats{}
	stats.Start()

	// 启动协程
	var wg sync.WaitGroup

	// 启动生产者
	wg.Add(1)
	go producer(ctx, queue, 20, &wg) // 生产20个任务

	// 启动监控器
	wg.Add(1)
	go monitor(ctx, queue, &wg)

	// 等待生产者完成
	wg.Wait()

	// 等待消费完成
	fmt.Printf("⏳ 等待任务消费完成...\n")
	time.Sleep(10 * time.Second)

	// 停止
	cancel()
	queue.Stop()
	stats.Stop()

	// 最终统计
	produced, consumed, errors, duration, maxQueue, _ := stats.GetStats()
	successRate := float64(consumed) / float64(produced) * 100

	fmt.Printf("\n🎯 基础测试最终报告:\n")
	fmt.Printf("=================================\n")
	fmt.Printf("运行时间: %.1fs\n", duration.Seconds())
	fmt.Printf("生产任务: %d 个\n", produced)
	fmt.Printf("消费任务: %d 个\n", consumed)
	fmt.Printf("积压任务: %d 个\n", produced-consumed)
	fmt.Printf("错误数量: %d 个\n", errors)
	fmt.Printf("峰值队列: %d 个\n", maxQueue)
	fmt.Printf("成功率: %.1f%%\n", successRate)
	fmt.Printf("生产速率: %.2f tasks/sec\n", float64(produced)/duration.Seconds())
	fmt.Printf("消费速率: %.2f tasks/sec\n", float64(consumed)/duration.Seconds())
	fmt.Printf("=================================\n")

	time.Sleep(2 * time.Second)
}

// 测试场景2: 高并发测试
func testHighConcurrency() {
	fmt.Printf("🚀 开始高并发测试\n")

	// 创建简单队列
	queue, err := taskqueue.NewSimpleTaskQueue(redisAddr, redisPass, redisDB)
	if err != nil {
		log.Fatalf("创建队列失败: %v", err)
	}

	// 注册处理器
	registerHandlers(queue)

	// 启动队列
	ctx, cancel := context.WithCancel(context.Background())
	go queue.Start(ctx)

	// 重置统计
	stats = &Stats{}
	stats.Start()

	// 启动协程
	var wg sync.WaitGroup

	// 启动多个生产者
	producerCount := 3
	tasksPerProducer := 15

	for i := 0; i < producerCount; i++ {
		wg.Add(1)
		go producer(ctx, queue, tasksPerProducer, &wg)
	}

	// 启动监控器
	wg.Add(1)
	go monitor(ctx, queue, &wg)

	// 等待所有生产者完成
	wg.Wait()

	// 等待消费完成
	fmt.Printf("⏳ 等待高并发任务消费完成...\n")
	time.Sleep(15 * time.Second)

	// 停止
	cancel()
	queue.Stop()
	stats.Stop()

	// 最终统计
	produced, consumed, errors, duration, maxQueue, _ := stats.GetStats()
	successRate := float64(consumed) / float64(produced) * 100

	fmt.Printf("\n🎯 高并发测试最终报告:\n")
	fmt.Printf("=================================\n")
	fmt.Printf("生产者数量: %d 个\n", producerCount)
	fmt.Printf("运行时间: %.1fs\n", duration.Seconds())
	fmt.Printf("生产任务: %d 个\n", produced)
	fmt.Printf("消费任务: %d 个\n", consumed)
	fmt.Printf("积压任务: %d 个\n", produced-consumed)
	fmt.Printf("错误数量: %d 个\n", errors)
	fmt.Printf("峰值队列: %d 个\n", maxQueue)
	fmt.Printf("成功率: %.1f%%\n", successRate)
	fmt.Printf("生产速率: %.2f tasks/sec\n", float64(produced)/duration.Seconds())
	fmt.Printf("消费速率: %.2f tasks/sec\n", float64(consumed)/duration.Seconds())
	fmt.Printf("=================================\n")

	time.Sleep(2 * time.Second)
}

// 测试场景3: 错误处理测试
func testErrorHandling() {
	fmt.Printf("🚀 开始错误处理测试\n")

	// 创建简单队列
	queue, err := taskqueue.NewSimpleTaskQueue(redisAddr, redisPass, redisDB)
	if err != nil {
		log.Fatalf("创建队列失败: %v", err)
	}

	// 注册会失败的处理器
	queue.RegisterHandler("error_task", func(ctx context.Context, t *task.Task) error {
		fmt.Printf("💥 [消费者] 故意失败的任务: %s\n", t.Payload)
		stats.AddConsumed()
		return fmt.Errorf("故意失败")
	})

	// 启动队列
	ctx, cancel := context.WithCancel(context.Background())
	go queue.Start(ctx)

	// 重置统计
	stats = &Stats{}
	stats.Start()

	// 提交一些会失败的任务
	for i := 1; i <= 5; i++ {
		taskID, err := queue.SubmitSimple(ctx, "error_task", fmt.Sprintf(`{"test_id": %d}`, i))
		if err != nil {
			fmt.Printf("❌ [生产者] 提交任务失败: %v\n", err)
			stats.AddError()
			continue
		}
		fmt.Printf("✅ [生产者] 错误测试任务 %d 已提交: %s...\n", i, taskID[:8])
		stats.AddProduced()
		time.Sleep(1 * time.Second)
	}

	// 等待处理完成
	time.Sleep(10 * time.Second)

	// 停止
	cancel()
	queue.Stop()
	stats.Stop()

	// 最终统计
	produced, consumed, errors, duration, maxQueue, _ := stats.GetStats()

	fmt.Printf("\n🎯 错误处理测试最终报告:\n")
	fmt.Printf("=================================\n")
	fmt.Printf("运行时间: %.1fs\n", duration.Seconds())
	fmt.Printf("生产任务: %d 个\n", produced)
	fmt.Printf("消费任务: %d 个 (包含失败)\n", consumed)
	fmt.Printf("错误数量: %d 个\n", errors)
	fmt.Printf("峰值队列: %d 个\n", maxQueue)
	fmt.Printf("=================================\n")

	time.Sleep(2 * time.Second)
}

func main() {
	fmt.Printf("🎉 TaskGo 统一测试套件\n")
	fmt.Printf("======================\n")

	// 测试场景1: 基础功能
	testBasicProducerConsumer()

	// 测试场景2: 高并发
	testHighConcurrency()

	// 测试场景3: 错误处理
	testErrorHandling()

	fmt.Printf("\n🎉 所有测试完成！\n")
}
