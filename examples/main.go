package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"

	taskgo "github.com/minhyannv/task-go"
	"github.com/minhyannv/task-go/internal/config"
	"github.com/minhyannv/task-go/internal/task"
)

// EmailPayload 邮件任务载荷
type EmailPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

// SMSPayload 短信任务载荷
type SMSPayload struct {
	Phone   string `json:"phone"`
	Message string `json:"message"`
}

// ReportPayload 报告任务载荷
type ReportPayload struct {
	UserID   int    `json:"user_id"`
	ReportID string `json:"report_id"`
	Type     string `json:"type"`
}

func main() {
	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建日志器
	logger, err := zap.NewDevelopment()
	if err != nil {
		log.Fatalf("创建日志器失败: %v", err)
	}
	defer logger.Sync()

	// 加载配置（可以从环境变量或配置文件）
	cfg, err := config.LoadFromEnv()
	if err != nil {
		logger.Fatal("加载配置失败", zap.Error(err))
	}

	// 创建TaskGo客户端
	client, err := taskgo.NewClient(ctx,
		taskgo.WithConfig(cfg),
		taskgo.WithLogger(logger),
	)
	if err != nil {
		logger.Fatal("创建TaskGo客户端失败", zap.Error(err))
	}
	defer client.Close()

	// 注册任务处理器
	registerHandlers(client, logger)

	// 启动客户端
	if err := client.Start(); err != nil {
		logger.Fatal("启动TaskGo客户端失败", zap.Error(err))
	}

	// 提交示例任务
	submitExampleTasks(ctx, client, logger)

	// 设置优雅关闭
	setupGracefulShutdown(ctx, cancel, client, logger)

	// 定期打印统计信息
	printStats(ctx, client, logger)
}

// registerHandlers 注册任务处理器
func registerHandlers(client *taskgo.Client, logger *zap.Logger) {
	// 注册邮件发送处理器
	client.RegisterHandler("send_email", func(ctx context.Context, t *task.Task) (string, error) {
		var payload EmailPayload
		if err := json.Unmarshal([]byte(t.Payload), &payload); err != nil {
			return "", fmt.Errorf("解析邮件载荷失败: %w", err)
		}

		logger.Info("发送邮件",
			zap.String("task_id", t.ID),
			zap.String("to", payload.To),
			zap.String("subject", payload.Subject),
		)

		// 模拟邮件发送
		time.Sleep(100 * time.Millisecond)

		return fmt.Sprintf("邮件已发送到 %s", payload.To), nil
	})

	// 注册短信发送处理器
	client.RegisterHandler("send_sms", func(ctx context.Context, t *task.Task) (string, error) {
		var payload SMSPayload
		if err := json.Unmarshal([]byte(t.Payload), &payload); err != nil {
			return "", fmt.Errorf("解析短信载荷失败: %w", err)
		}

		logger.Info("发送短信",
			zap.String("task_id", t.ID),
			zap.String("phone", payload.Phone),
			zap.String("message", payload.Message),
		)

		// 模拟短信发送
		time.Sleep(200 * time.Millisecond)

		return fmt.Sprintf("短信已发送到 %s", payload.Phone), nil
	})

	// 注册报告生成处理器（高优先级任务）
	client.RegisterHandler("generate_report", func(ctx context.Context, t *task.Task) (string, error) {
		var payload ReportPayload
		if err := json.Unmarshal([]byte(t.Payload), &payload); err != nil {
			return "", fmt.Errorf("解析报告载荷失败: %w", err)
		}

		logger.Info("生成报告",
			zap.String("task_id", t.ID),
			zap.Int("user_id", payload.UserID),
			zap.String("report_id", payload.ReportID),
			zap.String("type", payload.Type),
		)

		// 模拟报告生成
		time.Sleep(500 * time.Millisecond)

		return fmt.Sprintf("报告 %s 已生成", payload.ReportID), nil
	})

	// 注册可能失败的任务处理器（用于测试重试机制）
	client.RegisterHandler("flaky_task", func(ctx context.Context, t *task.Task) (string, error) {
		logger.Info("执行不稳定任务", zap.String("task_id", t.ID))

		// 模拟50%的失败率
		if time.Now().UnixNano()%2 == 0 {
			return "", fmt.Errorf("任务随机失败")
		}

		time.Sleep(100 * time.Millisecond)
		return "不稳定任务执行成功", nil
	})
}

// submitExampleTasks 提交示例任务
func submitExampleTasks(ctx context.Context, client *taskgo.Client, logger *zap.Logger) {
	logger.Info("提交示例任务...")

	// 提交简单任务（邮件发送）
	for i := 0; i < 5; i++ {
		emailPayload := EmailPayload{
			To:      fmt.Sprintf("user%d@example.com", i+1),
			Subject: fmt.Sprintf("欢迎信 #%d", i+1),
			Body:    "欢迎使用我们的服务！",
		}
		payloadBytes, _ := json.Marshal(emailPayload)

		if err := client.SubmitSimpleTask(ctx, "send_email", string(payloadBytes)); err != nil {
			logger.Error("提交邮件任务失败", zap.Error(err))
		}
	}

	// 提交延迟任务（短信发送，5秒后执行）
	for i := 0; i < 3; i++ {
		smsPayload := SMSPayload{
			Phone:   fmt.Sprintf("138000%05d", i+1),
			Message: fmt.Sprintf("您的验证码是：%06d", (i+1)*123456),
		}
		payloadBytes, _ := json.Marshal(smsPayload)

		delay := time.Duration(i+1) * 5 * time.Second
		if err := client.SubmitDelayTask(ctx, "send_sms", string(payloadBytes), delay); err != nil {
			logger.Error("提交短信任务失败", zap.Error(err))
		}
	}

	// 提交优先级任务（报告生成，高优先级）
	for i := 0; i < 3; i++ {
		reportPayload := ReportPayload{
			UserID:   1000 + i,
			ReportID: fmt.Sprintf("RPT-%d", time.Now().Unix()+int64(i)),
			Type:     "monthly",
		}
		payloadBytes, _ := json.Marshal(reportPayload)

		priority := 10 - i // 优先级递减
		if err := client.SubmitPriorityTask(ctx, "generate_report", string(payloadBytes), priority); err != nil {
			logger.Error("提交报告任务失败", zap.Error(err))
		}
	}

	// 提交一些可能失败的任务（测试重试机制）
	for i := 0; i < 3; i++ {
		if err := client.SubmitSimpleTaskWithOptions(ctx, "flaky_task", fmt.Sprintf("payload-%d", i), &taskgo.TaskOptions{
			Retry: intPtr(2), // 重试2次
		}); err != nil {
			logger.Error("提交不稳定任务失败", zap.Error(err))
		}
	}

	logger.Info("所有示例任务已提交")
}

// setupGracefulShutdown 设置优雅关闭
func setupGracefulShutdown(_ context.Context, cancel context.CancelFunc, client *taskgo.Client, logger *zap.Logger) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		logger.Info("收到关闭信号，正在优雅关闭...")
		cancel()
		client.Stop()
		logger.Info("TaskGo客户端已关闭")
		os.Exit(0)
	}()
}

// printStats 定期打印统计信息
func printStats(ctx context.Context, client *taskgo.Client, logger *zap.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stats, err := client.GetStats(ctx)
			if err != nil {
				logger.Error("获取统计信息失败", zap.Error(err))
				continue
			}

			logger.Info("TaskGo统计信息", zap.Any("stats", stats))
		}
	}
}

// intPtr 返回int指针
func intPtr(i int) *int {
	return &i
}
