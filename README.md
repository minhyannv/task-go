# TaskGo - 轻量级异步任务队列 Go SDK

🚀 基于 Redis 的 Go 异步任务队列框架，提供简洁的 API 和强大的功能。

## ✨ 特性

- 🎯 **简洁 API** - 一行代码提交任务
- ⚡ **高性能** - 基于 Redis，支持高并发
- 🔄 **自动重试** - 指数退避重试机制
- ⏰ **延迟任务** - 支持任务延迟执行
- 👥 **并发处理** - 多工作器并发执行
- 📊 **状态跟踪** - 完整的任务生命周期管理

## 🏗️ 队列类型

TaskGo 提供三种队列类型，每种专注于特定的调度策略：

| 队列类型 | 调度方式 | 适用场景 |
|----------|----------|----------|
| **简单队列** | FIFO | 普通后台任务、邮件发送 |
| **延迟队列** | 时间触发 | 定时任务、延迟提醒 |
| **优先级队列** | 优先级排序 | 紧急任务、VIP请求 |

## 🚀 快速开始

### 安装

```bash
go get github.com/minhyannv/task-go
```

### 基础使用

```go
package main

import (
    "context"
    "log"
    
    taskgo "github.com/minhyannv/task-go"
    "github.com/minhyannv/task-go/internal/task"
)

func main() {
    ctx := context.Background()
    
    // 创建客户端
    client, err := taskgo.NewClient(ctx)
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()
    
    // 注册处理器
    client.RegisterHandler("send_email", func(ctx context.Context, t *task.Task) (string, error) {
        // 处理邮件发送逻辑
        return "邮件已发送", nil
    })
    
    // 启动客户端
    if err := client.Start(); err != nil {
        log.Fatal(err)
    }
    
    // 提交任务
    client.SubmitSimpleTask(ctx, "send_email", `{"to":"user@example.com"}`)
    
    // 阻塞等待
    select {}
}
```

更多示例请参考 `examples/main.go`

## 📊 任务状态

- `pending` - 待执行
- `running` - 执行中  
- `done` - 已完成
- `failed` - 执行失败

## 📝 使用场景

- **简单队列**: 邮件发送、数据同步、日志处理
- **延迟队列**: 定时任务、延迟提醒、订单超时处理
- **优先级队列**: 紧急任务、VIP用户请求、系统关键任务

## 📄 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

🎉 **TaskGo - 让异步任务处理变得简单而强大！** 🚀 