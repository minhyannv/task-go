package handler_manager

import (
	"fmt"
	"sync"

	"github.com/minhyannv/task-go/internal/task"
	"go.uber.org/zap"
)

// HandlerManager 任务处理器管理器
type HandlerManager struct {
	logger   *zap.Logger
	handlers map[string]task.TaskHandler
	mu       sync.RWMutex
}

// NewHandlerManager 创建新的处理器管理器
func NewHandlerManager(logger *zap.Logger) *HandlerManager {
	return &HandlerManager{
		logger:   logger,
		handlers: make(map[string]task.TaskHandler),
	}
}

// RegisterHandler 注册任务处理器
func (hm *HandlerManager) RegisterHandler(taskType string, handler task.TaskHandler) error {
	if taskType == "" {
		return fmt.Errorf("任务类型不能为空")
	}
	if handler == nil {
		return fmt.Errorf("处理器不能为空")
	}

	hm.mu.Lock()
	defer hm.mu.Unlock()

	// 检查是否已存在
	if _, exists := hm.handlers[taskType]; exists {
		hm.logger.Sugar().Warnf("任务类型 %s 的处理器已存在，将被覆盖", taskType)
	}

	hm.handlers[taskType] = handler
	hm.logger.Sugar().Infof("已注册任务处理器: %s", taskType)
	return nil
}

// UnregisterHandler 注销任务处理器
func (hm *HandlerManager) UnregisterHandler(taskType string) error {
	if taskType == "" {
		return fmt.Errorf("任务类型不能为空")
	}

	hm.mu.Lock()
	defer hm.mu.Unlock()

	if _, exists := hm.handlers[taskType]; !exists {
		return fmt.Errorf("任务类型 %s 的处理器不存在", taskType)
	}

	delete(hm.handlers, taskType)
	hm.logger.Sugar().Infof("已注销任务处理器: %s", taskType)
	return nil
}

// GetHandler 获取任务处理器
func (hm *HandlerManager) GetHandler(taskType string) (task.TaskHandler, bool) {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	handler, exists := hm.handlers[taskType]
	return handler, exists
}

// HasHandler 检查是否存在指定类型的处理器
func (hm *HandlerManager) HasHandler(taskType string) bool {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	_, exists := hm.handlers[taskType]
	return exists
}

// ListHandlers 列出所有已注册的处理器类型
func (hm *HandlerManager) ListHandlers() []string {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	types := make([]string, 0, len(hm.handlers))
	for taskType := range hm.handlers {
		types = append(types, taskType)
	}
	return types
}

// GetHandlerCount 获取处理器数量
func (hm *HandlerManager) GetHandlerCount() int {
	hm.mu.RLock()
	defer hm.mu.RUnlock()

	return len(hm.handlers)
}

// ClearHandlers 清空所有处理器
func (hm *HandlerManager) ClearHandlers() {
	hm.mu.Lock()
	defer hm.mu.Unlock()

	hm.handlers = make(map[string]task.TaskHandler)
	hm.logger.Sugar().Info("已清空所有任务处理器")
}
