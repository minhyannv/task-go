package config

import (
	"time"

	"github.com/minhyannv/task-go/internal/constants"
)

// Config TaskGo配置
type Config struct {
	Redis  RedisConfig  `json:"redis" yaml:"redis"`
	Worker WorkerConfig `json:"worker" yaml:"worker"`
	Task   TaskConfig   `json:"task" yaml:"task"`
}

// RedisConfig Redis配置
type RedisConfig struct {
	Addr     string `json:"addr" yaml:"addr"`
	Password string `json:"password" yaml:"password"`
	DB       int    `json:"db" yaml:"db"`
}

// WorkerConfig Worker配置
type WorkerConfig struct {
	Count int `json:"count" yaml:"count"` // worker数量
}

// TaskConfig 任务配置
type TaskConfig struct {
	DefaultRetry    int           `json:"default_retry" yaml:"default_retry"`
	DefaultTimeout  time.Duration `json:"default_timeout" yaml:"default_timeout"`
	DefaultDelay    time.Duration `json:"default_delay" yaml:"default_delay"`
	DefaultPriority int           `json:"default_priority" yaml:"default_priority"`
}

// DefaultConfig 返回默认配置
func DefaultConfig() *Config {
	return &Config{
		Redis: RedisConfig{
			Addr:     constants.DefaultRedisAddr,
			Password: constants.DefaultRedisPassword,
			DB:       constants.DefaultRedisDB,
		},
		Worker: WorkerConfig{
			Count: constants.DefaultWorkerCount,
		},
		Task: TaskConfig{
			DefaultRetry:    constants.DefaultRetryCount,
			DefaultTimeout:  constants.DefaultTimeoutSec * time.Second,
			DefaultDelay:    constants.DefaultDelaySec * time.Second,
			DefaultPriority: constants.DefaultPriority,
		},
	}
}

// Validate 验证配置
func (c *Config) Validate() error {
	if c.Redis.Addr == "" {
		c.Redis.Addr = constants.DefaultRedisAddr
	}
	if c.Worker.Count <= 0 {
		c.Worker.Count = constants.DefaultWorkerCount
	}
	if c.Task.DefaultRetry < 0 {
		c.Task.DefaultRetry = constants.DefaultRetryCount
	}
	if c.Task.DefaultTimeout <= 0 {
		c.Task.DefaultTimeout = constants.DefaultTimeoutSec * time.Second
	}
	if c.Task.DefaultDelay < 0 {
		c.Task.DefaultDelay = constants.DefaultDelaySec * time.Second
	}
	if c.Task.DefaultPriority <= 0 {
		c.Task.DefaultPriority = constants.DefaultPriority
	}
	return nil
}
