package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

// LoadFromFile 从文件加载配置
func LoadFromFile(filepath string) (*Config, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	config := DefaultConfig()
	if err := json.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("配置验证失败: %w", err)
	}

	return config, nil
}

// LoadFromEnv 从环境变量加载配置
func LoadFromEnv() (*Config, error) {
	// 尝试加载.env文件
	_ = godotenv.Load()

	config := DefaultConfig()

	// Redis配置
	if addr := os.Getenv("TASKGO_REDIS_ADDR"); addr != "" {
		config.Redis.Addr = addr
	}
	if password := os.Getenv("TASKGO_REDIS_PASSWORD"); password != "" {
		config.Redis.Password = password
	}
	if dbStr := os.Getenv("TASKGO_REDIS_DB"); dbStr != "" {
		if db, err := strconv.Atoi(dbStr); err == nil {
			config.Redis.DB = db
		}
	}

	// Worker配置
	if countStr := os.Getenv("TASKGO_WORKER_COUNT"); countStr != "" {
		if count, err := strconv.Atoi(countStr); err == nil {
			config.Worker.Count = count
		}
	}

	// 任务配置
	if retryStr := os.Getenv("TASKGO_DEFAULT_RETRY"); retryStr != "" {
		if retry, err := strconv.Atoi(retryStr); err == nil {
			config.Task.DefaultRetry = retry
		}
	}
	if timeoutStr := os.Getenv("TASKGO_DEFAULT_TIMEOUT"); timeoutStr != "" {
		if timeout, err := time.ParseDuration(timeoutStr); err == nil {
			config.Task.DefaultTimeout = timeout
		}
	}
	if delayStr := os.Getenv("TASKGO_DEFAULT_DELAY"); delayStr != "" {
		if delay, err := time.ParseDuration(delayStr); err == nil {
			config.Task.DefaultDelay = delay
		}
	}
	if priorityStr := os.Getenv("TASKGO_DEFAULT_PRIORITY"); priorityStr != "" {
		if priority, err := strconv.Atoi(priorityStr); err == nil {
			config.Task.DefaultPriority = priority
		}
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("配置验证失败: %w", err)
	}

	return config, nil
}

// SaveToFile 保存配置到文件
func (c *Config) SaveToFile(filepath string) error {
	data, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return fmt.Errorf("序列化配置失败: %w", err)
	}

	if err := os.WriteFile(filepath, data, 0644); err != nil {
		return fmt.Errorf("写入配置文件失败: %w", err)
	}

	return nil
}
