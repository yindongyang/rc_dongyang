package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Config 故意保持极简：手写小型 YAML 解析器，避免引第三方库。
// 仅支持 "key: value" 与一层嵌套（用缩进区分）。
type Config struct {
	Server  ServerConfig
	Storage StorageConfig
	Worker  WorkerConfig
	Retry   RetryConfig
	Circuit CircuitConfig
}

type ServerConfig struct {
	Addr            string
	ShutdownTimeout time.Duration
}

type StorageConfig struct {
	Driver string
	DSN    string
}

type WorkerConfig struct {
	Concurrency    int
	PollInterval   time.Duration
	LeaseTimeout   time.Duration
	ReaperInterval time.Duration
}

type RetryConfig struct {
	MaxAttempts int
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	HTTPTimeout time.Duration
}

type CircuitConfig struct {
	FailThreshold int
	Cooldown      time.Duration
}

// Default 返回 MVP 可直接运行的默认配置。
func Default() *Config {
	return &Config{
		Server:  ServerConfig{Addr: ":8080", ShutdownTimeout: 15 * time.Second},
		Storage: StorageConfig{Driver: "sqlite", DSN: "./data/notify.db"},
		Worker: WorkerConfig{
			Concurrency:    8,
			PollInterval:   500 * time.Millisecond,
			LeaseTimeout:   60 * time.Second,
			ReaperInterval: 30 * time.Second,
		},
		Retry: RetryConfig{
			MaxAttempts: 8,
			BaseBackoff: 1 * time.Second,
			MaxBackoff:  1 * time.Hour,
			HTTPTimeout: 10 * time.Second,
		},
		Circuit: CircuitConfig{
			FailThreshold: 10,
			Cooldown:      1 * time.Minute,
		},
	}
}

// Load 读取 YAML（极简实现：仅支持本项目结构，不是通用 YAML 解析器）。
// 取舍：MVP 不引入 yaml.v3 等依赖；若用户没传配置文件，直接返回 Default。
func Load(path string) (*Config, error) {
	cfg := Default()
	if path == "" {
		return cfg, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("read config: %w", err)
	}
	return parse(string(data), cfg)
}

func parse(text string, cfg *Config) (*Config, error) {
	var section string
	for i, raw := range strings.Split(text, "\n") {
		line := stripComment(raw)
		if strings.TrimSpace(line) == "" {
			continue
		}
		// 顶层（无缩进）
		if !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			s := strings.TrimSuffix(strings.TrimSpace(line), ":")
			section = s
			continue
		}
		// 二级
		k, v, ok := splitKV(line)
		if !ok {
			return nil, fmt.Errorf("config line %d invalid: %q", i+1, raw)
		}
		if err := apply(cfg, section, k, v); err != nil {
			return nil, fmt.Errorf("config line %d (%s.%s): %w", i+1, section, k, err)
		}
	}
	return cfg, nil
}

func stripComment(s string) string {
	if i := strings.Index(s, "#"); i >= 0 {
		return s[:i]
	}
	return s
}

func splitKV(s string) (string, string, bool) {
	s = strings.TrimSpace(s)
	idx := strings.Index(s, ":")
	if idx < 0 {
		return "", "", false
	}
	k := strings.TrimSpace(s[:idx])
	v := strings.TrimSpace(s[idx+1:])
	v = strings.Trim(v, `"`)
	return k, v, true
}

func apply(c *Config, section, k, v string) error {
	parseDur := func() (time.Duration, error) { return time.ParseDuration(v) }
	parseInt := func() (int, error) {
		var n int
		_, err := fmt.Sscanf(v, "%d", &n)
		return n, err
	}
	switch section {
	case "server":
		switch k {
		case "addr":
			c.Server.Addr = v
		case "shutdown_timeout":
			d, err := parseDur()
			if err != nil {
				return err
			}
			c.Server.ShutdownTimeout = d
		}
	case "storage":
		switch k {
		case "driver":
			c.Storage.Driver = v
		case "dsn":
			c.Storage.DSN = v
		}
	case "worker":
		switch k {
		case "concurrency":
			n, err := parseInt()
			if err != nil {
				return err
			}
			c.Worker.Concurrency = n
		case "poll_interval":
			d, err := parseDur()
			if err != nil {
				return err
			}
			c.Worker.PollInterval = d
		case "lease_timeout":
			d, err := parseDur()
			if err != nil {
				return err
			}
			c.Worker.LeaseTimeout = d
		case "reaper_interval":
			d, err := parseDur()
			if err != nil {
				return err
			}
			c.Worker.ReaperInterval = d
		}
	case "retry":
		switch k {
		case "max_attempts":
			n, err := parseInt()
			if err != nil {
				return err
			}
			c.Retry.MaxAttempts = n
		case "base_backoff":
			d, err := parseDur()
			if err != nil {
				return err
			}
			c.Retry.BaseBackoff = d
		case "max_backoff":
			d, err := parseDur()
			if err != nil {
				return err
			}
			c.Retry.MaxBackoff = d
		case "http_timeout":
			d, err := parseDur()
			if err != nil {
				return err
			}
			c.Retry.HTTPTimeout = d
		}
	case "circuit":
		switch k {
		case "fail_threshold":
			n, err := parseInt()
			if err != nil {
				return err
			}
			c.Circuit.FailThreshold = n
		case "cooldown":
			d, err := parseDur()
			if err != nil {
				return err
			}
			c.Circuit.Cooldown = d
		}
	}
	return nil
}
