package config

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the top-level Watchtower configuration.
type Config struct {
	Server   ServerConfig   `yaml:"server"`
	Pipeline PipelineConfig `yaml:"pipeline"`
	Sinks    []SinkConfig   `yaml:"sinks"`
}

// ServerConfig controls the gRPC listener.
type ServerConfig struct {
	ListenAddr        string        `yaml:"listen_addr"`
	MaxRecvMsgSize    int           `yaml:"max_recv_msg_size"`
	MaxConcurrentStreams uint32     `yaml:"max_concurrent_streams"`
	KeepaliveTime     time.Duration `yaml:"keepalive_time"`
	KeepaliveTimeout  time.Duration `yaml:"keepalive_timeout"`
	EnableCompression bool          `yaml:"enable_compression"`
}

// PipelineConfig controls batching and buffering between ingestion and delivery.
type PipelineConfig struct {
	BatchSize     int           `yaml:"batch_size"`
	FlushInterval time.Duration `yaml:"flush_interval"`
	BufferSize    int           `yaml:"buffer_size"`
	Workers       int           `yaml:"workers"`
}

// SinkConfig describes a downstream delivery target.
type SinkConfig struct {
	Type string `yaml:"type"` // "elasticsearch", "opensearch", "watchtower"

	// Elasticsearch / OpenSearch settings.
	Addresses []string `yaml:"addresses,omitempty"`
	Index     string   `yaml:"index,omitempty"`
	Username  string   `yaml:"username,omitempty"`
	Password  string   `yaml:"password,omitempty"`
	TLS       bool     `yaml:"tls,omitempty"`

	// Upstream Watchtower forwarding settings.
	Target            string        `yaml:"target,omitempty"`
	EnableCompression bool          `yaml:"enable_compression,omitempty"`
	Timeout           time.Duration `yaml:"timeout,omitempty"`

	// Shared settings.
	BatchSize     int           `yaml:"batch_size,omitempty"`
	FlushInterval time.Duration `yaml:"flush_interval,omitempty"`
	RetryAttempts int           `yaml:"retry_attempts,omitempty"`
	RetryBackoff  time.Duration `yaml:"retry_backoff,omitempty"`
}

// Default returns a Config with sensible defaults for a small sidecar instance.
func Default() *Config {
	return &Config{
		Server: ServerConfig{
			ListenAddr:         ":9090",
			MaxRecvMsgSize:     4 * 1024 * 1024, // 4 MB
			MaxConcurrentStreams: 100,
			KeepaliveTime:      30 * time.Second,
			KeepaliveTimeout:   10 * time.Second,
			EnableCompression:  true,
		},
		Pipeline: PipelineConfig{
			BatchSize:     1024,
			FlushInterval: 2 * time.Second,
			BufferSize:    8192,
			Workers:       2,
		},
	}
}

// Load reads a YAML config file and merges it onto the defaults.
func Load(path string) (*Config, error) {
	cfg := Default()

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}

	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}

	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("validate config: %w", err)
	}

	return cfg, nil
}

func (c *Config) validate() error {
	if c.Pipeline.BatchSize <= 0 {
		return fmt.Errorf("pipeline.batch_size must be > 0")
	}
	if c.Pipeline.BufferSize <= 0 {
		return fmt.Errorf("pipeline.buffer_size must be > 0")
	}
	if c.Pipeline.Workers <= 0 {
		return fmt.Errorf("pipeline.workers must be > 0")
	}
	for i, s := range c.Sinks {
		switch s.Type {
		case "elasticsearch", "opensearch":
			if len(s.Addresses) == 0 {
				return fmt.Errorf("sink[%d] (%s): addresses required", i, s.Type)
			}
		case "watchtower":
			if s.Target == "" {
				return fmt.Errorf("sink[%d] (watchtower): target required", i)
			}
		default:
			return fmt.Errorf("sink[%d]: unknown type %q", i, s.Type)
		}
	}
	return nil
}
