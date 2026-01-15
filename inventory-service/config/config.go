package config

import (
	"os"

	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/pelletier/go-toml/v2"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	Server ServerConfig `toml:"server"`
	Redis  RedisConfig  `toml:"redis"`
	Nats   NatsConfig   `toml:"nats"`
	Logger *slog.Logger `toml:"-"`
}

type ServerConfig struct {
	Port string `toml:"port"`
}

type RedisConfig struct {
	Host   string        `toml:"host"`
	Port   string        `toml:"port"`
	Client *redis.Client `toml:"-"`
}

type NatsConfig struct {
	URL      string     `toml:"url"`
	Username string     `toml:"username"`
	Password string     `toml:"password"`
	Client   *nats.Conn `toml:"-"`
}

func LoadConfig(path string) (*Config, error) {
	// 1. Default Config
	config := &Config{
		Server: ServerConfig{Port: "8080"},
		Redis:  RedisConfig{Host: "localhost", Port: "6379"},
		Nats:   NatsConfig{URL: "nats://localhost:4222"},
	}

	// 2. Load from TOML if exists
	if _, err := os.Stat(path); err == nil {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := toml.Unmarshal(data, config); err != nil {
			return nil, err
		}
	}

	// 3. Override with Environment Variables (Docker Support)
	if port := os.Getenv("PORT"); port != "" {
		config.Server.Port = port
	}
	if host := os.Getenv("REDIS_HOST"); host != "" {
		config.Redis.Host = host
	}
	if port := os.Getenv("REDIS_PORT"); port != "" {
		config.Redis.Port = port
	}
	if url := os.Getenv("NATS_URL"); url != "" {
		config.Nats.URL = url
	}

	return config, nil
}
