package config

import (
	"os"
	"strconv"

	"log/slog"

	"github.com/nats-io/nats.go"
	"github.com/pelletier/go-toml/v2"
	"github.com/redis/go-redis/v9"
)

type Config struct {
	Server ServerConfig `toml:"server"`
	Redis  RedisConfig  `toml:"redis"`
	Nats   NatsConfig   `toml:"nats"`
	Sale   SaleConfig   `toml:"sale"`
	Logger *slog.Logger `toml:"-"`
}

// SaleConfig freezes sale parameters at startup — deliberately not runtime-mutable.
type SaleConfig struct {
	MaxPerUser int `toml:"max_per_user"` // per-user quantity cap
	IdemTTLSec int `toml:"idem_ttl_s"`   // idempotency key TTL; must exceed the redelivery chain
	ResvTTLSec int `toml:"resv_ttl_s"`   // reservation TTL; must exceed max_deliver x ack_wait
	QuotaTTLSec int `toml:"quota_ttl_s"` // per-user purchase counter TTL
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
	URL      string                `toml:"url"`
	Username string                `toml:"username"`
	Password string                `toml:"password"`
	Client   *nats.Conn            `toml:"-"`
	JS       nats.JetStreamContext `toml:"-"`
}

func LoadConfig(path string) (*Config, error) {
	// 1. Default Config
	config := &Config{
		Server: ServerConfig{Port: "8080"},
		Redis:  RedisConfig{Host: "localhost", Port: "6379"},
		Nats:   NatsConfig{URL: "nats://localhost:4222"},
		Sale: SaleConfig{
			MaxPerUser:  2,
			IdemTTLSec:  900,   // 15 min: > max_deliver(5) x ack_wait(30s) + human retry horizon
			ResvTTLSec:  300,   // 5 min: ~2x the worst-case legitimate confirm latency
			QuotaTTLSec: 90000, // sale duration + 1h
		},
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
	if username := os.Getenv("NATS_USERNAME"); username != "" {
		config.Nats.Username = username
	}
	if password := os.Getenv("NATS_PASSWORD"); password != "" {
		config.Nats.Password = password
	}
	if v := os.Getenv("MAX_PER_USER"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			config.Sale.MaxPerUser = n
		}
	}

	return config, nil
}
