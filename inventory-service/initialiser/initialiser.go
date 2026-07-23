package initialiser

import (
	"inventory-service/config"
	"inventory-service/datastore/nats"
	"inventory-service/datastore/redis"
	"inventory-service/logging"
	"log/slog"
	"os"
)

func InitDependencies() (*config.Config, func(), error) {
	// 1. Setup Logger: JSON handler wrapped so trace_id/span_id are lifted from
	// context, plus canonical service/version fields on every line.
	serviceName := os.Getenv("OTEL_SERVICE_NAME")
	if serviceName == "" {
		serviceName = "inventory"
	}
	handler := logging.New(slog.NewJSONHandler(os.Stdout, nil)).WithAttrs([]slog.Attr{
		slog.String("service", serviceName),
		slog.String("version", os.Getenv("SERVICE_VERSION")),
	})
	logger := slog.New(handler)
	slog.SetDefault(logger)

	// 2. Load Config
	config, err := config.LoadConfig("config/app.toml")
	if err != nil {
		logger.Warn("Failed to load config file, using defaults/env", "error", err)
	}
	config.Logger = logger

	// 3. Connect to Redis
	if err := redis.Connect(config); err != nil {
		return nil, nil, err
	}
	logger.Info("Connected to Redis")

	// 4. Connect to NATS
	if err := nats.Connect(config); err != nil {
		redis.Close(config.Redis.Client) // Cleanup Redis if NATS fails
		return nil, nil, err
	}
	logger.Info("Connected to NATS")

	cleanup := func() {
		logger.Info("Cleaning up dependencies...")
		if err := redis.Close(config.Redis.Client); err != nil {
			logger.Error("Failed to close Redis", "error", err)
		}
		nats.Close(config.Nats.Client)
		logger.Info("Dependencies cleaned up")
	}

	return config, cleanup, nil
}
