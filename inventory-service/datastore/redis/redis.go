package redis

import (
	"context"
	"fmt"
	"inventory-service/config"
	"time"

	"github.com/redis/go-redis/v9"
)

func Connect(config *config.Config) error {
	rdb := redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", config.Redis.Host, config.Redis.Port),
		PoolSize:     100, // Increased for 5000 VUs
		MinIdleConns: 50,  // Keep 50 connections warm
		PoolTimeout:  30 * time.Second,
	})

	if err := rdb.Ping(context.Background()).Err(); err != nil {
		config.Logger.Error("Redis: Failed to ping", "error", err)
		return err
	}

	config.Redis.Client = rdb
	config.Logger.Info("Redis: Connected successfully")
	return nil
}

const luaScript = `
local stock = redis.call("GET", KEYS[1])
if not stock then
    return 0
end
if tonumber(stock) >= tonumber(ARGV[1]) then
    redis.call("DECRBY", KEYS[1], ARGV[1])
    return 1
else
    return 0
end
`

func DecrementStock(ctx context.Context, client *redis.Client, productID string, quantity int) (bool, error) {
	productKey := fmt.Sprintf("product:%s", productID)
	res, err := client.Eval(ctx, luaScript, []string{productKey}, quantity).Result()
	if err != nil {
		return false, err
	}
	return res.(int64) == 1, nil
}

func SeedStock(ctx context.Context, client *redis.Client, productID string, quantity int) error {
	productKey := fmt.Sprintf("product:%s", productID)
	return client.Set(ctx, productKey, quantity, 0).Err()
}

func Close(client *redis.Client) error {
	return client.Close()
}
