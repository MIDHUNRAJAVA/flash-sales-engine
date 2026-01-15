package actions

import (
	"context"
	"fmt"
	"inventory-service/config"
	"inventory-service/datastore/nats"
	"inventory-service/datastore/redis"
	"inventory-service/models"
	"time"
)

func ProcessBuyRequest(ctx context.Context, config *config.Config, req models.BuyRequest) (*models.OrderEvent, error) {

	config.Logger.Info("Processing", "user_id", req.UserID, "product_id", req.ProductID)

	// 1. Atomic Decrement
	success, err := redis.DecrementStock(ctx, config.Redis.Client, req.ProductID, req.Quantity)
	if err != nil {
		config.Logger.Error("Redis error", "error", err)
		return nil, fmt.Errorf("failed to decrement stock: %w", err)
	}

	if !success {
		config.Logger.Warn("Out of stock", "product_id", req.ProductID)
		return nil, fmt.Errorf("out of stock")
	}
	config.Logger.Info("Stock decremented successfully", "product_id", req.ProductID, "quantity", req.Quantity)

	// 2. Create Order Event
	orderID := fmt.Sprintf("ord-%d-%s", time.Now().UnixNano(), req.UserID)
	event := models.OrderEvent{
		OrderID:   orderID,
		UserID:    req.UserID,
		ProductID: req.ProductID,
		Quantity:  req.Quantity,
		Status:    "PENDING",
		Timestamp: time.Now().Unix(),
	}

	// 3. Publish Event
	if err := nats.PublishOrderEvent(config.Nats.Client, event); err != nil {
		config.Logger.Error("NATS error", "error", err)
		return nil, fmt.Errorf("failed to publish event: %w", err)
	}

	config.Logger.Info("Order event published successfully", "order_id", event.OrderID)
	return &event, nil
}

func SeedStock(ctx context.Context, config *config.Config, productID string, quantity int) error {

	config.Logger.Info("Seeding stock", "product_id", productID, "quantity", quantity)
	if err := redis.SeedStock(ctx, config.Redis.Client, productID, quantity); err != nil {
		config.Logger.Error("Redis error", "error", err)
		return err
	}
	return nil
}
