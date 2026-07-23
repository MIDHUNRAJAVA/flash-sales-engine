package handler

import (
	"inventory-service/actions"
	"inventory-service/config"
	"inventory-service/datastore/redis"
	"inventory-service/logging"
	"inventory-service/models"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

func HandleHealth(config *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {
		redisOk := config.Redis.Client != nil && config.Redis.Client.Ping(c.Request.Context()).Err() == nil
		natsOk := config.Nats.Client != nil && config.Nats.Client.IsConnected()

		status := "ok"
		httpStatus := http.StatusOK
		if !redisOk || !natsOk {
			status = "degraded"
			httpStatus = http.StatusServiceUnavailable
		}

		c.JSON(httpStatus, gin.H{
			"status": status,
			"redis":  redisOk,
			"nats":   natsOk,
		})
	}
}

func HandleBuy(config *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {

		ctx := c.Request.Context()

		var req models.BuyRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			config.Logger.ErrorContext(ctx, "Invalid request body", "error", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// '|' is the field separator inside the reservation value.
		if strings.ContainsAny(req.UserID+req.ProductID, "|{} ") {
			c.JSON(http.StatusBadRequest, gin.H{"error": "userID/productID contain invalid characters"})
			return
		}

		requestID := c.GetHeader("X-Request-ID")
		config.Logger.InfoContext(ctx, "Received request", "user_id", logging.HashUserID(req.UserID),
			"product_id", req.ProductID, "quantity", req.Quantity, "request_id", requestID)

		outcome, err := actions.ProcessBuyRequest(ctx, config, req, requestID)
		if err != nil {
			// Reservation failed or was rolled back — the order was NOT accepted,
			// and it is safe to retry. 503 (not 500): defined, retryable.
			config.Logger.ErrorContext(ctx, "Buy not accepted", "error", err, "request_id", requestID)
			c.Header("Retry-After", "2")
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "order not accepted, please retry"})
			return
		}

		switch outcome.Code {
		case redis.CodeSuccess:
			c.JSON(http.StatusOK, gin.H{
				"message":         "Order Queued",
				"order_id":        outcome.OrderID,
				"remaining_stock": outcome.Remaining,
			})
		case redis.CodeDuplicate:
			// Idempotent replay: same result as the original attempt, not an error.
			c.JSON(http.StatusOK, gin.H{
				"message":   "Order Queued",
				"order_id":  outcome.OrderID,
				"duplicate": true,
			})
		case redis.CodeOutOfStock:
			c.JSON(http.StatusGone, gin.H{"error": "Out of Stock"})
		case redis.CodeQuota:
			c.JSON(http.StatusConflict, gin.H{"error": "quota_exceeded", "max_per_user": config.Sale.MaxPerUser})
		case redis.CodeInvalidQty:
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid quantity"})
		case redis.CodeSaleClosed:
			c.JSON(http.StatusForbidden, gin.H{"error": "sale_not_open"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "internal error"})
		}
	}
}

func HandleSeed(config *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {

		var req models.SeedRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			config.Logger.Error("Invalid request body", "error", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		config.Logger.Info("Received request", "product_id", req.ProductID, "quantity", req.Quantity)

		if err := actions.SeedStock(c.Request.Context(), config, req.ProductID, req.Quantity); err != nil {
			config.Logger.Error("Failed to seed stock", "error", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		config.Logger.Info("Stock seeded, sale gate OPEN")
		c.JSON(http.StatusOK, gin.H{"message": "Stock Seeded Successfully", "sale_state": "OPEN"})
	}
}
