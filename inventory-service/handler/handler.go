package handler

import (
	"inventory-service/actions"
	"inventory-service/config"
	"inventory-service/models"
	"net/http"

	"github.com/gin-gonic/gin"
)

func HandleBuy(config *config.Config) gin.HandlerFunc {
	return func(c *gin.Context) {

		var req models.BuyRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			config.Logger.Error("Invalid request body", "error", err)
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		config.Logger.Info("Received request", "user_id", req.UserID, "product_id", req.ProductID, "quantity", req.Quantity)

		event, err := actions.ProcessBuyRequest(c.Request.Context(), config, req)
		if err != nil {
			if err.Error() == "out of stock" {
				config.Logger.Warn("Out of stock", "product_id", req.ProductID)
				c.JSON(http.StatusGone, gin.H{"error": "Out of Stock"})
			} else {
				config.Logger.Error("Internal server error", "error", err)
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			}
			return
		}

		config.Logger.Info("Order queued successfully", "order_id", event.OrderID)
		c.JSON(http.StatusOK, gin.H{
			"message":  "Order Queued",
			"order_id": event.OrderID,
		})
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
		config.Logger.Info("Stock seeded successfully")
		c.JSON(http.StatusOK, gin.H{"message": "Stock Seeded Successfully"})
	}
}
