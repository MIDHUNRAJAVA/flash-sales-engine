package models

// BuyRequest represents the expected payload for a buy request
// Ensure JSON tags match the expected payload keys
// Example: {"userID": "user-123", "productID": "test-product", "quantity": 10}
type BuyRequest struct {
	UserID    string `json:"userID" binding:"required"`
	ProductID string `json:"productID" binding:"required"`
	Quantity  int    `json:"quantity" binding:"required"`
}

type SeedRequest struct {
	ProductID string `json:"productID" binding:"required"`
	Quantity  int    `json:"quantity" binding:"required"`
}

type OrderEvent struct {
	OrderID   string `json:"order_id"`
	UserID    string `json:"user_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	Status    string `json:"status"`
	Timestamp int64  `json:"timestamp"`
}
