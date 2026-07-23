package models

// BuyRequest represents the expected payload for a buy request.
// ClientNonce identifies one logical purchase attempt: the gateway mints it
// (and reuses it across retries of the same attempt), so the derived
// idempotency key is stable exactly when it needs to be.
// Example: {"userID": "user-123", "productID": "test-product", "quantity": 1, "clientNonce": "..."}
type BuyRequest struct {
	UserID      string `json:"userID" binding:"required"`
	ProductID   string `json:"productID" binding:"required"`
	Quantity    int    `json:"quantity" binding:"required"`
	ClientNonce string `json:"clientNonce"`
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
	RequestID string `json:"request_id,omitempty"`
}
