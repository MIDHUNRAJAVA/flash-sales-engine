# Inventory Service API

## Base URL
`http://localhost:8080`

## Endpoints

### 1. Buy Item
**POST** `/buy`

Decrements stock atomically and queues an order event.

**Request Body:**
```json
{
  "user_id": "string",
  "product_id": "string",
  "quantity": "int"
}
```

**Responses:**
- `200 OK`: Order Queued
- `410 Gone`: Out of Stock
- `500 Internal Server Error`: System Error

### 2. Seed Stock
**POST** `/seed`

Seeds initial stock for testing.

**Responses:**
- `200 OK`: Seeded
