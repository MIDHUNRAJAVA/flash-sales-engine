# Infrastructure

## Redis
Used for:
- **Atomic Stock Decrement**: Uses Lua scripts to ensure consistency.
- **Rate Limiting**: (In Gateway Service)

**Configuration:**
- Host: `localhost` (or `redis` in Docker)
- Port: `6379`

## NATS
Used for:
- **Async Order Processing**: Decouples "Buy" request from DB write.
- **Subject**: `orders.pending`

**Configuration:**
- URL: `nats://localhost:4222` (or `nats://nats:4222` in Docker)
