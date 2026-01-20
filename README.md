# Flash Sales Engine

A high-performance, distributed flash sales system designed to handle massive concurrent traffic during flash sale events. The system uses microservices architecture with event-driven design to ensure consistency, scalability, and resilience.

## 🏗️ Architecture Overview

This system consists of three core microservices communicating through Redis and NATS message broker:

```
┌──────────────┐       ┌─────────────────┐       ┌───────────────┐
│   Gateway    │──────▶│    Inventory    │──────▶│ Order Service │
│  (Node.js)   │       │   Service (Go)  │       │   (Python)    │
└──────────────┘       └─────────────────┘       └───────────────┘
      │                        │                         │
      │                        │                         │
   ┌──▼──┐              ┌──────▼────┐            ┌──────▼──────┐
   │Redis│              │   Redis   │            │  PostgreSQL │
   └─────┘              └───────────┘            └─────────────┘
  (Rate                 (Inventory)                  (Orders)
   Limit)
                        ┌───────────┐
                        │   NATS    │
                        │ (Message  │
                        │  Broker)  │
                        └───────────┘
```

### Services

#### 1. **Gateway Service** (Node.js/Express)
- **Purpose**: API gateway with rate limiting
- **Port**: 3000
- **Responsibilities**:
  - Rate limiting using Redis (5 requests per minute per user)
  - Request validation and routing
  - Forward buy requests to inventory service
- **Technology**: Node.js, Express, Redis client

#### 2. **Inventory Service** (Go/Gin)
- **Purpose**: Core inventory management with atomic stock operations
- **Port**: 8080
- **Responsibilities**:
  - Atomic stock decrement using Redis Lua scripts
  - Prevent overselling through atomic operations
  - Publish order events to NATS
  - Stock seeding for testing
- **Technology**: Go, Gin framework, Redis, NATS
- **Key Features**:
  - Structured JSON logging (slog)
  - Graceful shutdown handling
  - Environment-based configuration

#### 3. **Order Service** (Python/AsyncIO)
- **Purpose**: Asynchronous order processing
- **Responsibilities**:
  - Subscribe to NATS `orders.pending` topic
  - Process and persist orders to PostgreSQL
  - Simulate payment processing
  - Async event handling
- **Technology**: Python 3.11, NATS-py, psycopg2, asyncio

## 🔑 Key Design Patterns

### 1. **Atomic Stock Management**
Uses Redis Lua scripts to ensure atomic stock decrement operations:
```lua
-- Check stock availability and decrement atomically
if tonumber(stock) >= tonumber(quantity) then
    redis.call("DECRBY", product_key, quantity)
    return 1
else
    return 0
end
```

### 2. **Event-Driven Architecture**
- **Decoupling**: Inventory service publishes events; Order service consumes them
- **Async Processing**: Buy requests return immediately; orders process asynchronously
- **Message Broker**: NATS provides reliable message delivery

### 3. **Rate Limiting**
Gateway implements sliding window rate limiting:
- 5 requests per minute per user
- Redis-based counter with expiration
- Prevents system abuse

### 4. **Structured Logging**
Inventory service uses JSON-structured logs for easy parsing:
```json
{"time":"2023-10-27T10:00:00Z","level":"INFO","msg":"Order queued","order_id":"ord-123"}
```

## 📡 API Endpoints

### Gateway Service (Port 3000)

#### POST `/buy`
Purchase an item (with rate limiting)

**Request:**
```json
{
  "user_id": "user-123",
  "product_id": "product-456",
  "quantity": 2
}
```

**Responses:**
- `200 OK`: Order queued successfully
- `410 Gone`: Out of stock
- `429 Too Many Requests`: Rate limit exceeded
- `500 Internal Server Error`: System error

### Inventory Service (Port 8080)

#### POST `/buy`
Process stock decrement and queue order

**Request:**
```json
{
  "userID": "user-123",
  "productID": "product-456",
  "quantity": 2
}
```

**Responses:**
- `200 OK`: { "message": "Order Queued", "order_id": "ord-..." }
- `410 Gone`: { "error": "Out of Stock" }
- `500 Internal Server Error`: System error

#### POST `/seed`
Seed initial stock for testing

**Request:**
```json
{
  "productID": "product-456",
  "quantity": 1000
}
```

**Response:**
- `200 OK`: { "message": "Stock Seeded Successfully" }

## 🛠️ Technology Stack

### Infrastructure
- **Redis**: In-memory data store for inventory and rate limiting
- **NATS**: Message broker with JetStream for event streaming
- **PostgreSQL**: Relational database for order persistence
- **MongoDB**: NoSQL database (configured but not actively used)
- **Elasticsearch + Kibana**: Log aggregation and visualization
- **Docker Compose**: Container orchestration

### Languages & Frameworks
- **Go 1.x**: Inventory service (Gin framework)
- **Node.js 18**: Gateway service (Express)
- **Python 3.11**: Order service (AsyncIO)

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose
- (Optional) Go, Node.js, Python for local development

### Running with Docker Compose

1. **Start all services:**
```bash
docker-compose up --build
```

2. **Seed stock:**
```bash
curl -X POST http://localhost:8080/seed \
  -H "Content-Type: application/json" \
  -d '{"productID": "product-1", "quantity": 1000}'
```

3. **Make a purchase:**
```bash
curl -X POST http://localhost:3000/buy \
  -H "Content-Type: application/json" \
  -d '{"user_id": "user-1", "product_id": "product-1", "quantity": 2}'
```

### Service URLs
- Gateway Service: http://localhost:3000
- Inventory Service: http://localhost:8080
- PostgreSQL: localhost:5432
- Redis: localhost:6379
- NATS: localhost:4222
- NATS Monitoring: http://localhost:8222
- Kibana: http://localhost:5601

## 🧪 Load Testing

The repository includes a K6 load test script (`load_test.js`) for performance testing:

```bash
k6 run load_test.js
```

**Test Profile:**
- Ramp-up: 10s to 10 users
- Sustained: 30s at 50 users
- Ramp-down: 10s to 0 users

**Tests:**
- Stock seeding
- Valid purchase requests
- Invalid purchase requests (out of stock)

## 📁 Project Structure

```
flash-sales-engine/
├── gateway-service/          # Node.js API Gateway
│   ├── index.js             # Express server with rate limiting
│   ├── package.json         # Node dependencies
│   └── Dockerfile           # Container definition
│
├── inventory-service/        # Go Inventory Management
│   ├── main.go              # Entry point
│   ├── actions/             # Business logic
│   ├── config/              # Configuration management
│   ├── datastore/           # Redis & NATS clients
│   │   ├── redis/           # Atomic stock operations
│   │   └── nats/            # Event publishing
│   ├── handler/             # HTTP handlers
│   ├── models/              # Data models
│   ├── initialiser/         # Dependency initialization
│   ├── docs/                # API & infrastructure docs
│   └── Dockerfile           # Multi-stage build
│
├── order-service/            # Python Order Processing
│   ├── main.py              # NATS subscriber & DB writer
│   ├── requirements.txt     # Python dependencies
│   └── Dockerfile           # Container definition
│
├── docker-compose.yml        # Complete system orchestration
└── load_test.js             # K6 performance tests
```

## 🔧 Configuration

### Environment Variables

**Gateway Service:**
- `REDIS_HOST`: Redis hostname (default: localhost)
- `REDIS_PORT`: Redis port (default: 6379)
- `INVENTORY_SERVICE_URL`: Inventory service endpoint

**Inventory Service:**
- `PORT`: Server port (default: 8080)
- `REDIS_HOST`: Redis hostname (default: localhost)
- `REDIS_PORT`: Redis port (default: 6379)
- `NATS_URL`: NATS connection URL (default: nats://localhost:4222)

**Order Service:**
- `NATS_URL`: NATS connection URL
- `DB_HOST`: PostgreSQL hostname
- `DB_NAME`: Database name
- `DB_USER`: Database user
- `DB_PASS`: Database password

### Inventory Service Configuration File

Optional `config/app.toml`:
```toml
[server]
port = "8080"

[redis]
host = "localhost"
port = "6379"

[nats]
url = "nats://localhost:4222"
```

## 🎯 Use Cases

### Flash Sale Scenario
1. **Preparation**: Seed stock for flash sale item
2. **Sale Start**: Thousands of concurrent users attempt to buy
3. **Atomic Operations**: Redis ensures no overselling
4. **Rate Limiting**: Gateway prevents abuse
5. **Async Processing**: Orders queue via NATS for background processing
6. **Persistence**: Order service saves confirmed orders to PostgreSQL

### Key Challenges Solved
- ✅ **Race Conditions**: Atomic Redis Lua scripts
- ✅ **Overselling**: Stock checked and decremented in single operation
- ✅ **High Concurrency**: Redis connection pooling (100 connections)
- ✅ **System Overload**: Rate limiting (5 req/min/user)
- ✅ **Coupling**: Event-driven architecture via NATS
- ✅ **Observability**: Structured logging to ELK stack

## 🔍 Monitoring & Observability

### Logs
- **Format**: JSON structured logs
- **Aggregation**: Elasticsearch
- **Visualization**: Kibana (http://localhost:5601)

### NATS Monitoring
- **Dashboard**: http://localhost:8222
- **Metrics**: Messages published, subscriptions, connections

### Example Log Entry
```json
{
  "time": "2023-10-27T10:15:30Z",
  "level": "INFO",
  "msg": "Order queued successfully",
  "order_id": "ord-1698403530000-user-123",
  "product_id": "product-1",
  "quantity": 2
}
```

## 🔒 Reliability Features

1. **Graceful Shutdown**: Services handle SIGTERM/SIGINT properly
2. **Connection Pooling**: Optimized Redis connections (100 pool size, 50 min idle)
3. **Health Checks**: Docker Compose dependencies ensure startup order
4. **Error Handling**: Comprehensive error logging and HTTP status codes
5. **Transaction Safety**: PostgreSQL ensures order data consistency

## 📚 Documentation

Additional documentation is available in the `inventory-service/docs/` directory:
- `api.md`: Detailed API specifications
- `infrastructure.md`: Redis & NATS configuration details
- `logger.md`: Logging system documentation

## 🤝 Contributing

This is a demonstration project showcasing microservices architecture for flash sales systems. Key learning points:
- Atomic operations for inventory management
- Event-driven microservices communication
- Rate limiting and traffic control
- Multi-language microservices integration
- Container orchestration with Docker Compose

## 📝 License

This project is open source and available for educational purposes.

## 🎓 Learning Resources

This project demonstrates:
- **Microservices Architecture**: Service decomposition and communication
- **Event-Driven Design**: Async messaging with NATS
- **Distributed Systems**: Handling concurrency and consistency
- **Database Patterns**: Atomic operations, event sourcing concepts
- **DevOps**: Docker, container orchestration, monitoring
- **Multi-language Development**: Go, Node.js, Python integration

---

**Built for handling high-concurrency flash sales with reliability and scalability.**
