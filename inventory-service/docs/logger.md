# Logger System

The Inventory Service uses **Structured Logging** via Go's standard library `log/slog`.

## Format
Logs are output in **JSON** format to `stdout`. This allows log aggregators like **Elasticsearch/Kibana** (ELK Stack) to parse and index them easily.

## Usage
```go
logger.Info("Connected to Redis", "host", "localhost")
logger.Error("Failed to process order", "error", err, "order_id", "123")
```

## Output Example
```json
{"time":"2023-10-27T10:00:00Z","level":"INFO","msg":"Connected to Redis","host":"localhost"}
```

## Configuration
The logger is initialized in `main.go`:
```go
logger := sconfig.LoggerNew(sconfig.LoggerNewJSONHandler(os.Stdout, nil))
sconfig.LoggerSetDefault(logger)
```
