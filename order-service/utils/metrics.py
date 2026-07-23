from prometheus_client import Counter, Gauge, Histogram, start_http_server

orders_processed = Counter(
    "orders_processed_total", "Order outcomes",
    ["result"],  # success|failed|dlq|dropped_dead_reservation
)
order_processing_duration = Histogram(
    "order_processing_duration_seconds", "Full pipeline time per message",
)
db_insert_duration = Histogram(
    "db_insert_duration_seconds", "Postgres insert latency",
)
payment_simulation_duration = Histogram(
    "payment_simulation_duration_seconds", "Simulated payment latency",
)
consumer_lag = Gauge(
    "nats_consumer_lag", "Pending messages on the ORDERS work queue",
)
postgres_errors = Counter(
    "postgres_errors_total", "Postgres error count", ["type"],  # query_error|breaker_open
)
redis_errors = Counter(
    "redis_errors_total", "Redis error count", ["type"],  # confirm_cas_error
)


def start_metrics_server(port: int):
    start_http_server(port)
