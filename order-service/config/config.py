from pydantic_settings import BaseSettings

class Config(BaseSettings):
    DB_HOST: str = "postgres"
    DB_PORT: int = 5432
    DB_NAME: str = ""
    DB_USER: str = ""
    DB_PASS: str = ""
    DB_MIN_SIZE: int = 10
    DB_MAX_SIZE: int = 20
    NATS_URL: str = "nats://nats:4222"
    NATS_USER: str = ""
    NATS_PASSWORD: str = ""
    REDIS_HOST: str = "redis-stock"   # stock instance: reservation state lives there
    REDIS_PORT: int = 6379
    LOG_LEVEL: str = "INFO"
    METRICS_PORT: int = 9100

    # Consumer contract (must stay consistent with the reservation TTL):
    # worst-case legit confirm = MAX_DELIVER x ACK_WAIT ~ 150s < resv TTL 300s.
    ACK_WAIT_SECONDS: int = 30      # ~20x p99 processing; too short = duplicate storms
    MAX_DELIVER: int = 5
    MAX_ACK_PENDING: int = 16       # < DB pool max, or "concurrency" is just queueing

    class Config:
        env_file = ".env"
