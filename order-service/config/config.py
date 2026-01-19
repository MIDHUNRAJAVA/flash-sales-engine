from pydantic_settings import BaseSettings

class Config(BaseSettings):
    DB_HOST: str = "postgres"
    DB_PORT: int = 5432
    DB_NAME: str = ""
    DB_USER: str = ""
    DB_PASS: str = ""
    DB_MIN_SIZE: int = 10
    DB_MAX_SIZE: int = 50
    NATS_URL: str = "nats://nats:4222"
    NATS_USER: str = ""
    NATS_PASSWORD: str = ""
    LOG_LEVEL: str = "INFO"

    class Config:
        env_file = ".env"
