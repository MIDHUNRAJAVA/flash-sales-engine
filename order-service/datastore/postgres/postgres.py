import asyncpg
from config.config import Config
from utils.logger import get_logger

logger = get_logger()

class PostgresClient:
    def __init__(self, config: Config):
        self.config = config
        self.pool = None

    async def connect(self):
        try:
            self.pool = await asyncpg.create_pool(
                host=self.config.DB_HOST,
                port=self.config.DB_PORT,
                database=self.config.DB_NAME,
                user=self.config.DB_USER,
                password=self.config.DB_PASS,
                min_size=self.config.DB_MIN_SIZE,
                max_size=self.config.DB_MAX_SIZE
            )
            await self._ensure_table_exists()
            logger.info("Connected to Postgres (Async)")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise e

    async def _ensure_table_exists(self):
        async with self.pool.acquire() as connection:
            await connection.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id VARCHAR(255) PRIMARY KEY,
                    user_id VARCHAR(255),
                    product_id VARCHAR(255),
                    quantity INT,
                    status VARCHAR(50),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

    async def insert_order(self, order):
        try:
            await self.pool.execute("""
                INSERT INTO orders (order_id, user_id, product_id, quantity, status)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (order_id) DO NOTHING
            """, order.order_id, order.user_id, order.product_id, order.quantity, order.status)
            return True
        except Exception as e:
            logger.error(f"Failed to insert order {order.order_id}: {e}")
            return False

    async def close(self):
        if self.pool:
            await self.pool.close()
