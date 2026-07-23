import asyncpg
import pybreaker
from config.config import Config
from utils.logger import get_logger
from utils import metrics

logger = get_logger()

# Opens after 10 consecutive-ish failures; while OPEN, calls raise
# CircuitBreakerError immediately instead of piling onto a struggling pool.
# reset_timeout=5 lets one trial request through to probe recovery.
db_breaker = pybreaker.CircuitBreaker(fail_max=10, reset_timeout=5, name="postgres")


def _reraise(exc):
    raise exc

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

    async def _do_insert(self, order):
        await self.pool.execute("""
            INSERT INTO orders (order_id, user_id, product_id, quantity, status)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (order_id) DO NOTHING
        """, order.order_id, order.user_id, order.product_id, order.quantity, order.status)

    async def insert_order(self, order):
        # NOTE: pybreaker.call_async is Tornado-only (@gen.coroutine); with no
        # Tornado installed it raises NameError('gen'). We instead drive the
        # breaker's state machine with its synchronous .call() sentinels so
        # open/close still works around this async DB call.
        if db_breaker.current_state == pybreaker.STATE_OPEN:
            # Open -> transient: handler naks (requeue), not a permanent failure.
            metrics.postgres_errors.labels(type="breaker_open").inc()
            raise pybreaker.CircuitBreakerError("postgres breaker open")
        try:
            await self._do_insert(order)
        except Exception as e:
            try:
                db_breaker.call(_reraise, e)  # count the failure toward tripping
            except Exception:
                pass
            metrics.postgres_errors.labels(type="query_error").inc()
            logger.error("db_insert_failed", order_id=order.order_id, error=str(e))
            return False
        try:
            db_breaker.call(lambda: None)     # count success (closes half-open)
        except pybreaker.CircuitBreakerError:
            pass
        return True

    async def close(self):
        if self.pool:
            await self.pool.close()
