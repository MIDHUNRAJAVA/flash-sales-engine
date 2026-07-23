import redis.asyncio as aioredis
from config.config import Config
from utils.logger import get_logger
from utils import metrics

logger = get_logger()

# CAS RESERVED -> CONFIRMED. Exactly one of confirm / cancel / expire wins:
#   1  = confirmed now (this delivery claimed the reservation)
#  -1  = already CONFIRMED (redelivery after a crash — safe to re-run the
#        idempotent Postgres insert)
#   0  = reservation is CANCELLED/EXPIRED/gone — stock already went back,
#        do NOT fulfill this order.
CONFIRM_LUA = """
local v = redis.call('GET', KEYS[1])
if not v then return 0 end
local state = string.match(v, '^(%u+)')
if state == 'CONFIRMED' then return -1 end
if state ~= 'RESERVED' then return 0 end
-- match, not find: find returns (start, end) and the second value would
-- silently become sub's end argument, truncating the tail
local rest = string.match(v, '(|.*)$')
redis.call('SET', KEYS[1], 'CONFIRMED' .. rest, 'EX', 86400)
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
"""


class RedisClient:
    def __init__(self, config: Config):
        self.config = config
        self.client = None
        self._confirm = None

    async def connect(self):
        self.client = aioredis.Redis(
            host=self.config.REDIS_HOST,
            port=self.config.REDIS_PORT,
            decode_responses=True,
        )
        await self.client.ping()
        self._confirm = self.client.register_script(CONFIRM_LUA)
        logger.info("Connected to Redis (stock instance)")

    async def confirm_reservation(self, product_id: str, order_id: str) -> int:
        keys = [
            f"{{sale:{product_id}}}:resv:{order_id}",
            f"{{sale:{product_id}}}:resv_deadlines",
        ]
        try:
            return int(await self._confirm(keys=keys, args=[order_id]))
        except Exception:
            # Connection blip / script error: let the handler's except block
            # nak for redelivery, but count it as a Redis error, not a
            # generic order-processing failure.
            metrics.redis_errors.labels(type="confirm_cas_error").inc()
            raise

    async def close(self):
        if self.client:
            await self.client.aclose()
