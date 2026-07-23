import nats
from nats.js.api import StreamConfig, RetentionPolicy, DiscardPolicy, StorageType
from nats.js.errors import NotFoundError
from config.config import Config
from utils.logger import get_logger

logger = get_logger()

ORDER_SUBJECT = "orders.pending"
DLQ_SUBJECT = "orders.dlq"


class NatsClient:
    def __init__(self, config: Config):
        self.config = config
        self.nats_connection = None
        self.js = None

    async def connect(self):
        self.nats_connection = await nats.connect(
            servers=[self.config.NATS_URL],
            user=self.config.NATS_USER,
            password=self.config.NATS_PASSWORD,
            max_reconnect_attempts=-1,
        )
        self.js = self.nats_connection.jetstream()
        logger.info("Connected to NATS")

        await self._ensure_streams()
        return self.nats_connection, self.js

    async def _ensure_streams(self):
        # Identical config to the inventory service's ensureStreams — whichever
        # service starts first creates them; both must agree exactly.
        # ORDERS is a WorkQueue with DiscardNew: depth == unprocessed work, and
        # at capacity the PUBLISH fails (compensation path) instead of silently
        # deleting an accepted order (which DiscardOld would do).
        streams = [
            StreamConfig(
                name="ORDERS",
                subjects=[ORDER_SUBJECT],
                retention=RetentionPolicy.WORK_QUEUE,
                discard=DiscardPolicy.NEW,
                storage=StorageType.FILE,
                max_msgs=100_000,
                max_bytes=64 * 1024 * 1024,
                max_age=24 * 3600,
                duplicate_window=600,
            ),
            StreamConfig(
                name="ORDERS_DLQ",
                subjects=[DLQ_SUBJECT],
                retention=RetentionPolicy.LIMITS,
                discard=DiscardPolicy.OLD,
                storage=StorageType.FILE,
                max_age=168 * 3600,
            ),
        ]
        for sc in streams:
            try:
                await self.js.stream_info(sc.name)
                logger.info(f"Stream '{sc.name}' already exists")
            except NotFoundError:
                await self.js.add_stream(sc)
                logger.info(f"Stream '{sc.name}' created")

    async def publish_dlq(self, data: bytes, headers: dict | None = None):
        await self.js.publish(DLQ_SUBJECT, data, headers=headers)

    async def close(self):
        if self.nats_connection:
            await self.nats_connection.close()
