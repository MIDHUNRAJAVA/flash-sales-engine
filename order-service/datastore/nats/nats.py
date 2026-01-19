import nats
from config.config import Config
from utils.logger import get_logger

logger = get_logger()

from nats.js.errors import NotFoundError

class NatsClient:
    def __init__(self, config: Config):
        self.config = config
        self.nats_connection = None

    async def connect(self):
        try:
            self.nats_connection = await nats.connect(
                servers=[self.config.NATS_URL],
                user=self.config.NATS_USER,
                password=self.config.NATS_PASSWORD
            )
            self.js = self.nats_connection.jetstream()
            logger.info("Connected to NATS")
            
            await self._setup_stream()
            
            return self.nats_connection, self.js
        except Exception as e:
            logger.error(f"NATS connection error: {e}")
            raise e

    async def _setup_stream(self):
        try:
            await self.js.stream_info("orders_stream")
            logger.info("JetStream 'orders_stream' already exists")
        except NotFoundError:
            await self.js.add_stream(name="orders_stream", subjects=["orders.>"])
            logger.info("JetStream 'orders_stream' created")
        except Exception as e:
            logger.error(f"Error checking/creating stream: {e}")

    async def close(self):
        if self.nats_connection:
            await self.nats_connection.close()
