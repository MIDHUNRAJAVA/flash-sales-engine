from config.config import Config
from datastore.postgres.postgres import PostgresClient
from datastore.nats.nats import NatsClient
from datastore.redis.redis import RedisClient

class Dependencies:
    def __init__(self, config, db_client, redis_client, nats_client, nats_connection, jetstream):
        self.config = config
        self.db_client = db_client
        self.redis_client = redis_client
        self.nats_client = nats_client
        self.nats_connection = nats_connection
        self.jetstream = jetstream

async def init_dependencies():
    # 1. Config
    config = Config()

    # 2. Postgres
    db_client = PostgresClient(config)
    await db_client.connect()

    # 3. Redis (stock instance — reservation confirm CAS lives there)
    redis_client = RedisClient(config)
    await redis_client.connect()

    # 4. NATS
    nats_client = NatsClient(config)
    nats_connection, jetstream = await nats_client.connect()

    return Dependencies(config, db_client, redis_client, nats_client, nats_connection, jetstream)
