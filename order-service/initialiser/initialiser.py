from config.config import Config
from datastore.postgres.postgres import PostgresClient
from datastore.nats.nats import NatsClient

class Dependencies:
    def __init__(self, config, db_client, nats_client, nats_connection, jetstream):
        self.config = config
        self.db_client = db_client
        self.nats_client = nats_client
        self.nats_connection = nats_connection
        self.jetstream = jetstream

async def init_dependencies():
    # 1. Config
    config = Config()
    
    # 2. Postgres
    db_client = PostgresClient(config)
    await db_client.connect()
    
    # 3. NATS
    nats_client = NatsClient(config)
    nats_connection, jetstream = await nats_client.connect()
    
    return Dependencies(config, db_client, nats_client, nats_connection, jetstream)
