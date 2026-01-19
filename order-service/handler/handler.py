import json
from actions.actions import process_order
from datastore.postgres.postgres import PostgresClient
from utils.logger import get_logger

logger = get_logger()

def create_order_handler(db_client: PostgresClient):
    async def message_handler(msg):
        subject = msg.subject
        try:
            data = json.loads(msg.data.decode())
            logger.info(f"Received order: {data.get('order_id')}")
            
            await process_order(db_client, data)
            await msg.ack()
            
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON format: {e}")
            await msg.term() # Do not retry malformed messages
            
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            await msg.nak()
            
    return message_handler
