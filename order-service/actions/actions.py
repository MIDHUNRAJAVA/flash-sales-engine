import asyncio
from datastore.postgres.postgres import PostgresClient
from models.order import Order
from utils.logger import get_logger

logger = get_logger()

async def process_order(db_client: PostgresClient, order_data: dict):
    # Convert dict to model (validates data)
    order = Order(**order_data)
    
    # Simulate Processing (Payment Gateway)
    await asyncio.sleep(0.5) 
    
    # Update status
    order.status = 'CONFIRMED'
    
    # Persist
    if await db_client.insert_order(order):
        logger.info(f"Order {order.order_id} processed and saved.")
    else:
        error_msg = f"Failed to save order {order.order_id}"
        logger.error(error_msg)
        raise Exception(error_msg)
