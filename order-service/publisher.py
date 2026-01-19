import asyncio
import nats
import json
from config.config import Config

async def main():
    config = Config()
    
    # Connect to NATS
    nc = await nats.connect(
        servers=[config.NATS_URL],
        user=config.NATS_USER,
        password=config.NATS_PASSWORD
    )
    
    js = nc.jetstream()

    # Create a sample order
    order_payload = {
        "order_id": "order_001",
        "user_id": "user_test",
        "product_id": "product_xyz",
        "quantity": 2,
        "status": "PENDING"
    }

    # Publish to the stream
    ack = await js.publish("orders.pending", json.dumps(order_payload).encode())
    print(f"Published message: {order_payload}")
    print(f"Ack: stream={ack.stream}, seq={ack.seq}")

    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
