import asyncio
import json
import nats
import psycopg2
from psycopg2 import sql
import os

# Database Config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "flashsale")
DB_USER = os.getenv("DB_USER", "user")
DB_PASS = os.getenv("DB_PASS", "password")

async def main():
    # 1. Connect to NATS
    nats_url = os.getenv("NATS_URL", "nats://localhost:4222")
    nc = await nats.connect(nats_url)
    print("Connected to NATS")

    # 2. Connect to Postgres (Synchronous for simplicity in this demo, usually use asyncpg)
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create Table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                order_id VARCHAR(255) PRIMARY KEY,
                user_id VARCHAR(255),
                product_id VARCHAR(255),
                quantity INT,
                status VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        print("Connected to Postgres and ensured table exists")
    except Exception as e:
        print(f"Database error: {e}")
        return

    # 3. Define Message Handler
    async def message_handler(msg):
        subject = msg.subject
        data = json.loads(msg.data.decode())
        print(f"Received order: {data['order_id']}")

        # Simulate Processing (Payment Gateway)
        await asyncio.sleep(0.5) # Simulate latency

        try:
            # Insert into Postgres
            cursor.execute("""
                INSERT INTO orders (order_id, user_id, product_id, quantity, status)
                VALUES (%s, %s, %s, %s, %s)
            """, (data['order_id'], data['user_id'], data['product_id'], data['quantity'], 'CONFIRMED'))
            
            print(f"Order {data['order_id']} processed and saved.")
            
            # Publish Confirmation (Optional)
            # await nc.publish("orders.confirmed", json.dumps({"order_id": data['order_id']}).encode())
            
        except Exception as e:
            print(f"Failed to process order {data['order_id']}: {e}")

    # 4. Subscribe
    sub = await nc.subscribe("orders.pending", cb=message_handler)
    print("Subscribed to 'orders.pending'")

    # Keep running
    while True:
        await asyncio.sleep(1)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
