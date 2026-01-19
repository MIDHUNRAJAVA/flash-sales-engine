import asyncio
from initialiser.initialiser import init_dependencies
from handler.handler import create_order_handler
from utils.logger import get_logger

import signal

logger = get_logger()

async def main():
    # 1. Init Dependencies
    deps = await init_dependencies()
    
    # 2. Setup Handler
    handler = create_order_handler(deps.db_client)
    
    # 3. Subscribe
    # Use JetStream with a durable consumer and queue group for persistence + load balancing
    await deps.jetstream.subscribe("orders.pending", durable="order_processor", cb=handler)
    logger.info("Subscribed to 'orders.pending' (JetStream Durable)")
    
    # 4. Keep running until signal
    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("Signal received, shutting down...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    await stop_event.wait()

    # 5. Cleanup
    logger.info("Closing connections...")
    await deps.db_client.close()
    await deps.nats_client.close()
    logger.info("Shutdown complete.")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
