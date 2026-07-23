import asyncio
import json
import os
import signal

from nats.js.api import ConsumerConfig, AckPolicy
from initialiser.initialiser import init_dependencies
from datastore.nats.nats import ORDER_SUBJECT
from handler.handler import create_order_handler
from tracing import init_tracing
from utils.logger import get_logger
from utils import metrics

logger = get_logger()

async def poll_consumer_lag(js, nats_connection, stop_event):
    """Refresh the lag gauge AND beacon it to the gateway every ~1s.

    The gateway subscribes to core-NATS 'control.orders.lag' for back-pressure;
    publish is fire-and-forget (core NATS, not JetStream)."""
    while not stop_event.is_set():
        try:
            info = await js.consumer_info("ORDERS", "order_processor")
            num_pending = info.num_pending
            metrics.consumer_lag.set(num_pending)
            await nats_connection.publish(
                "control.orders.lag", json.dumps({"lag": num_pending}).encode()
            )
        except Exception as e:
            logger.warning("lag_poll_failed", error=str(e))
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1)
        except asyncio.TimeoutError:
            pass

async def main():
    # 0. Tracing — must instrument asyncpg before the pool is built.
    init_tracing(
        os.getenv("OTEL_SERVICE_NAME", "order-service"),
        os.getenv("SERVICE_VERSION", "unknown"),
        os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
    )

    # 1. Init Dependencies
    deps = await init_dependencies()
    cfg = deps.config

    # 2. Metrics endpoint
    metrics.start_metrics_server(cfg.METRICS_PORT)
    logger.info(f"Metrics on :{cfg.METRICS_PORT}/metrics")

    # 3. Setup Handler
    handler = create_order_handler(deps.db_client, deps.redis_client,
                                   deps.nats_client, cfg.MAX_DELIVER,
                                   max_concurrency=cfg.MAX_ACK_PENDING)

    # 4. Subscribe — durable + queue group for load balancing across replicas.
    # ack_wait must comfortably exceed p99 processing (~1s) or redelivery of
    # in-flight work manufactures duplicate-processing storms; max_ack_pending
    # stays below the DB pool so concurrency is real, not queueing.
    consumer_cfg = ConsumerConfig(
        ack_policy=AckPolicy.EXPLICIT,
        ack_wait=cfg.ACK_WAIT_SECONDS,
        max_deliver=cfg.MAX_DELIVER,
        max_ack_pending=cfg.MAX_ACK_PENDING,
    )
    # nats-py requires queue == durable for queue-group push subscriptions.
    sub = await deps.jetstream.subscribe(
        ORDER_SUBJECT,
        durable="order_processor",
        queue="order_processor",
        cb=handler,
        manual_ack=True,
        config=consumer_cfg,
    )
    logger.info("Subscribed to 'orders.pending' (JetStream durable, explicit ack)")

    # 5. Run until signal
    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("Signal received, shutting down...")
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    lag_task = asyncio.create_task(
        poll_consumer_lag(deps.jetstream, deps.nats_connection, stop_event)
    )
    await stop_event.wait()

    # 6. Graceful shutdown: stop admitting new messages, then let in-flight
    # ones actually finish (CAS + Postgres commit + ack) before tearing down
    # the pool/redis/nats — closing those out from under a running
    # process_message would abort a message post-CAS/pre-ack with no ack
    # ever sent, forcing a redelivery-driven retry instead of a clean one.
    # Unacked messages redeliver cleanly after ack_wait regardless, so this
    # is a latency optimization for shutdown, not a correctness requirement,
    # but it avoids manufacturing avoidable redeliveries on every deploy.
    logger.info("Closing connections...")
    lag_task.cancel()
    await sub.unsubscribe()
    if handler.pending_tasks:
        logger.info("waiting_for_inflight", count=len(handler.pending_tasks))
        await asyncio.gather(*handler.pending_tasks, return_exceptions=True)
    await deps.nats_client.close()
    await deps.redis_client.close()
    await deps.db_client.close()
    logger.info("Shutdown complete.")

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
