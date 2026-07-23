import asyncio
import json
import time
from pydantic import ValidationError
from opentelemetry import trace
from opentelemetry.trace import SpanKind
from opentelemetry.propagate import extract
from actions.actions import process_order
from datastore.postgres.postgres import PostgresClient
from datastore.redis.redis import RedisClient
from models.order import Order
from utils.logger import get_logger
from utils import metrics

logger = get_logger()
tracer = trace.get_tracer(__name__)


def create_order_handler(db_client: PostgresClient, redis_client: RedisClient,
                         nats_client, max_deliver: int, max_concurrency: int = 16):
    # nats-py awaits the subscription callback serially — without dispatching
    # to tasks, MaxAckPending "permits" 16 in flight but actual concurrency is
    # 1 (0.5s payment => 2 orders/s instead of 32/s). The semaphore keeps the
    # real concurrency at the sized value, bounded below the DB pool.
    sem = asyncio.Semaphore(max_concurrency)
    tasks = set()

    async def message_handler(msg):
        await sem.acquire()
        task = asyncio.create_task(process_message(msg))
        tasks.add(task)
        task.add_done_callback(lambda t: (tasks.discard(t), sem.release()))

    # Exposed so main.py can drain in-flight processing (CAS + Postgres +
    # ack) on shutdown before tearing down the DB pool / Redis / NATS conns.
    message_handler.pending_tasks = tasks

    async def _finalize(msg, ack_type, delivered, **kwargs):
        # ack/nak/term under a child span so the terminal decision is visible.
        with tracer.start_as_current_span(
            "nats.ack", attributes={"ack_type": ack_type, "delivery_count": delivered}
        ):
            if ack_type == "ack":
                await msg.ack()
            elif ack_type == "nak":
                await msg.nak(**kwargs)
            else:
                await msg.term()

    async def process_message(msg):
        start = time.monotonic()
        delivered = msg.metadata.num_delivered if msg.metadata else 1

        # Continuation of the buy trace: the inventory Go service injects a W3C
        # traceparent header — extract it so this span links to that trace.
        ctx = extract(dict(msg.headers or {}))
        with tracer.start_as_current_span(
            "order.process",
            context=ctx,
            kind=SpanKind.CONSUMER,
            attributes={"messaging.system": "nats", "message_delivery_count": delivered},
        ):
            # ── Permanent failures: DLQ first, then term (never redelivered) ──
            try:
                data = json.loads(msg.data.decode())
                order = Order(**data)
            except (json.JSONDecodeError, ValidationError) as e:
                logger.error("dlq_forwarded", reason="malformed", error=str(e),
                             message_delivery_count=delivered)
                await nats_client.publish_dlq(msg.data, headers={"X-Failure": "malformed"})
                metrics.orders_processed.labels(result="dlq").inc()
                await _finalize(msg, "term", delivered)
                logger.info("message_termed", reason="malformed",
                            message_delivery_count=delivered)
                return

            logger.info("message_received", order_id=order.order_id,
                        message_delivery_count=delivered)

            try:
                # Claim the reservation BEFORE side effects: once CONFIRMED the
                # expiry reaper cannot return its stock mid-processing.
                claim = await redis_client.confirm_reservation(order.product_id, order.order_id)
                if claim == 0:
                    # Reservation was cancelled (publish compensation) or expired:
                    # stock already went back. Fulfilling now would oversell.
                    logger.warning("dropped_dead_reservation", order_id=order.order_id,
                                   result="dropped_dead_reservation")
                    # ack first: if the ack itself fails, we must fall through to
                    # the except block below and count it there, not here too.
                    await _finalize(msg, "ack", delivered)
                    metrics.orders_processed.labels(result="dropped_dead_reservation").inc()
                    return
                # claim == 1 (ours now) or -1 (claimed on an earlier delivery that
                # crashed before ack) — either way proceed; the insert is idempotent.

                await process_order(db_client, order)

                # ack before counting "success": if the ack call itself raises
                # (e.g. NATS conn blip), we must land in the except block below
                # and count it once as failed/dlq — not double-count it here too.
                await _finalize(msg, "ack", delivered)
                metrics.orders_processed.labels(result="success").inc()
                metrics.order_processing_duration.observe(time.monotonic() - start)
                logger.info("message_acked", order_id=order.order_id, result="success",
                            message_delivery_count=delivered)

            except Exception as e:
                # Transient failure (DB down, Redis blip, breaker OPEN). nak ->
                # redelivery with delay; after max_deliver attempts, park it in
                # the DLQ instead of poisoning the queue forever.
                logger.error("order_processing_error", order_id=order.order_id,
                             message_delivery_count=delivered, max_deliver=max_deliver,
                             error=str(e))
                if delivered >= max_deliver:
                    await nats_client.publish_dlq(msg.data, headers={"X-Failure": "max_deliver"})
                    await _finalize(msg, "term", delivered)
                    metrics.orders_processed.labels(result="dlq").inc()
                    logger.info("dlq_forwarded", order_id=order.order_id, reason="max_deliver",
                                message_delivery_count=delivered)
                    logger.info("message_termed", order_id=order.order_id, reason="max_deliver",
                                message_delivery_count=delivered)
                else:
                    await _finalize(msg, "nak", delivered, delay=5)
                    metrics.orders_processed.labels(result="failed").inc()
                    logger.info("message_naked", order_id=order.order_id, result="failed",
                                message_delivery_count=delivered)

    return message_handler
