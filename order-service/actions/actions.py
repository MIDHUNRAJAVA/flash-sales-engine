import asyncio
import time
from opentelemetry import trace
from datastore.postgres.postgres import PostgresClient
from models.order import Order
from utils.logger import get_logger
from utils import metrics

logger = get_logger()
tracer = trace.get_tracer(__name__)

async def process_order(db_client: PostgresClient, order: Order):
    # Simulate Processing (Payment Gateway)
    pay_start = time.monotonic()
    with tracer.start_as_current_span("payment.simulate"):
        await asyncio.sleep(0.5)
    metrics.payment_simulation_duration.observe(time.monotonic() - pay_start)
    logger.info("payment_simulated", order_id=order.order_id)

    order.status = 'CONFIRMED'

    # Persist — idempotent (ON CONFLICT DO NOTHING on the order_id PK), so a
    # redelivery after a crash-before-ack re-runs this safely.
    db_start = time.monotonic()
    with tracer.start_as_current_span(
        "postgres.insert_order", attributes={"db.sql.table": "orders"}
    ) as span:
        logger.info("db_insert_attempted", order_id=order.order_id)
        ok = await db_client.insert_order(order)
        span.set_attribute("rows_affected", 1 if ok else 0)
    metrics.db_insert_duration.observe(time.monotonic() - db_start)

    if ok:
        logger.info("db_insert_succeeded", order_id=order.order_id)
    else:
        logger.error("db_insert_failed", order_id=order.order_id)
        raise Exception(f"Failed to save order {order.order_id}")
