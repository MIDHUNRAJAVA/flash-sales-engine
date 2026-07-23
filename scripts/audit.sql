-- audit.sql — Postgres-side integrity audit for the flash-sale engine.
--
-- Usage:
--   psql "host=localhost port=5432 dbname=flashsale user=flashsale_user" -f scripts/audit.sql
-- Override the product under audit by passing -v pid=p2, or edit the \set below.
-- (psql resolves -v/--set BEFORE the \set here would run only if you comment it out;
--  simplest override: `psql -v pid=p2 -f scripts/audit.sql` and delete/comment the next line.)
\set pid 'p1'

\echo '================================================================'
\echo 'Flash-sale audit for product:' :'pid'
\echo '================================================================'

-- ----------------------------------------------------------------------------
-- ZERO-OVERSELL AUDIT EQUATION
-- ----------------------------------------------------------------------------
-- The invariant the whole system defends is conservation of stock. At any point:
--
--     initial_stock  ==  redis_available_stock
--                      +  SUM(confirmed order quantity)      <- this file, check #1
--                      +  SUM(open (un-expired) reservations)
--
-- Every unit of inventory is in exactly one of three buckets: still sellable
-- (redis stock counter), sold and durably recorded (Postgres CONFIRMED rows),
-- or held in-flight by a reservation that has neither confirmed nor expired
-- (redis reservation zset). Oversell == the sum EXCEEDS initial_stock. Undersell
-- (leak) == the sum is LESS than initial_stock (units stuck in limbo).
--
-- SQL can only see the Postgres term. The redis terms (available stock and open
-- reservation count via ZCARD {sale:<pid>}:resv_deadlines) live in redis and are
-- read/asserted in scripts/chaos_test.sh, which combines all three terms and
-- checks the full equation. This file audits the durable-truth side.
-- ----------------------------------------------------------------------------

-- 1. Total confirmed quantity (the Postgres term of the audit equation).
\echo '--- [1] Total CONFIRMED quantity (durable sold units) ---'
SELECT COALESCE(SUM(quantity), 0) AS total_confirmed_qty
FROM orders
WHERE product_id = :'pid'
  AND status = 'CONFIRMED';

-- 2. Duplicate order_ids — MUST return 0 rows.
--    order_id is the PK, so a nonzero result means either a corrupted table or a
--    broken idempotency path that inserted the same logical order twice.
\echo '--- [2] Duplicate order_ids (MUST be 0 rows) ---'
SELECT order_id, count(*)
FROM orders
GROUP BY order_id
HAVING count(*) > 1;

-- 3. Per-user max purchased quantity — MUST be <= MAX_PER_USER (=2).
--    Enforces the per-user quota end-to-end at the durable layer: even under
--    races/retries, no user may have more than 2 confirmed units.
\echo '--- [3] Per-user max quantity (MUST be <= 2) ---'
WITH per_user AS (
    SELECT user_id, SUM(quantity) AS user_qty
    FROM orders
    WHERE product_id = :'pid'
      AND status = 'CONFIRMED'
    GROUP BY user_id
)
SELECT
    COALESCE(MAX(user_qty), 0)                        AS max_qty_any_user,
    2                                                 AS max_per_user_limit,
    COALESCE(MAX(user_qty), 0) <= 2                   AS within_quota,
    COUNT(*) FILTER (WHERE user_qty > 2)              AS users_over_quota
FROM per_user;

-- Show any offending users explicitly (should be empty).
\echo '--- [3b] Users OVER quota (MUST be 0 rows) ---'
SELECT user_id, SUM(quantity) AS user_qty
FROM orders
WHERE product_id = :'pid'
  AND status = 'CONFIRMED'
GROUP BY user_id
HAVING SUM(quantity) > 2
ORDER BY user_qty DESC;

-- 4. Total row count for the product (all statuses) — sanity / volume check.
\echo '--- [4] Total order rows for product (all statuses) ---'
SELECT
    count(*)                                        AS total_rows,
    count(*) FILTER (WHERE status = 'CONFIRMED')    AS confirmed_rows
FROM orders
WHERE product_id = :'pid';

\echo '================================================================'
\echo 'Audit complete. Combine [1] with redis stock + ZCARD resv_deadlines'
\echo 'in chaos_test.sh to verify the full zero-oversell equation.'
\echo '================================================================'
