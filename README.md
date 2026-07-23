# Flash-Sale Engine

A production-grade flash-sale engine: hundreds of thousands of buyers stampede a tiny stock (e.g. 2000 units) in the same second, and the system holds a **hard zero-oversell invariant** — stock can never go below zero, an accepted order is never silently lost, a duplicate request never produces a second order, and every buyer gets a *defined* response (never a hang, never a 500 for a policy decision). Every correctness decision is collapsed into one atomic Redis Lua script, the message leg is made durable with JetStream PubAck + dedup + compensating transactions, and the hot path is fronted by four layers of admission control so at most a few thousand req/s ever reach the stock counter.

The full engineering rationale — every trade-off, failure scenario, and phase gate — lives in [`docs/production-plan.md`](docs/production-plan.md). This README is the operational summary.

---

## Architecture

```
                          500k buyers
                              │
                    ┌─────────▼─────────┐
                    │  nginx (edge, :80)│  TLS, per-IP limit_req/limit_conn,
                    │  Layer 0/1        │  slowloris timeouts, security headers
                    └─────────┬─────────┘
                              │  proxy_pass
                    ┌─────────▼───────────────────────┐
                    │  gateway  (Node, :3000)          │  Layer 2/3/4
                    │  hybrid token-bucket + sliding   │◄── redis-ratelimit
                    │  window limiter, waiting room,   │    (noeviction)
                    │  in-flight cap, circuit breaker  │
                    └─────────┬────────────────────────┘
                              │  POST /buy
                    ┌─────────▼───────────────────────┐
                    │  inventory (Go, :8080)           │
                    │  ATOMIC Lua buy.lua:             │◄── redis-stock
                    │  idempotency + quota + stock     │    (AOF)
                    │  decrement + reservation, one    │
                    │  serialized step                 │
                    └─────────┬────────────────────────┘
                              │  js.Publish + PubAck + Nats-Msg-Id
                    ┌─────────▼───────────────────────┐
                    │  NATS JetStream (:4222/:8222)    │  WorkQueue, R=3, DiscardNew,
                    │  stream ORDERS + ORDERS_DLQ      │  10m dedup window
                    └─────────┬────────────────────────┘
                              │  pull, explicit ack (after commit)
                    ┌─────────▼───────────────────────┐
                    │  order-service (Python, :9100)   │
                    │  confirm.lua CAS → Postgres      │──► postgres (:5432)
                    │  insert (UNIQUE order_id) → ack  │──► redis-stock (confirm)
                    └──────────────────────────────────┘

  Observability plane (side-car to every hop):
  metrics ──► Prometheus (:9090) ──► Grafana (:3001) + Alertmanager (:9093)
  traces  ──► OTel Collector (:4317) ──► Tempo (:3200)      [W3C traceparent, incl. NATS hop]
  logs    ──► Vector ──► Loki (:3100)                       [structured JSON, PII-scrubbed]
```

---

## The core guarantee

Stock is never oversold, accepted orders never vanish, duplicates never double-charge, and every request gets a defined answer. All four are provable **post-sale by a closed-form audit equation** that must balance to the unit:

```
redis_stock  +  Σ confirmed_qty (Postgres)  +  Σ open_reservations (RESERVED)  =  initial_stock
```

Correctness lives entirely in the Redis Lua layer (single command thread → serialized snapshot). Admission control (nginx, rate limiter, waiting room) protects the latency SLO and fairness — it can fail open without ever causing an oversell.

---

## The 4 bugs fixed

| # | Bug | Fix |
|---|-----|-----|
| **1** | Core NATS `Publish()` returned before durability — an "accepted" order could be lost. | Synchronous `js.Publish(ctx)` that returns only **after the PubAck** (Raft quorum has the message), with `Nats-Msg-Id` = idempotency key so the retry loop is safe. "Queued" now means *replicated*. |
| **2** | Stock leaked when publish failed after the decrement (stuck permanently in RESERVED). | Atomic **`cancel.lua`** compensation: CAS `RESERVED→CANCELLED` + `INCRBY` stock + quota rollback + idempotency-key delete, all in one script. A bare `INCRBY` would oversell on an ambiguous publish timeout; the CAS tombstone closes that hole. |
| **3** | Rate-limiter counters shared the same Redis as the stock counter — abuse traffic starved the sale. | Dedicated **`redis-ratelimit`** instance, `maxmemory-policy noeviction` so it fails *loud* (503, fail-closed) instead of silently evicting counters and letting everyone through. |
| **4** | Fixed-window limiter admitted 2× the limit at every window boundary. | **Hybrid token-bucket + sliding-window** evaluated in one Lua script, timestamps from Redis `TIME` (never gateway wall clock — 500 pods = 500 clocks). |

---

## Services

| Service | Language | Port | Responsibility |
|---------|----------|------|----------------|
| nginx | — | 80 | Edge: TLS, per-IP rate/conn limits, slowloris timeouts, security headers |
| gateway | Node.js | 3000 | Rate limiting, waiting room, per-instance in-flight cap, circuit breaker, identity/trace propagation |
| inventory | Go | 8080 | Atomic Lua reservation (idempotency + quota + stock), PubAck publish, compensation, expiry reaper |
| order-service | Python | 9100 (metrics) | JetStream consumer: `confirm.lua` CAS → Postgres insert → ack; DLQ routing, lag beacon |
| redis-stock | Redis | 6379 | Stock counter + reservation state machine (AOF persistence) |
| redis-ratelimit | Redis | (internal) | Rate-limit counters + waiting-room queue (noeviction) |
| nats | NATS JetStream | 4222 / 8222 | Durable `ORDERS` WorkQueue stream (R=3) + `ORDERS_DLQ` |
| postgres | PostgreSQL | 5432 | Durable order ledger, `UNIQUE(order_id)` duplicate backstop |
| prometheus | Prometheus | 9090 | Metrics scrape + alert rule evaluation |
| grafana | Grafana | 3001 | "Flash Sale War Room" dashboard (auto-provisioned) |
| alertmanager | Alertmanager | 9093 | Alert routing by severity (P1/P2/P3) |
| otel-collector | OTel Collector | 4317 | Trace ingest + spanmetrics → Prometheus |
| tempo | Tempo | 3200 | Distributed trace backend |
| loki | Loki | 3100 | Log backend |
| vector | Vector | (internal) | Log shipping + PII scrub → Loki |

---

## How to run

### Docker (single-node, all-in)

```bash
cp .env.example .env          # fill in POSTGRES_PASSWORD etc.
docker compose up -d --build
```

Exposed ports / URLs:

| Service | URL |
|---------|-----|
| Gateway (direct) | http://localhost:3000 |
| Gateway via nginx (real ingress) | http://localhost:80 |
| Inventory | http://localhost:8080 |
| Order-service metrics | http://localhost:9100/metrics |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3001 (admin / admin) |
| Alertmanager | http://localhost:9093 |
| Tempo | http://localhost:3200 |
| Loki | http://localhost:3100 |
| NATS monitoring | http://localhost:8222 |

> **Traffic path:** `POST /buy` is meant to flow **nginx → gateway → inventory**. Hitting inventory's `:8080/buy` directly **bypasses rate limiting and the waiting room** — it's for testing the atomic core in isolation, never a production path.

---

## API

### `POST /buy`

```json
{ "userID": "u-123", "productID": "flash-1", "quantity": 1, "clientNonce": "optional" }
```

`clientNonce` is normally minted by the gateway (stable across retries of one attempt, bound to the authenticated user); a client-supplied nonce is echoed for idempotency testing.

| Status | Meaning |
|--------|---------|
| `200 { order_id, ... }` | Reserved and durably queued. |
| `200 { duplicate: true, order_id }` | Idempotent replay — same request → same result (the client whose first response was lost). |
| `429` | Rate limited (token bucket) or per-user quota window exceeded; carries `Retry-After`, `X-RateLimit-*`. |
| `503` | Shed or queued — `{ error: "queued", position, eta_seconds, poll_url }` with `Retry-After`. A *shed*, not a bug. |
| `410` | Out of stock (sold out). |
| `409` | Per-user quantity quota exceeded. |

`500` is never returned for a rate-limit or shed decision — a limiter Redis error fails **closed** to 503.

### Other endpoints

| Endpoint | Purpose |
|----------|---------|
| `POST /seed` `{ productID, quantity }` | Pre-warm stock and open the sale (inventory). |
| `POST /queue/join` `{ userID }` | Enter the waiting room; returns position + ETA. |
| `GET /queue/position?userID=` | Poll queue position (cheap `ZRANK`). |
| `GET /health` | Per-service health / breaker state. |
| `GET /metrics` | Prometheus exposition on every service. |

---

## Observability

- **Structured logs** — JSON on every line, closed event vocabulary, `trace_id`/`span_id` on every record so logs and traces are two views of one graph. `user_id` is **sha256-hashed** and IPs masked to /24 in the logging layer; Vector re-applies the PII scrub in-pipeline (defense in depth) → Loki (hot 7d, cold 90d).
- **Distributed traces** — one trace spans gateway → inventory → Redis → NATS → order-service → Postgres. The **NATS message header carries W3C `traceparent`**, so the trace survives the queue boundary (where the interesting async latency lives). The gateway does *not* trust an inbound client `traceparent`; it starts a fresh trace. OTel SDKs → Collector → Tempo; head-sampled 100% during the sale window.
- **Metrics** — unsampled Prometheus counters/histograms with **bounded label cardinality** (`service, endpoint, method, status_code, result` — never `user_id`). The auto-provisioned **"Flash Sale War Room"** Grafana dashboard: sale health (`stock_remaining` gauge, confirmed vs. stock, acceptance %), latency p50/95/99 per hop, throughput/errors, infra saturation.
- **Alerting** — rules in [`alerts.yml`](alerts.yml) routed by Alertmanager by severity: **P1** (page now: stock leaked, `compensation_failed > 0`, 5xx > 1%, oversell rule), **P2** (page ≤5m: decrement p99 > 10ms, publish failure ratio, DLQ > 10, consumer lag), **P3** (Slack: 429 ratio, queue depth, DLQ > 0). Invariant alerts fire from **metrics**, never from the sampled/best-effort trace pipeline.

---

## Load testing

```bash
k6 run -e SCALE=0.01 load_test.js          # laptop smoke test (default)
k6 run -e SCALE=1    load_test.js          # full sale-shaped load — run DISTRIBUTED
```

`SCALE` linearly scales the `ramping-arrival-rate` executor: `SCALE=1` ramps to ~25,000 req/s (what ~500k humans generate through the edge layers). VUs model connections, but arrival *rate* is what stresses the stock counter — so the test asserts at that layer, and at `SCALE=1` you run it across ~10–40 distributed runners (k6-operator / k6 Cloud), not 500k VUs on one box.

Scenarios: `flood` (the herd, unique user per iteration), `replay` (one user + one nonce × 200 → exactly one order), `burst` (Bug 4 boundary regression — 10 rapid buys → exactly 1 success per 10s).

Thresholds / assertions:
- `accepted_orders count <= STOCK` — **the invariant: no oversell**.
- `server_errors count == 0` — a 500 is a bug, not a policy.
- `buy_latency p(99) < 500ms`; every response is one of a defined set (never a hang).
- **Post-run SQL duplicate audit** (the DB is the system of record; k6 can't hold cross-VU state):
  ```sql
  SELECT order_id, count(*) FROM orders GROUP BY order_id HAVING count(*) > 1;  -- must return 0 rows
  ```

---

## Repository layout

| Path | Description |
|------|-------------|
| `nginx/` | Edge nginx config (rate limits, timeouts, security headers) |
| `gateway-service/` | Node.js gateway: limiter, waiting room, back-pressure, breaker, tracing |
| `inventory-service/` | Go inventory service: atomic Lua buy/cancel scripts, PubAck publish, reaper |
| `order-service/` | Python JetStream consumer: confirm CAS, Postgres persistence, DLQ |
| `otel/` | OpenTelemetry Collector config (OTLP ingest + spanmetrics) |
| `tempo/` | Tempo trace backend config |
| `loki/` | Loki log backend config |
| `vector/` | Vector log shipping + PII scrub config |
| `grafana/` | Grafana provisioning + "War Room" dashboard JSON |
| `alertmanager/` | Alertmanager routing config |
| `prometheus.yml` | Prometheus scrape + rule-file config |
| `alerts.yml` | Prometheus alert rules (P1/P2/P3) |
| `docker-compose.yml` | Full single-node stack |
| `load_test.js` | k6 load + correctness harness |
| `.env.example` | Environment template (`cp` → `.env`) |
| `docs/production-plan.md` | Full engineering plan: trade-offs, failure scenarios, runbooks |

---

## Verified

The correctness core was exercised end-to-end:

- **Zero oversell under concurrent load** — thousands of concurrent buys against tiny stock leave `GET stock` at 0, never negative; Postgres row count equals stock exactly; no user exceeds `MAX_PER_USER`.
- **Atomic compensation on NATS failure** — killing NATS mid-flood leaves the audit equation balanced to the unit and `compensation_failed == 0`; stock is restored on every failed publish.
- **Reservation expiry reaper** — reservations for a dead order-service return to stock at TTL, and redelivered messages hit `confirm.lua → 0` (dead reservation) and are dropped without a Postgres write.
- **Idempotent replay** — the same idempotency key fired hundreds of times yields exactly one decrement and one `order_id`; all replays return that same order.
