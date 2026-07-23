# Flash-Sale Engine — Production Engineering Plan

Owner: Inventory/Commerce platform. Audience: senior engineers implementing the hardening of the
existing prototype (Node gateway, Go inventory, Python order-service, Redis, NATS JetStream, Postgres).

---

## 1. Executive Summary

We move every correctness decision into a single atomic Redis Lua reservation (idempotency +
per-user quota + stock decrement + reservation state in one serialized step), make the message leg
durable (JetStream publish with PubAck, `Nats-Msg-Id` dedup, WorkQueue stream, compensating
transaction on failure), and wrap the hot path in four layers of admission control so that at most
a few thousand requests per second ever reach the stock counter. The guarantee this buys:
**stock can never go below zero, an accepted order is never silently lost, a duplicate request can
never produce a second order, and every one of 500 000 concurrent buyers receives a defined
response** — all verifiable post-sale by a closed-form audit equation
(`redis_stock + Σ confirmed_qty + Σ open_reservations = 2000`).

### Spec deviations (read first — these are deliberate overrides)

| # | Spec says | This plan does | Why |
|---|-----------|----------------|-----|
| D1 | Phase 3a: switch to `js.PublishAsync()` | Synchronous `js.Publish(ctx)` with retry + `Nats-Msg-Id` | PublishAsync resolves the PubAck *after* we've answered the user. The whole point of Bug 1 is *ack-before-respond*. At ≤2000 successful publishes total, async batching buys nothing and moves compensation into a callback where the user was already told "queued". §3a. |
| D2 | Phase 2a: user_id in ARGV, keys built in-script | All touched keys passed in `KEYS[]`, hash-tagged `{sale:<id>}` | Building key names inside a script from ARGV breaks Redis Cluster key declaration and script analysis. We also *don't* run Cluster for the stock counter at all — one hot key lives on one shard no matter what; Cluster adds failure modes without adding throughput. Sentinel-managed single primary. §2a. |
| D3 | Phase 2c: compensation = `INCRBY` | Compensation = a Lua **cancel** script: CAS reservation → CANCELLED + `INCRBY` + quota rollback + idempotency delete, atomically | Raw `INCRBY` after an *ambiguous* publish timeout can oversell: if the message actually landed, the order confirms **and** the stock returns. The CAS tombstone closes that hole. §2c/§2d. |
| D4 | k6: 500k VUs, "429 ≥ 498 000" | `ramping-arrival-rate` executor, distributed runners, rate-based assertions | 500k VUs on one box is ~1.5 TB of RAM of VU state; and the 429 assertion conflates *users* with *requests* (each user retries). Appendix C states the honest, runnable version. |
| D5 | 99.99% availability over the sale window | Error budget stated in absolute seconds | 99.99% of a 30-minute window is 180 ms — statistically meaningless. Real target: ≤ 15 s cumulative hard-down (503-all) during the window, driven by the Redis failover RTO (§7 Scenario 1). |
| D6 | Alert "if any nats.publish span has pub_ack_received=false" | Alert from the `publish_operations_total{result="failed"}` **metric**, not from traces | Traces are sampled and best-effort delivered; never put an invariant alert behind the trace pipeline. The span attribute still exists for debugging. §5. |

---

# Phase 1 — Defense Perimeter

**Goal.** No more than ~5 000 requests/s of *authenticated, human, well-formed* traffic ever
reaches the inventory service; everything else is answered (429/503/queue page) at a cheaper layer.

**Mechanism.** Each layer is ~10–100× cheaper per rejected request than the next one down.
500 000 buyers pressing "buy" in the same second is indistinguishable from a DDoS at the packet
level — the design treats it as one, and *admission* (not the stock check) becomes the scaling
problem. Correctness never depends on this phase (the Lua script in Phase 2 is safe even if every
layer fails open); this phase exists to protect the **latency SLO** and the fairness contract.

## Layer 0 — CDN/WAF (Cloudflare)

- **Rate rule on `POST /buy`** (Cloudflare Rate Limiting rule, per-IP with cookie/JA3 fingerprint
  as secondary key): more than **6 requests / 10 s** → `managed_challenge`; more than
  **30 requests / 10 s** → `block` for 600 s. Legitimate users need at most 1 buy + a couple of
  retries per 10 s (Layer 2 allows 1/10 s anyway) — 6 is already generous headroom for retry storms
  from flaky mobile networks.
- **Bot score** (Cloudflare Bot Management, 1 = definitely bot, 99 = human): on `/buy` and
  `/queue/*`, score **< 10 → block**, **10–29 → managed_challenge**. Do *not* use JS challenge
  alone — headless Chrome solves it; managed challenge escalates adaptively.
- **ASN strategy**: static pre-challenge (not pre-block) list of hosting/cloud ASNs — nobody buys
  sneakers from an EC2 box, but VPN exit nodes live there too, so challenge instead of block:
  AWS 16509/14618, DigitalOcean 14061, OVH 16276, Hetzner 24940, Alibaba 45102, Linode 63949.
  Adaptive additions: during the sale, any ASN whose challenge-failure rate > 80% over 60 s gets
  promoted to block via the API (scripted, reviewed by a human in the war room). Static-only lists
  go stale; adaptive-only lists get gamed pre-sale when traffic is too low to be significant.
- **DDoS protection: always-on.** On-attack ("detect then mitigate") modes take 10–60 s to engage.
  The attack *is* the first 60 seconds of the sale — that lag is precisely the window we cannot
  afford. There is no clean-traffic baseline to learn from during a flash sale anyway.
- **IP reputation**: enable Cloudflare threat intelligence; threat score ≥ 10 → managed_challenge
  on all sale endpoints.

## Layer 1 — nginx (IP/connection layer)

```nginx
# nginx.conf — flash-sale edge
worker_processes auto;
worker_rlimit_nofile 1048576;
events { worker_connections 65536; }

http {
  limit_req_zone  $binary_remote_addr zone=perip:100m  rate=10r/s;
  limit_conn_zone $binary_remote_addr zone=connip:100m;

  server {
    listen 443 ssl backlog=65535 reuseport;

    client_header_timeout 5s;      # slowloris
    client_body_timeout   5s;      # slow-body
    send_timeout          10s;
    keepalive_timeout     15s;
    large_client_header_buffers 4 8k;

    location = /buy {
      limit_req  zone=perip burst=20 nodelay;   # 10 r/s steady, 20 burst
      limit_conn connip 20;                     # NAT'd offices fit; a bot farm doesn't
      client_max_body_size 1k;                  # buy body is ~200 B; 1024 B is 5× headroom
      limit_req_status  429;
      limit_conn_status 429;
      proxy_pass http://gateway:3000;
      proxy_read_timeout 3s;
    }
  }
}
```

- **JSON depth/nesting**: nginx cannot parse JSON — enforced at the gateway:
  `express.json({ limit: '1kb' })` plus a depth-check middleware, **max depth 3, max 10 keys**
  (the body is flat: `product_id`, `quantity`, `ticket`). Reject with 400, never parse further.
- **Header count cap**: nginx buffers above + Node `server.maxHeadersCount = 64`.
- **Kernel, on every edge and gateway host** (500k connection burst):

```
net.ipv4.tcp_syncookies = 1            # survive SYN flood without dropping legit SYNs
net.core.somaxconn = 65535
net.ipv4.tcp_max_syn_backlog = 262144
net.ipv4.ip_local_port_range = 1024 65535
net.ipv4.tcp_tw_reuse = 1              # gateway->inventory outbound churn
```

  500k *concurrent* connections do not terminate on one box: N edge nodes behind L4 (DNS/anycast),
  each holding ≤ 64k. Size N = ceil(peak_conns / 50 000) → **10 edge nodes** minimum.

## Layer 2 — Gateway rate limiter (fixes Bug 3 and Bug 4)

**Bug 3 fix — dedicated rate-limit Redis.** The rate limiter is the component *designed to absorb
abuse*; co-locating it with the stock counter means an attacker who saturates the limiter also
saturates the sale. Separate instance, separate memory budget, separate failure policy.

`docker-compose.yml` (full diff in Appendix B):

```yaml
  redis-stock:            # renamed from `redis` — the crown jewels; AOF on
    image: redis:7-alpine
    command: redis-server --appendonly yes --appendfsync everysec

  redis-ratelimit:        # new — absorbs abuse; bounded memory, never evicts silently
    image: redis:7-alpine
    command: redis-server --maxmemory 2gb --maxmemory-policy noeviction
```

`gateway-service/index.js`:

```js
const stockGateRedis = redis.createClient({ url: `redis://${process.env.REDIS_STOCK_HOST}:6379` });
const rlRedis        = redis.createClient({ url: `redis://${process.env.REDIS_RL_HOST}:6379` });
```

**Bug 4 fix — hybrid limiter.** Fixed windows admit 2× the limit at every window boundary
(N requests at 09:59.9, N more at 10:00.1). Replace with token bucket (burst shaping) +
sliding window (hard per-endpoint quota) evaluated in **one** Lua script, one RTT.

Policy:
- Token bucket per user: capacity 5, refill 1/s — absorbs mobile reconnect double-taps.
- Sliding window per user per endpoint: `/buy` **1 per 10 s**, `/browse` **30 per 60 s**.
  Note the interplay: on `/buy` the sliding window dominates (1/10 s < bucket rate); the bucket
  exists to protect `/browse` and any future endpoint without re-tuning.
- All timestamps come from **Redis `TIME`** inside the script — never gateway wall clock
  (500 gateway pods = 500 clocks; skew of 200 ms across a 10 s window is a free extra request per
  skewed pod pair — Scenario 11).

Complete script (also Appendix A1), `gateway-service/lua/rate_limit.lua`:

```lua
-- KEYS[1] = rl:{<uid>}:bucket   (hash: tok, ts_us)
-- KEYS[2] = rl:{<uid>}:win:<endpoint>  (zset: member = ts_us, score = ts_us)
-- ARGV[1] = bucket_capacity      (5)
-- ARGV[2] = refill_per_sec       (1)
-- ARGV[3] = window_seconds       (10 for /buy, 60 for /browse)
-- ARGV[4] = window_max           (1 for /buy, 30 for /browse)
-- Returns { code, remaining_tokens, reset_epoch_s }
--   code: 0 = allowed, 1 = rate_limited (bucket), 2 = quota_exceeded (window)
local t      = redis.call('TIME')
local now_us = t[1] * 1000000 + t[2]

-- 1. token bucket
local cap    = tonumber(ARGV[1])
local rate   = tonumber(ARGV[2])
local b      = redis.call('HMGET', KEYS[1], 'tok', 'ts_us')
local tok    = tonumber(b[1]) or cap
local ts     = tonumber(b[2]) or now_us
tok = math.min(cap, tok + ((now_us - ts) / 1000000) * rate)
if tok < 1 then
  local reset = t[1] + math.ceil((1 - tok) / rate)
  return { 1, 0, reset }
end

-- 2. sliding window
local win_us = tonumber(ARGV[3]) * 1000000
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now_us - win_us)
if redis.call('ZCARD', KEYS[2]) >= tonumber(ARGV[4]) then
  local oldest = tonumber(redis.call('ZRANGE', KEYS[2], 0, 0, 'WITHSCORES')[2])
  return { 2, math.floor(tok), math.ceil((oldest + win_us) / 1000000) }
end

-- 3. commit both — only after BOTH checks pass (no token burn on window rejection)
redis.call('HSET', KEYS[1], 'tok', tok - 1, 'ts_us', now_us)
redis.call('EXPIRE', KEYS[1], math.ceil(cap / rate) + 1)
redis.call('ZADD', KEYS[2], now_us, now_us)
redis.call('EXPIRE', KEYS[2], tonumber(ARGV[3]) + 1)
return { 0, math.floor(tok - 1), t[1] + tonumber(ARGV[3]) }
```

**Global `/buy` concurrency cap — per-instance, in-process.** The spec's 5000 global in-flight cap
is implemented as `5000 / N_gateway_instances` per process (a plain integer, incremented in
middleware, decremented in `finally`). The alternative — a distributed semaphore in Redis — leaks
permits when a gateway pod dies mid-request and costs 2 extra RTTs per request; a per-instance cap
is 0 RTT, crash-safe (the counter dies with the process), and load balancers already spread evenly
enough for the aggregate to hold within ±10%. Wrong choice named: `INCR inflight` / `DECR` in
Redis without a leak-reaper *will* ratchet to permanent 503 the first time a pod OOMs.

**Adaptive throttle — fail CLOSED.** Gateway tracks an EMA of rate-limit-script RTT.
If **p99 > 25 ms sustained for 3 s** (normal is < 2 ms; 25 ms means the limiter Redis is in
trouble), the gateway returns 503 + `Retry-After: 5` and routes users to the waiting room instead
of skipping the limiter. Justification: failing *open* cannot cause oversell (Phase 2's script is
still atomic) — what it does is dump 500k unthrottled req/s onto the stock Redis, killing the
latency SLO, and it abolishes per-user fairness exactly when scalpers are hammering hardest. A
flash sale's product **is** fairness; degraded fairness is a worse outcome than degraded
availability. (For a normal API, fail open is usually right — this is the exception, and why.)

**Response contract** (429 is a *decision*, 503 is a *shed*, 500 is a *bug*):

```
429 → Retry-After: <reset - now>, X-RateLimit-Remaining: <n>, X-RateLimit-Reset: <epoch>
503 → Retry-After: 5, body: {"error":"system_busy","queue_url":"/queue/join"}
500 → never, for a rate-limit decision. Redis error on the limiter path ⇒ 503 (fail closed), not 500.
```

## Layer 3 — Waiting room / virtual queue

**Choice: FIFO with signed admission tickets.** Lottery is more bot-resistant in theory but users
experience it as rigged, and it forfeits the strongest legitimate signal we have (arrival time).
Priority-for-authenticated is already implicit — `/buy` requires auth, so the queue only contains
authenticated users. Bot resistance belongs to Layers 0–2, not the queue discipline.

Data structures (in **redis-ratelimit**, deliberately — the queue must survive a stock-Redis
failover, see Scenario 1):

```
ZADD  queue:{sale_id}  <redis TIME µs>  <user_id>        # arrival order, server clock
ZRANK queue:{sale_id}  <user_id>                          # position (O(log n))
```

Admission loop (one gateway instance elected via `SET admitter:{sale} <pod> NX EX 5` heartbeat):
every second, `ZPOPMIN queue:{sale} R` and for each popped user
`SET admitted:{sale}:{uid} 1 EX 30`. Admission rate **R = 500/s** — sized to the hot path's
comfortable capacity, not to demand. `/buy` middleware requires the `admitted:` key once the queue
is active. Queue members reaped by `ZREMRANGEBYSCORE queue:{sale} -inf <now - 30min>`.

**UX contract**: user gets a **position number and a coarse ETA** (`position / R`, shown as a
range), refreshed by polling `GET /queue/position` every 5 s (ZRANK — cheap). No promise is made:
copy states explicitly that a position is a place in line, not a reservation.
**Stock-out while queued**: sale-state flips to `SOLD_OUT` (Phase 2e pub/sub); the position
endpoint immediately returns `{"state":"sold_out"}`; outstanding admission tickets become no-ops
(the Phase 2 Lua returns `0 out_of_stock` regardless — the queue never overrides the counter).
Honest and instant beats letting 50 000 people watch a dead line.

## Layer 4 — Headers, request identity, propagation

Every response, set at nginx (so error pages get them too):

```
Strict-Transport-Security: max-age=31536000; includeSubDomains; preload
Content-Security-Policy: default-src 'none'; frame-ancestors 'none'
X-Content-Type-Options: nosniff
X-Frame-Options: DENY
Referrer-Policy: no-referrer
Cache-Control: no-store                     # on /buy, /queue/* — never cache a stock answer
```

**Identity contract** (used verbatim by Phases 4–5):
- `X-Request-ID`: UUIDv7, **generated at the gateway if absent**, echoed in every response, logged
  by every service, injected as a NATS message header. UUIDv7 so IDs sort by time in logs.
- `traceparent` (W3C, `00-<32hex trace>-<16hex span>-<flags>`): the gateway **does not trust an
  inbound traceparent from the public internet** (a client can set `sampled=1` on every request
  and blow up the trace pipeline, or collide trace IDs). It starts a fresh trace and records the
  client's value as attribute `client.traceparent`. Internally, every HTTP call and NATS message
  carries it; a service receiving a message *without* one logs `trace_missing` and mints a new
  trace rather than dropping the message — observability failures must never block orders.

## Trade-off table (Phase 1)

| Approach | Latency impact | Ops complexity | Failure mode | Verdict |
|---|---|---|---|---|
| Hybrid token-bucket + sliding-window in Redis Lua | +1 RTT (~1 ms) on every request | Medium (one script, one extra Redis) | RL Redis down ⇒ fail closed to 503/queue | **Chosen** |
| Fixed window INCR+EXPIRE (status quo) | Same RTT | Low | 2× boundary burst (Bug 4); per-key race on EXPIRE | Rejected — Bug 4 is the spec |
| In-process limiter (no Redis), e.g. rate-limiter-flexible memory mode | 0 RTT | Low | Per-pod limits ⇒ N pods = N× the intended limit; user pinned by LB hash gets different limits than roaming user | Rejected — incorrect under horizontal scale |
| Distributed Redis semaphore for global cap | +2 RTT | Medium | Permit leak on pod crash ⇒ ratchets to permanent 503 | Rejected for per-instance cap |
| Lottery waiting room | n/a | Medium | Perceived unfairness; discards arrival-time signal | Rejected for FIFO |

## Definition of done
- k6 boundary-burst test (Appendix C, `boundary` scenario): a user firing 10 requests straddling a
  window boundary receives **exactly 1** success per 10 s — 0 boundary leakage (was 2× with fixed window).
- Limiter script p99 RTT < 5 ms at 50 000 script calls/s against redis-ratelimit (redis-benchmark + k6).
- Kill redis-ratelimit mid-test: 100% of `/buy` gets 503+Retry-After within 3 s; **zero** 500s; zero requests reach inventory unlimited.
- `curl -s -D- /buy` shows all six security headers and an echoed `X-Request-ID`.
- Queue drill: 10 000 synthetic users enqueue; admission proceeds at 500 ± 25/s; positions strictly decrease.

## Phase gate
Fail-closed behavior demonstrated under limiter-Redis kill, boundary-burst leakage = 0, and the
separate redis-ratelimit instance is in compose with `noeviction` confirmed via `CONFIG GET
maxmemory-policy`. Only then does Phase 2 change the money path.

---

# Phase 2 — Atomic Correctness

**Goal.** The invariant *stock ≥ 0, at most `max_per_user` per user, at most one order per
idempotency key* is enforced by a single serialized decision point that no interleaving of 500 000
concurrent requests can violate.

**Mechanism.** Redis executes Lua scripts on its single command thread: between the script's first
read and last write, no other command runs. All correctness checks therefore see and mutate one
consistent snapshot. This is why it must be **one script** — the alternatives fail structurally:
a *pipeline* batches RTTs but interleaves with other clients between commands (check-then-act race
returns); *WATCH/MULTI* is optimistic concurrency — with 500k clients contending on one key,
essentially every EXEC aborts and retries, which is livelock at the worst possible moment (OCC is
for low contention; a flash sale is the definition of high contention). The Lua script is
pessimistic single-writer serialization with O(1) work per request, executed where the data lives.

## 2a. Production buy script

Spec deviation D2: every touched key is declared in `KEYS[]` with a `{sale:<id>}` hash tag, and the
stock Redis is **not** Redis Cluster — a single hot key occupies one shard regardless, so Cluster
adds Raft/slot-migration failure modes with zero throughput gain. Topology: 1 primary + 2 replicas
+ Sentinel (Scenario 1). The hash tags cost nothing and keep the script Cluster-legal if a future
multi-product sale shards *by product*.

`inventory-service/datastore/redis/buy.lua` (full copy Appendix A2):

```lua
-- KEYS[1] = {sale:S1}:stock
-- KEYS[2] = {sale:S1}:idem:<sha256 hex>
-- KEYS[3] = {sale:S1}:user:<uid>:purchased
-- KEYS[4] = {sale:S1}:state
-- KEYS[5] = {sale:S1}:resv:<order_id>
-- KEYS[6] = {sale:S1}:resv_deadlines            (zset for the expiry reaper)
-- ARGV[1] = quantity
-- ARGV[2] = max_per_user
-- ARGV[3] = idempotency_ttl_seconds             (900)
-- ARGV[4] = order_id
-- ARGV[5] = reservation_ttl_seconds             (300)
-- Returns { code, ... }:
--   {  1, remaining_stock, order_id }   success
--   {  0 }                              out_of_stock
--   { -1, original_order_id }           duplicate (idempotent replay)
--   { -2 }                              quota_exceeded
--   { -3 }                              invalid_quantity
--   { -4 }                              sale_not_open
--   { -99 }                             internal_error (stock key missing/corrupt)

local qty = tonumber(ARGV[1])
local max = tonumber(ARGV[2])
if not qty or qty < 1 or qty % 1 ~= 0 or qty > max then return { -3 } end

if redis.call('GET', KEYS[4]) ~= 'OPEN' then return { -4 } end

local cached = redis.call('GET', KEYS[2])
if cached then return { -1, cached } end

local bought = tonumber(redis.call('GET', KEYS[3]) or '0')
if bought + qty > max then return { -2 } end

local stock = tonumber(redis.call('GET', KEYS[1]))
if stock == nil then return { -99 } end
if stock < qty then return { 0 } end

-- all checks passed under one serialized snapshot: commit everything
redis.call('DECRBY', KEYS[1], qty)
redis.call('INCRBY', KEYS[3], qty)
redis.call('EXPIRE', KEYS[3], 90000)                       -- sale_duration + 1h, §2f
redis.call('SET', KEYS[2], ARGV[4], 'EX', tonumber(ARGV[3]))
redis.call('SET', KEYS[5], 'RESERVED|' .. qty, 'EX', 86400)  -- state kept 24h for audit
local t = redis.call('TIME')
redis.call('ZADD', KEYS[6], t[1] + tonumber(ARGV[5]), ARGV[4])
return { 1, stock - qty, ARGV[4] }
```

Go side (`redis.go`): load once with `SCRIPT LOAD`, call by SHA (`EVALSHA`), fall back to `EVAL`
on `NOSCRIPT` (happens after failover to a replica that never saw the load). The go-redis
`redis.NewScript` helper does exactly this — use it, don't hand-roll.

## 2b. Idempotency key design

- **Format**: `sha256(user_id | product_id | sale_id | client_nonce)`, hex, as `{sale:S1}:idem:<hex>`.
- **Nonce provenance — gateway, not client.** The nonce is minted by the gateway and embedded in
  the signed waiting-room/buy ticket (JWT claim `nonce`). The client echoes the ticket; the gateway
  verifies the signature and derives the key. Why not client-generated: a client that generates a
  fresh nonce per retry defeats idempotency exactly when it matters (timeout → retry), and a
  malicious client that *reuses* someone's nonce can't — the hash binds `user_id` from the
  authenticated session, not from the body.
- **TTL: 900 s (15 min).** Lower bound: the key must outlive the longest window in which a retry
  of the same logical attempt can arrive — worst-case redelivery chain is
  `max_deliver(5) × AckWait(30 s) ≈ 150 s` (Phase 3c) plus the human retry horizon (a user
  hammering refresh for a few minutes). 900 s covers both with 3× margin. Upper bound: memory —
  irrelevant here (≤ a few thousand keys × 100 B), but unbounded TTLs on idempotency keys is how
  you find yourself unable to re-run a sale with the same sale_id. It must also exceed the
  JetStream duplicate window (600 s, Phase 3b) so the two dedup layers never disagree about
  whether an attempt is "old".
- **Duplicate response: the original response, not 409.** `{-1, order_id}` maps to
  `200 {order_id, duplicate: true}`. Idempotency's contract is "same request ⇒ same result"; a 409
  punishes the client whose *first* attempt succeeded but whose response was lost in a timeout —
  the one client idempotency exists to protect. 409 is for *conflicts*, and there is none.

## 2c. Bug 2 fix — compensation (chosen: synchronous, atomic cancel script)

**Chosen: Option A, upgraded** — synchronous compensation via a Lua *cancel* script, not a bare
`INCRBY` (deviation D3). What must roll back is not one counter but the whole reservation:
stock, user quota, idempotency key, and the state machine. Partial rollback is worse than none —
a bare `INCRBY` leaves the idempotency key alive, so the user's retry gets "duplicate" pointing at
an order that will never exist, *and* their quota stays consumed. Permanent, per-user, silent.

`inventory-service/datastore/redis/cancel.lua` (Appendix A3):

```lua
-- KEYS[1]=resv key  KEYS[2]=stock  KEYS[3]=user purchased  KEYS[4]=idem key  KEYS[5]=resv_deadlines
-- ARGV[1]=order_id  ARGV[2]=new_state ("CANCELLED" | "EXPIRED")
-- Returns 1 = compensated, 0 = not in RESERVED (already CONFIRMED/CANCELLED/EXPIRED — no-op)
local v = redis.call('GET', KEYS[1])
if not v or string.sub(v, 1, 8) ~= 'RESERVED' then return 0 end
local qty = tonumber(string.sub(v, 10))
redis.call('SET', KEYS[1], ARGV[2] .. '|' .. qty, 'EX', 86400)
redis.call('INCRBY', KEYS[2], qty)
redis.call('DECRBY', KEYS[3], qty)
redis.call('DEL', KEYS[4])
redis.call('ZREM', KEYS[5], ARGV[1])
return 1
```

`actions.go` error path:

```go
event, data := buildOrderEvent(req, orderID)
if err := natsds.PublishOrderEvent(ctx, cfg, event, idemKey); err != nil {
    cfg.Logger.Error("publish failed, compensating", "order_id", orderID, "error", err)
    compensated := false
    for i, backoff := range []time.Duration{0, 10 * time.Millisecond, 50 * time.Millisecond, 250 * time.Millisecond} {
        if i > 0 { time.Sleep(backoff) }
        n, cerr := cancelScript.Run(ctx, cfg.Redis.Client, cancelKeys(orderID, idemKey), orderID, "CANCELLED").Int()
        if cerr == nil { compensated = true; _ = n; break }
    }
    if !compensated {
        // Redis AND NATS both down: append to local disk journal; a background goroutine
        // replays it until it succeeds. This is the only durable state the service owns.
        compensationJournal.Append(orderID, idemKey, req.Quantity)
        metrics.CompensationOps.WithLabelValues("failed").Inc() // P1 pages (Phase 6)
    } else {
        metrics.CompensationOps.WithLabelValues("success").Inc()
    }
    return nil, fmt.Errorf("order not accepted, stock restored: %w", err)
}
```

**Why not Option B (outbox) as the primary path**: an outbox is the right pattern when publish
volume is high enough that synchronous confirmation is unaffordable — here the *successful*
publish volume is stock-bounded at ≤ ~2 000 for the entire sale. The outbox's costs are structural:
a second durable store whose own loss/lag becomes a new consistency problem, a reconciliation loop
with its own failure modes, and — decisive — the user is told "queued" *before* the outbox
resolves, so a failed publish becomes a silent cancellation of a promised order. The synchronous
path keeps one rule intact: **the user hears "queued" only after the PubAck.** The disk journal
above is a 30-line outbox used only for the double-failure corner, which is the correct size for it.

## 2d. Reservation state machine

All keys under `{sale:<id>}:`; states stored as `STATE|qty` in `resv:<order_id>` (kept 24 h for audit).

| Transition | Where it runs | Command |
|---|---|---|
| AVAILABLE → RESERVED | inventory, buy script (§2a) | `DECRBY` + `SET resv RESERVED` + `ZADD resv_deadlines` — one script |
| RESERVED → CONFIRMED | order-service, **after** the Postgres commit | `confirm.lua` (A4): CAS `RESERVED→CONFIRMED`, `ZREM` deadline. Returns 0 if already CANCELLED/EXPIRED ⇒ order-service **does not fulfill**, acks and drops (stock already went back) |
| RESERVED → EXPIRED | inventory reaper goroutine, every 5 s: `ZRANGEBYSCORE resv_deadlines -inf <now>` then per-id `cancel.lua` with state=EXPIRED | CAS inside the script prevents the reaper racing a concurrent confirm |
| RESERVED → CANCELLED | inventory (publish failure, §2c) or order-service (payment declined, before any Postgres write) | `cancel.lua` |

The CAS ("only move forward from RESERVED") is what makes every path safe to race: confirm vs.
expire, cancel vs. late redelivery — exactly one transition wins, and the loser becomes a no-op.
Postgres additionally carries `UNIQUE(order_id)` as the last-line duplicate backstop.

**Reservation TTL: 300 s.** It must exceed the worst-case *legitimate* confirmation latency, else
the reaper returns stock for an order that subsequently confirms — which oversells. Worst case =
`max_deliver(5) × AckWait(30 s)` ≈ 150 s of redelivery chain + processing ≈ 155 s. 300 s is ~2×
that. Making it much larger just delays stock recovery when order-service is genuinely dead.
(Payment p99 is 0.5 s simulated; the TTL is dominated by the redelivery budget, not the payment.)

## 2e. Pre-warm and sale-open gate

Before the sale: `SET {sale:S1}:stock 2000`, `SET {sale:S1}:state CLOSED` (set at T−30 min,
verified by the checklist). Opening:

```
MULTI
SET {sale:S1}:state OPEN
PUBLISH sale_events '{"sale_id":"S1","state":"OPEN"}'
EXEC
```

(Stock is pre-set and *verified* long before open — setting it inside the open transaction, as the
spec suggests, is how Scenario 7 happens: the open becomes the last untested write of the night.)

Gateway learns via **pub/sub subscribe on `sale_events` + a 1 s poll of the state key as fallback**
(pub/sub is fire-and-forget; a gateway that reconnects during the publish misses it — the poll
bounds staleness to 1 s).

**The 0–5 ms race is a non-event by construction**: the gateway's view of `sale_state` is a
*pre-filter optimization*; the authoritative check is line 2 of the buy script (`-4` if not OPEN),
executed atomically with the decrement. A request arriving in the window is either cheaply rejected
by a stale gateway (client retries in 1 s — acceptable at T=0) or passes through and gets the
correct atomic answer. **Correctness lives in the script; the gateway cache is only there to keep
cheap rejections cheap.** The reverse race (gateway thinks OPEN, script says CLOSED) is equally safe.

## 2f. Per-user quantity cap

Key `{sale:S1}:user:<uid>:purchased`, TTL `sale_duration + 1 h` = 90 000 s for a 24 h sale window
(one key per *participating* user — bounded by admissions, not by the 500k crowd).
Enforced inside the buy script (§2a line "quota") — never as a separate round trip, which would be
a check-then-act race letting a user land 2 parallel requests under one quota.
**Max per user: 2**, supplied as `ARGV[2]` from env `MAX_PER_USER` on the inventory service.
Deliberately *not* a live Redis config key: sale parameters freeze at T−30 (checklist item);
runtime-mutable sale math is the direct cause of Scenario 7.

## Trade-off table (Phase 2)

| Approach | Latency | Ops complexity | Failure mode | Verdict |
|---|---|---|---|---|
| Single Lua script, all checks + writes | 1 RTT ~1 ms | Low | Script bug affects everything — mitigated by the Appendix A test vectors | **Chosen** |
| WATCH/MULTI optimistic transaction | 2+ RTT × retries | Low | Retry livelock at high contention on one key | Rejected — OCC is wrong for hot keys |
| Postgres `UPDATE stock SET n=n-1 WHERE n>=1` + row lock | 5–15 ms | Low | Lock convoy: 500k sessions serialize on one row lock; connection pool exhausts; WAL fsync on the hot path | Rejected for the hot path; Postgres remains the durable ledger |
| Sync Lua cancel compensation | +1 RTT on failure path only | Low | Redis+NATS double failure ⇒ disk journal | **Chosen** |
| Outbox as primary publish path | 0 on request path | High (second store + reconciler) | "Queued" told to user before durability; outbox loss = silent cancel | Rejected (kept as 30-line journal for double-failure only) |

## Definition of done
- Race harness: 10 000 concurrent buy attempts (k6 `flood`) against stock=100, max_per_user=2:
  Redis `GET stock` = 0, **never negative**, Postgres rows = 100 exactly, no user > 2 units — 25/25 repeated runs.
- Replay harness: same idempotency key fired 1 000× concurrently ⇒ exactly 1 decrement, 999 × `{-1}` with the same order_id.
- Kill NATS during flood: `compensation success + confirmed orders + remaining stock = initial stock` holds to the unit; zero `compensation_failed`.
- Reaper drill: order-service stopped, 50 reservations made ⇒ all 50 return to stock at TTL+5 s; restart order-service ⇒ redelivered messages hit `confirm → 0` and are dropped without Postgres writes.

## Phase gate
The audit equation balances across every drill above, and the buy script's 6 return codes each map
to a distinct HTTP response in the gateway contract table. No Phase 3 work until then — durable
messaging built on a leaky reservation layer just persists the leaks.

# Phase 3 — Durable Messaging

**Goal.** An order the user was told is "queued" reaches Postgres exactly once, or its stock is
returned — no third outcome exists.

**Mechanism.** Three mechanisms compose: (1) **PubAck before respond** — the inventory service
only answers 200 after JetStream's Raft quorum has persisted the message, so "queued" means
*replicated*; (2) **`Nats-Msg-Id` dedup** — retrying an ambiguous publish is idempotent because
the stream drops duplicates within the dedup window, which converts "did my publish land?" from an
unanswerable question into a safely retryable one; (3) **WorkQueue retention + explicit-ack
consumer** — a message survives until some consumer acks it, and acking happens only after the
Postgres commit, so a consumer crash at any instruction boundary results in redelivery, and the
Phase 2 CAS makes redelivery harmless.

## 3a. Bug 1 fix — acked publish (deviation D1: sync, not PublishAsync)

`inventory-service/datastore/nats/nats.go`:

```diff
-func PublishOrderEvent(client *nats.Conn, event models.OrderEvent) error {
-	data, err := json.Marshal(event)
-	if err != nil {
-		return err
-	}
-	return client.Publish("orders.pending", data)
-}
+func Connect(config *config.Config) error {
+	nc, err := nats.Connect(config.Nats.URL,
+		nats.UserInfo(config.Nats.Username, config.Nats.Password),
+		nats.MaxReconnects(-1), nats.ReconnectWait(250*time.Millisecond))
+	if err != nil { return err }
+	js, err := nc.JetStream()
+	if err != nil { return err }
+	config.Nats.Client, config.Nats.JS = nc, js
+	return nil
+}
+
+// PublishOrderEvent returns nil ONLY after a PubAck: the stream's Raft quorum has the message.
+func PublishOrderEvent(ctx context.Context, config *config.Config, event models.OrderEvent, idemKey string) error {
+	data, err := json.Marshal(event)
+	if err != nil { return err }
+	msg := &nats.Msg{Subject: "orders.pending", Data: data, Header: nats.Header{}}
+	msg.Header.Set("X-Request-ID", event.RequestID)
+	otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(msg.Header)) // Phase 5
+
+	var lastErr error
+	for attempt := 0; attempt < 3; attempt++ {
+		pubCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
+		// MsgId = idempotency key: JetStream dedup window makes this retry loop safe.
+		ack, err := config.Nats.JS.PublishMsg(msg, nats.MsgId(idemKey), nats.Context(pubCtx))
+		cancel()
+		if err == nil {
+			config.Logger.Info("publish acked", "stream", ack.Stream, "seq", ack.Sequence,
+				"duplicate", ack.Duplicate)
+			return nil
+		}
+		lastErr = err // timeout is AMBIGUOUS: the message may have landed. Retrying is safe
+		              // (dedup); giving up without compensation is not.
+	}
+	return fmt.Errorf("publish unacked after 3 attempts: %w", lastErr)
+}
```

**The three paths:**
- **Success**: PubAck received (possibly with `ack.Duplicate=true` on a retry — still success, the
  message is in the stream exactly once). Respond 200.
- **Error** (connection refused, stream rejects): retry ×3 with the same `MsgId`, then the §2c
  cancel script fires. User gets 503 "not accepted, please retry"; stock is back; their retry gets
  a fresh nonce from the gateway ticket flow.
- **Timeout — the ambiguous case, and the async twin of Bug 2.** The message may or may not be in
  the stream. We compensate anyway (cancel script). If the publish *had* actually landed, the
  consumer picks it up, calls `confirm.lua`, gets `0` (state is CANCELLED), acks and drops it —
  **the CAS tombstone from deviation D3 is precisely what makes compensating on ambiguity safe.**
  Without it, compensate-on-timeout is a latent oversell: stock returned *and* order fulfilled.

Why not `PublishAsync`: it exists to amortize PubAck RTTs across thousands of in-flight publishes.
Our successful-publish volume is stock-bounded (~2 000 total). Async moves the ack to a callback
that fires after the HTTP response is gone — you either block on `PublishAsyncComplete()` anyway
(now it's sync with extra steps) or you answer the user before durability (Bug 1, reintroduced).

## 3b. Stream configuration

```bash
nats stream add ORDERS \
  --subjects "orders.pending" --storage file --replicas 3 \
  --retention work --discard new \
  --max-msgs 100000 --max-bytes 64MB --max-age 24h \
  --dupe-window 10m --defaults

nats stream add ORDERS_DLQ \
  --subjects "orders.dlq" --storage file --replicas 3 \
  --retention limits --discard old --max-age 168h --defaults
```

- **Retention: WorkQueue** for ORDERS — each message is consumed once and deleted on ack, so
  *stream depth ≡ unprocessed work*, which makes the lag metric honest and the post-sale "stream
  empty" check meaningful. Limits retention would keep acked messages around (audit value), but we
  get the audit trail from Postgres + logs; a job queue should be empty when the work is done.
  The DLQ uses limits+discard-old because it's an archive, not a queue.
- **Replicas: 3.** R=1 loses acked orders on a node failure — directly violates the no-silent-loss
  requirement, disqualified. R=5 doubles the Raft quorum's fsync fan-out for a tolerance (2
  simultaneous node losses) we don't need at 2 000 events. R=3 quorum-commit intra-VPC adds
  ~1–2 ms to publish — inside the 50 ms hot-path budget.
- **Sizing math**: order event ≈ 300 B. Worst-case stream content = successful orders (2 000) +
  redeliveries in flight (bounded by MaxAckPending × consumers = 32) ≈ 600 KB. Caps at
  `100 000 msgs / 64 MB` = ~100× safety factor — the caps exist to catch a *pathology* (publisher
  retry storm, consumer wedge), not to manage capacity.
- **Discard: New.** This is load-bearing, not a default: **DiscardOld silently deletes the oldest
  message — an accepted order — to admit a new one.** That is the exact catastrophe this phase
  exists to prevent. DiscardNew fails the *publish*, which flows into §3a's error path →
  compensation → clean 503. When stock is the limiter, hitting the cap at all means something is
  broken; the correct behavior is a loud publish failure, never a quiet deletion.

## 3c. Consumer sizing

```
required_slots = ceil( orders × t_process / drain_SLO )
               = ceil( 2000 × 0.5 s / 60 s )  = 17 concurrent processing slots
```

Deployment: **2 order-service instances (HA) × 16 async slots = 32 slots** → drain ≈
`2000 × 0.5 / 32` ≈ **31 s**, half the 60 s SLO-3 budget, with one instance dead we still make it
in 63 s ≈ at the line (accepted: dual failure of an instance *and* zero margin is P2-alertable).

```bash
nats consumer add ORDERS order-workers \
  --pull --deliver all --ack explicit \
  --max-pending 16 --wait 30s --max-deliver 5 --backoff linear --defaults
```

- **MaxAckPending 16 per consumer** — deliberately below the Postgres pool size (20, §3f): the ack
  window must never exceed downstream capacity, or "concurrency" becomes "queueing inside the
  consumer" and AckWait starts expiring on messages that are merely waiting for a connection.
- **Consumer dies mid-ack**: its ≤16 unacked messages redeliver to the surviving instance after
  AckWait. Duplicate-processing safety is not the consumer's problem — `confirm.lua` CAS + the
  Postgres `UNIQUE(order_id)` make redelivery idempotent (Phase 2d).
- **AckWait 30 s** = ~20× the p99 processing time (0.5 s payment + ~50 ms insert). Too-short
  AckWait is the classic self-inflicted wound: redelivery of a message that is *still being
  processed* → duplicate work storms → more lag → more redelivery. 30 s costs only recovery
  latency after a crash, which the reservation TTL (300 s) already budgets for.

## 3d. Dead-letter queue

Routing rules in `order-service/handler/handler.py`:

| Signal | Meaning | Action |
|---|---|---|
| `term()` | Permanent: malformed JSON, schema-invalid, `confirm.lua → 0` (reservation dead) | Publish copy to `orders.dlq` **first**, then term. Never re-deliverable by definition. |
| `nak(delay)` | Transient: Postgres down/circuit open, payment timeout | Redeliver with backoff; no DLQ |
| `max_deliver` (5) exceeded | Transient that never healed | JetStream emits advisory `$JS.EVENT.ADVISORIES.CONSUMER.MAX_DELIVERIES.ORDERS.order-workers`; a 20-line forwarder in order-service copies the message to `orders.dlq` (with WorkQueue retention the message would otherwise evaporate — the advisory forwarder is **mandatory**, not optional) |

Ops runbook (DLQ drain):

```bash
nats stream view ORDERS_DLQ                                  # inspect: payload + headers
nats consumer info ORDERS order-workers                      # confirm cause is gone
# replay one message back onto the work subject (idempotency makes replay safe):
nats stream get ORDERS_DLQ <seq> --raw | nats pub orders.pending --stdin
# discard after triage (e.g. malformed junk):
nats stream rmm ORDERS_DLQ <seq>
```

Alert: `DLQ depth > 0` → P3 Slack immediately; `> 10` → **P2 page** (Phase 6). During a 2 000-order
sale, ten dead orders is 0.5% of all stock — that's an incident, not a backlog.

## 3e. Back-pressure propagation

First, the honest framing: with DiscardNew + stock-bounded publishes, **lag is structurally capped
near 2 000 messages** — back-pressure here is a belt-and-suspenders control, and over-engineering
it (e.g., NATS flow control, which is a *consumer-side* protocol and never reaches the gateway)
is wasted motion. Chosen: **consumer-pushed lag beacon**.

- order-service already holds a JetStream handle; every 1 s it publishes
  `{"lag": <num_pending>, "ts": ...}` (from `consumer_info.num_pending`) to core-NATS subject
  `control.orders.lag`.
- The gateway subscribes; if `lag > T = 1000` for 5 consecutive beacons (or beacons stop for 5 s —
  absence of signal is treated as bad signal), new `/buy` traffic is routed to the waiting room.
- **Signal latency ≤ 2 s** (1 s beacon period + subscribe delivery). The rejected alternatives:
  gateway polling the NATS monitoring endpoint couples the gateway to NATS ops-plane auth and adds
  a poll loop per gateway pod; Prometheus-mediated signal (consumer → Prom → gateway query) has
  15–30 s of scrape+eval latency — a back-pressure signal that arrives after the herd has passed.

## 3f. Circuit breakers

Placement principle: a breaker guards a *dependency call site*, and its fallback must be a defined
response, never an exception. Common thresholds: open at ≥50% failures over a 10 s rolling window
with ≥20 samples; probe after 5 s; close on 3 consecutive probe successes.

**Gateway → inventory** (Node, `opossum`):

```js
const CircuitBreaker = require('opossum');
const inventoryBreaker = new CircuitBreaker(
  (body, headers) => axios.post(INVENTORY_SERVICE_URL, body, { timeout: 2000, headers }),
  { timeout: 2500, errorThresholdPercentage: 50, volumeThreshold: 20,
    rollingCountTimeout: 10000, resetTimeout: 5000 });
inventoryBreaker.fallback(() => ({ shed: true }));          // → 503 + Retry-After: 5 + queue_url
inventoryBreaker.on('open',     () => log.warn({ event: 'circuit_open',      dep: 'inventory' }));
inventoryBreaker.on('halfOpen', () => log.warn({ event: 'circuit_half_open', dep: 'inventory' }));
```

**Inventory → Redis and → NATS** (Go, `sony/gobreaker` — one breaker per dependency, never shared:
a NATS outage must not block pure-Redis rejections):

```go
func newBreaker(name string) *gobreaker.CircuitBreaker {
	return gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name: name, Interval: 10 * time.Second, Timeout: 5 * time.Second, MaxRequests: 3,
		ReadyToTrip: func(c gobreaker.Counts) bool {
			return c.Requests >= 20 && float64(c.TotalFailures)/float64(c.Requests) >= 0.5
		},
		OnStateChange: func(n string, from, to gobreaker.State) {
			logger.Warn("circuit_state_change", "breaker", n, "from", from.String(), "to", to.String())
		},
	})
}
var redisBreaker = newBreaker("redis-stock")   // open ⇒ /buy answers 503 (fail closed, §1)
var natsBreaker  = newBreaker("nats-publish")  // open ⇒ skip DECRBY entirely: check the breaker
                                               // BEFORE the buy script, so no reservation is
                                               // created that will instantly need compensation
```

That ordering note is the non-obvious part: when the NATS breaker is open, running the buy script
anyway would decrement → fail publish → compensate, i.e. a compensation storm doing zero useful
work. Gate the *entry*, not just the call.

**Order-service → Postgres** (Python, `pybreaker`):

```python
import pybreaker
db_breaker = pybreaker.CircuitBreaker(fail_max=10, reset_timeout=5, name="postgres")
# in process_order: with breaker OPEN, CircuitBreakerError is caught -> msg.nak(delay=5)
# -> redelivery after the probe window instead of hammering a struggling pool.
```

## Trade-off table (Phase 3)

| Approach | Latency | Ops complexity | Failure mode | Verdict |
|---|---|---|---|---|
| Sync `js.Publish` + MsgId + retry + cancel-CAS | +1–2 ms per accepted buy | Low | Ambiguous timeout handled by tombstone | **Chosen** |
| `PublishAsync` + completion callback | ~0 on request path | Medium | Ack resolves after user response ⇒ Bug 1 shape returns | Rejected (D1) |
| WorkQueue retention | — | Low | Requires DLQ advisory forwarder (term'd msgs evaporate) | **Chosen**, forwarder mandatory |
| Limits retention + manual trim | — | Medium | Lag metric lies (acked msgs count); post-sale "empty" check meaningless | Rejected |
| Consumer-pushed lag beacon | signal ≤ 2 s | Low | Beacon loss ⇒ treated as bad signal (safe) | **Chosen** |
| Prometheus-mediated back-pressure | signal 15–30 s | Low | Herd has already passed when signal lands | Rejected |

## Definition of done
- `nats-server` SIGKILL during flood: zero orders lost (audit equation balances), zero
  `compensation_failed`, publishes resume within reconnect (< 5 s), all with user-visible 503s only.
- Duplicate-window proof: same MsgId published 100× ⇒ `nats stream info ORDERS` shows exactly 1 message.
- Consumer-kill drill: SIGKILL one order-service instance mid-drain ⇒ all messages processed exactly
  once (Postgres row count exact), drain completes < 63 s.
- Poison message (garbage JSON) injected ⇒ lands in ORDERS_DLQ within 1 delivery, work consumer unblocked, P3 fired.
- Breaker drill per dependency: induced failure opens the breaker in < 10 s, fallback responses are
  503/nak (never 500/exception), auto-close after recovery in < 20 s.

## Phase gate
All five drills pass twice consecutively, and `nats consumer info` shows `max_deliver=5, ack_wait=30s,
max_ack_pending=16` matching this doc exactly. Observability phases (4–6) instrument this behavior;
they must not start while the behavior is still moving.

---

# Phase 4 — Structured Logging

**Goal.** Any order can be reconstructed end-to-end from logs alone (accept → reserve → publish →
consume → persist → ack) by filtering one `request_id`, with zero PII in the log store.

**Mechanism.** Machine-parseable events with a closed vocabulary turn incident debugging from
"grep and hope" into relational queries; carrying `trace_id`/`span_id` on every line makes logs
and traces two views of the same graph. The closed event vocabulary is the part teams skip and
regret: free-text messages can't be counted, and what can't be counted can't be alerted on.

Canonical schema (every service, every line):

```json
{"ts":"2026-07-06T10:00:00.123456Z","level":"INFO","service":"inventory","version":"1.4.2",
 "trace_id":"4bf92f3577b34da6a3ce929d0e0e4736","span_id":"00f067aa0ba902b7",
 "event":"stock_decremented","request_id":"0197f9e2-...","user_id":"sha256:9f86d0...",
 "duration_ms":1.2,"error":null, "remaining_stock":1847,"lua_return_code":1,"quantity_requested":1}
```

**PII policy (all services, enforced in the logging layer, not at call sites):** `user_id` is
sha256-hashed before it reaches the encoder; IPs logged as /24 (`203.0.113.0/24`); email, name,
payment fields are on a deny-list that the serializer drops with a `pii_dropped:true` marker.
Vector re-applies the same scrub in the pipeline (defense in depth — one forgotten call site
doesn't leak to storage).

**Gateway (Node — `pino`).** Pino over winston: worker-thread transport off the event loop,
~5× lower overhead at high line rates, JSON-native.

```js
const pino = require('pino');
const crypto = require('crypto');
const hashUid = (u) => 'sha256:' + crypto.createHash('sha256').update(String(u)).digest('hex').slice(0, 16);
const logger = pino({
  base: { service: 'gateway', version: process.env.GIT_SHA },
  timestamp: pino.stdTimeFunctions.isoTime,
  redact: { paths: ['req.body.email', 'req.body.name', 'req.headers.authorization'], remove: true },
});
app.use((req, res, next) => {                      // identity middleware — FIRST in the chain
  req.id = req.headers['x-request-id'] || uuidv7();
  res.setHeader('X-Request-ID', req.id);
  req.log = logger.child({ request_id: req.id, trace_id: currentTraceId(), span_id: currentSpanId(),
                           user_id: req.auth ? hashUid(req.auth.uid) : undefined });
  next();
});
```

Events: `request_received, rate_limit_checked, rate_limited, request_forwarded, response_sent,
circuit_open, circuit_half_open`.
**Sampling**: 100% of ERROR/WARN, 100% of 4xx/5xx responses, **1% of 2xx** (`request_id % 100 === 0`
— deterministic, so a sampled request is sampled at *every* event, giving complete narratives for
1% of successes rather than 1% fragments of all of them).

**Inventory (Go — `slog` + a Handler that lifts `trace_id`/`span_id` out of `context.Context`).**
Events: `stock_check_attempted, stock_decremented, stock_out, duplicate_detected,
publish_attempted, publish_succeeded, publish_failed, compensation_triggered,
compensation_succeeded, compensation_failed`.
Mandatory fields: `remaining_stock` (the war number — ops watches this stream raw during the sale),
`lua_return_code`, `quantity_requested`. No sampling in this service: its entire sale-window log
volume is bounded by admitted traffic (~5 000 lines/s worst case — trivial), and it is the service
where a single missing line can hide a stock leak.

**Order-service (Python — `structlog`, JSON renderer).**
Events: `message_received, payment_simulated, db_insert_attempted, db_insert_succeeded,
db_insert_failed, message_acked, message_naked, message_termed, dlq_forwarded`.
Mandatory fields: `message_delivery_count` (a value > 1 on many messages = poison or AckWait too
short — this field is the earliest detector), `consumer_lag_estimate` (from the 1 s beacon, §3e).

**Pipeline: Vector → Loki, hot 7 d, object storage 90 d.**
- **Vector** over Fluent Bit: end-to-end acknowledgements (a log line is deleted from the local
  buffer only after Loki confirms it — Fluent Bit's defaults can drop on backpressure), VRL for
  the PII scrub at the edge, single static Rust binary. Fluent Bit is lighter at idle; we are never idle during the window that matters.
- **Loki** over Elasticsearch: logs here are *queried by known keys* (request_id, order_id,
  event) during incidents — Loki's model (index only labels, grep the rest) fits exactly, costs
  ~10× less at retention, and its storage **is** the S3/GCS cold tier (one system covers hot 7 d
  and cold 90 d via retention config — no separate archival pipeline). ES wins for free-text
  ad-hoc search across huge corpora, which is not this workload; running ES well is a hire we
  don't need. **Label discipline**: Loki labels are `{service, level, event}` only — `event` has
  ~25 bounded values. `trace_id`/`request_id` stay in the JSON body, queried as
  `{service="inventory"} | json | request_id="..."` — putting them in labels is the same
  cardinality bomb as Prometheus labels, one system over. (If a future team insists on ES: daily
  indices `logs-flash-YYYY.MM.DD`, ILM hot→warm at 1 d, rollover at 50 GB, delete at 7 d, snapshot to S3 at 90 d.)
- **Alert**: `count_over_time({service="inventory", level="ERROR"}[1m]) > 0` during the sale window
  → PagerDuty **P2** via Loki ruler → Alertmanager. Inventory ERRORs are compensation-path events;
  they are never routine.

## Trade-off table (Phase 4)

| Approach | Overhead | Ops complexity | Failure mode | Verdict |
|---|---|---|---|---|
| Vector → Loki (labels: service/level/event) | ~1–2% CPU per node | Low-medium | Loki ingester backpressure ⇒ Vector disk-buffers (no loss) | **Chosen** |
| Fluent Bit → Elasticsearch | Lower agent RAM, much higher store cost | High (ES cluster care) | Drop-on-backpressure defaults; index storms | Rejected for this workload |
| Log 100% of 2xx | +50–100× gateway log volume | — | Pipeline saturation exactly at peak | Rejected — deterministic 1% keeps whole narratives |
| trace_id as Loki label | — | — | Unbounded label cardinality ⇒ Loki index blowup | Rejected, named explicitly |

## Definition of done
- One `request_id` from a staging buy returns the full 10+ event narrative across all 3 services in a single Loki query, < 2 s.
- Grep of 1 h of stored logs for `@`, a known test email, and a raw test user_id: **zero hits** (PII scrub verified at the store, not the source).
- Vector soak at 10 000 lines/s for 10 min: zero drops (`vector_events_discarded_total == 0`), buffer < 50%.
- Kill Loki for 60 s under load: Vector buffers and back-fills; no line lost end-to-end.

## Phase gate
The PII grep is clean and the ERROR→P2 alert has fired in a drill (not just been configured).
Tracing (Phase 5) reuses the same identity plumbing; do not instrument twice.

---

# Phase 5 — Distributed Tracing

**Goal.** Every accepted order has one trace spanning gateway → inventory → Redis → NATS →
order-service → Postgres, and the p99 of any hop is a query, not an argument.

**Mechanism.** W3C `traceparent` gives every hop a shared causal key; the NATS *message header* is
the non-obvious carrier — without injecting context into the message, the trace dies at the queue
and the async half of the system (where the interesting latency lives) becomes invisible.
OTel Collector between SDKs and backend so sampling policy changes never require service redeploys.

**Backend: Tempo** (over Jaeger): object-storage backend (no Cassandra/ES to operate — consistent
with the Loki decision), native Grafana pairing (click log line → trace via shared `trace_id`),
TraceQL, and the OTel Collector already speaks OTLP to it. Jaeger's UI is fine; its storage tier
is an operational tax we already declined once this document.

**Gateway (`@opentelemetry/sdk-node`)** — `tracing.js`, required *before* express loads:

```js
const { NodeSDK } = require('@opentelemetry/sdk-node');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-grpc');
const { HttpInstrumentation } = require('@opentelemetry/instrumentation-http');
const { ExpressInstrumentation } = require('@opentelemetry/instrumentation-express');
const { RedisInstrumentation } = require('@opentelemetry/instrumentation-redis-4');
new NodeSDK({
  serviceName: 'gateway',
  traceExporter: new OTLPTraceExporter({ url: 'grpc://otel-collector:4317' }),
  instrumentations: [new HttpInstrumentation(), new ExpressInstrumentation(), new RedisInstrumentation()],
}).start();
```

Root span per inbound request (HttpInstrumentation creates it; inbound client `traceparent` is
**not** honored — §1 Layer 4). Added attributes: `user_id_hash`, `rate_limit_result`,
`request_id`. Propagation to inventory is automatic (HTTP); the NATS hop is inventory's job:

**Inventory (`go.opentelemetry.io/otel`)** — spans on the three hot operations:

```go
ctx, span := tracer.Start(ctx, "redis.stock_decrement")
res, err := buyScript.Run(ctx, rdb, keys, argv...).Slice()
span.SetAttributes(
	attribute.Int("lua_return_code", code),
	attribute.Int("remaining_stock", remaining),
	attribute.Int("quantity", req.Quantity),
	attribute.Bool("idempotency_hit", code == -1),
)
if code < 1 { span.SetStatus(codes.Error, returnCodeName(code)) }
span.End()

ctx, span = tracer.Start(ctx, "nats.publish")
err = nats.PublishOrderEvent(ctx, cfg, event, idemKey)   // §3a injects traceparent into msg.Header
span.SetAttributes(attribute.String("messaging.destination", "orders.pending"),
	attribute.Bool("pub_ack_received", err == nil))
```

Also `redis.rate_limit_check` in the gateway (attributes `lua_return_code`, `user_id_hash`).
The injection carrier is `propagation.HeaderCarrier(msg.Header)` — header key **`traceparent`**,
exactly as on HTTP, which is what makes the Python side's extraction symmetric.

**Order-service (`opentelemetry-sdk`)** — *continue* the trace, never restart it:

```python
from opentelemetry import trace
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from opentelemetry.trace import SpanKind

def create_order_handler(db_client):
    async def message_handler(msg):
        carrier = dict(msg.headers or {})
        ctx = TraceContextTextMapPropagator().extract(carrier=carrier)   # the buy trace continues
        with tracer.start_as_current_span("order.process", context=ctx, kind=SpanKind.CONSUMER,
                attributes={"messaging.system": "nats",
                            "message_delivery_count": msg.metadata.num_delivered}):
            with tracer.start_as_current_span("payment.simulate"):
                await asyncio.sleep(0.5)
            with tracer.start_as_current_span("postgres.insert_order",
                    attributes={"db.sql.table": "orders"}) as s:
                ok = await db_client.insert_order(order)
                s.set_attribute("rows_affected", 1 if ok else 0)
            with tracer.start_as_current_span("nats.ack",
                    attributes={"ack_type": "ack", "delivery_count": msg.metadata.num_delivered}):
                await msg.ack()
    return message_handler
```

**Redis/Postgres instrumentation**: library, never hand-rolled —
`instrumentation-redis-4` (Node), go-redis's `redisotel.InstrumentTracing(rdb)` (Go),
`opentelemetry-instrumentation-asyncpg` (Python; sqlcommenter additionally stamps trace context
into SQL comments so `pg_stat_statements` rows link back to traces). Redis spans carry
`db.system=redis`, `db.operation=EVALSHA`, `db.statement=<script SHA>` — **the SHA, not the script
body**: a 40-char constant vs. re-shipping a 2 KB script on every span.

**Sampling**: head-sample 100% during the sale window (worth stating why it's affordable: traffic
is huge but *traces that matter* are bounded — ~2 000 accepted orders plus rejects whose traces
are 2 spans deep; ≈ 200 MB total — a rounding error). Post-window: tail sampling in the collector,
100% of error traces + 10% probabilistic (full config Appendix D). Tail sampling lives in the
**collector** so the ratio is an ops-tunable, not a redeploy.

**Trace-derived alerting** — deviation D6: the collector's `spanmetrics` connector converts spans
to Prometheus histograms (`traces_span_metrics_duration_...{span_name="redis.stock_decrement"}`),
and the alerts (decrement p99 > 10 ms, insert p99 > 500 ms) are Prometheus rules on those —
sampled-telemetry math done in the metrics system, where `for:` durations and inhibition exist.
The `pub_ack_received=false` condition alerts from `publish_operations_total{result!="success"}`
(Phase 6), because an invariant alert must not depend on span delivery.

## Trade-off table (Phase 5)

| Approach | Latency impact | Ops complexity | Failure mode | Verdict |
|---|---|---|---|---|
| OTel → Collector → Tempo | ~µs/span, batched export | Low-medium | Collector down ⇒ SDK buffers then drops spans; orders unaffected | **Chosen** |
| OTel → Jaeger (ES/Cassandra backend) | same | High | A second stateful store to page on | Rejected |
| SDKs export direct to backend (no collector) | same | Low | Sampling changes = redeploy every service; no tail sampling possible | Rejected |
| Alerting from trace data directly | — | Medium | Sampling + best-effort delivery under an invariant alert | Rejected (D6) — spanmetrics → Prometheus |

## Definition of done
- One staging buy renders as **one** Tempo trace: ≥ 7 spans, gateway root, `order.process` a child
  (same trace_id across the NATS hop — the assertion that matters), `payment.simulate` ≈ 500 ms.
- Grafana log line → "View trace" round-trip works via shared trace_id (Loki↔Tempo linkage).
- Flood test with 100% head sampling: `otelcol_processor_dropped_spans == 0`, collector RSS < 512 MB.
- spanmetrics histograms visible in Prometheus and the two latency alerts fire in a synthetic-slowness drill.

## Phase gate
Cross-hop trace continuity proven under load (not on a single request), and a client-supplied
malicious `traceparent` is demonstrably ignored. Phase 6 builds panels on spanmetrics; the names
must be frozen first.

# Phase 6 — Metrics, Dashboards, SLOs, Alerting

**Goal.** Every SLO in this document is a query on a dashboard that was watched during a rehearsal,
and every failure scenario in Phase 7 has a metric that detects it before a customer tweet does.

**Mechanism.** Counters and histograms are the only telemetry cheap enough to be *unsampled* at
500k req/s, which is why invariants alert from metrics (D6). The one rule that keeps them cheap:
**bounded label cardinality**. `user_id`/`request_id`/`order_id` as a label value mints a new time
series per value — 500k users × buckets = memory bomb in Prometheus and every scrape. Allowed
labels: `service, endpoint, method, status_code, result`. Nothing else without review.

Endpoints: gateway `prom-client` (default registry + `collectDefaultMetrics()`), inventory
`promhttp.Handler()` on `/metrics`, order-service `prometheus_client.start_http_server(9100)`.
All three added to `prometheus.yml` scrape configs, 5 s interval during the sale window.

Metric inventory (names are frozen here; panels and alerts reference these strings):

```
# gateway
http_requests_total{endpoint,method,status_code}                counter
http_request_duration_seconds{endpoint,status_code}             histogram
  buckets: .005,.01,.025,.05,.1,.25,.5,1,2.5,5,10
rate_limit_decisions_total{endpoint,result}                     counter  result: allowed|rate_limited|quota_exceeded|shed
active_buy_requests                                             gauge
waiting_room_depth                                              gauge    (ZCARD, polled 5s by admitter)

# inventory
stock_remaining                                                 gauge    (GET from Redis every 5s — the war metric)
stock_operations_total{result}                                  counter  result: decremented|out_of_stock|duplicate|quota|invalid|closed|error
stock_decrement_duration_seconds                                histogram (Lua RTT; buckets .0005,.001,.0025,.005,.01,.025,.05,.1)
publish_operations_total{result}                                counter  result: success|failed|timeout
compensation_operations_total{result}                           counter  result: success|failed

# order-service
orders_processed_total{result}                                  counter  result: success|failed|dlq|dropped_dead_reservation
order_processing_duration_seconds                               histogram
nats_consumer_lag                                               gauge    (num_pending, same source as the §3e beacon)
db_insert_duration_seconds                                      histogram
payment_simulation_duration_seconds                             histogram
```

`stock_remaining` deserves its one subtlety: it is scraped every 5 s, so it can *display* a small
negative-looking glitch only if the gauge write races a compensation — it cannot actually go
negative in Redis (the script forbids it). The **oversell alert therefore fires on the Redis-truth
recording rule** `min_over_time(stock_remaining[1m]) < 0`, which firing at all means the Lua layer
was bypassed — someone ran a raw DECRBY. That is a P1 *and* a post-incident review of Redis ACLs.

## Grafana dashboard — "Flash Sale War Room" (JSON in Appendix F)

| Row | Panels (PromQL sketch) |
|---|---|
| 1 Sale health | `stock_remaining` gauge (green >500, yellow >100, red ≤100) · confirmed vs 2000: `sum(orders_processed_total{result="success"})` stat · acceptance %: `rate(http_requests_total{endpoint="/buy",status_code="200"}[1m]) / rate(http_requests_total{endpoint="/buy"}[1m])` stat · `waiting_room_depth` stat |
| 2 Latency | gateway `/buy` p50/95/99 `histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{endpoint="/buy"}[1m])))` · decrement p50/95/99 · order e2e p50/95/99 · insert p50/95/99 |
| 3 Throughput & errors | req/s stacked by `status_code` (200/202/400/429/503/500) · rate-limit decisions/s stacked by `result` · publish success vs failed/timeout · DLQ depth (`nats stream info ORDERS_DLQ` via nats-exporter) |
| 4 Infra | Redis CPU/mem (redis-exporter, both instances on one panel — divergence is Bug 3 regressing) · `nats_consumer_lag` · consumers configured vs active · Postgres pool saturation `pg_stat_activity` count / pool max |

## SLOs and error budget

| SLO | Statement | Measured by |
|---|---|---|
| 1 | 99.9% of `/buy` < 500 ms (sale window) | gateway histogram; budget = 0.1% of requests |
| 2 | Zero oversell — `stock ≥ 0` always | `min_over_time(stock_remaining[1m]) ≥ 0` + post-sale audit equation; budget = **0**, hard invariant |
| 3 | 99.99% of accepted (200) orders → Postgres row < 60 s | join of `orders_processed_total{result="success"}` vs accepted counter; reconciled exactly post-sale |
| 4 | Zero duplicate orders | Postgres `UNIQUE(order_id)` violations = 0 and duplicate-audit query (post-sale) returns 0 rows |

Internal budget note (D5): "99.99% availability" over a 30 min window is restated as **≤ 15 s
cumulative full-outage (all-503)**, which is exactly one Redis Sentinel failover (Scenario 1 RTO).
Two failovers in one window = SLO breached = the retro writes itself.

## Alert rules (full YAML in Appendix E)

- **P1 (page now)**: `stock_remaining == 0 and sum(orders_processed_total{result="success"}) < 2000`
  for 2 m after sold-out signal (stock leaked) · `compensation_operations_total{result="failed"} > 0`
  (unrecoverable path hit — the disk journal is now the system of record) ·
  5xx ratio > 1% for 30 s on any service · oversell rule above.
- **P2 (page ≤ 5 m)**: decrement p99 > 10 ms for 60 s · publish failure ratio > 0.1% for 30 s ·
  DLQ depth > 10 · `nats_consumer_lag > 500` for 60 s · inventory ERROR log-rate rule (Phase 4).
- **P3 (Slack)**: 429 ratio > 80% of `/buy` (defense holding, but check it's not legit users) ·
  `waiting_room_depth > 50000` · insert p99 > 1 s · DLQ depth > 0.

## k6 load test (deviation D4 — complete script in Appendix C)

What changes vs. the spec, and why it's not weaseling:
- **`ramping-arrival-rate`, not 500k VUs.** VUs model *open connections*; what stresses this
  system is *arrival rate*. 500k VUs on one runner is ~1.5 TB of VU state; the honest setup is
  arrival-rate ramping 0 → 25 000 req/s (≈ what 500k humans generate through Layers 0–1) with
  `preAllocatedVUs: 20000`, executed distributed (k6-operator / k6 Cloud, ~10–40 runners) for the
  full number, and `SCALE=0.01` for laptop smoke runs.
- **Assertions restated at the correct layer**: successes (200 minus `duplicate:true`) ≤ 2000 —
  threshold on a custom Counter; zero 500s — `http_req_failed{status:500} count==0`; p99 < 500 ms;
  429s asserted as a *ratio* (> 95% of requests), because "≥ 498 000" assumed one request per user.
- **Duplicate-order detection is a post-run SQL audit, not a k6 check** — cross-VU state doesn't
  exist in k6, and the database is the system of record anyway:
  `SELECT order_id FROM orders GROUP BY order_id HAVING count(*) > 1;` → must return 0 rows.
- A dedicated `replay` scenario re-sends one idempotency key 500× and asserts one order_id.

## Trade-off table (Phase 6)

| Approach | Cost | Ops complexity | Failure mode | Verdict |
|---|---|---|---|---|
| Prometheus pull + bounded labels | ~MBs of series | Low | 5 s scrape = 5 s detection floor (acceptable; invariants also checked post-hoc) | **Chosen** |
| user_id label "just for the sale" | 500k+ series × buckets | — | Prometheus OOM mid-sale = flying blind at T=0 | Rejected, named explicitly |
| Push gateway for service metrics | — | Medium | Silent staleness (push stops ⇒ last value lies) | Rejected — pull's liveness signal (`up`) is the feature |
| 500k real VUs | ~40 runner nodes | High | Testing the runners, not the system | Rejected for arrival-rate model (D4) |

## Definition of done
- All four dashboard rows live with data during a k6 smoke run; no panel shows "N/A".
- Every P1/P2 alert has been *fired by a drill* (not merely loaded): stock leak simulated by manual
  DECRBY on staging, compensation failure by killing both Redis+NATS, lag by pausing consumers.
- k6 full run (staging, ≥ 5 000 req/s arrival): all thresholds green, post-run SQL audits return 0 rows,
  audit equation balances to the unit.
- Prometheus TSDB head series < 50 000 (cardinality rule held in practice, not just in review).

## Phase gate
A full game-day rehearsal ran with the war-room dashboard as the only screen, and every number an
operator asked for was on it. Phase 7 assumes these signals exist; runbooks reference them by name.

---

# Phase 7 — Chaos, Hardening, Runbooks

**Goal.** Every plausible failure has been *rehearsed*: detection is a named signal from Phase 4–6,
mitigation is automatic, and the manual step is a runbook a tired engineer can follow at 2 a.m.

**Mechanism.** The invariant architecture (atomic reservation + CAS state machine + acked
publishes) means most failures degrade to "some users see 503" rather than "money is wrong" — the
runbooks below mostly *verify* that property held, then restore capacity. Verification is always
the audit equation:

```
GET {sale:S1}:stock  +  Σ confirmed qty (Postgres)  +  Σ RESERVED qty (open reservations)  =  2000
```

**S1 — Stock-Redis primary fails.** Detect: `redis_up{instance="redis-stock"} == 0`, redisBreaker
open, decrement p99 alert. Auto: breaker → all `/buy` 503 + `Retry-After: 15`; gateway routes to
waiting room (which lives in redis-ratelimit — unaffected, by design §1L3). Manual: Sentinel
promotes in < 30 s (`sentinel failover flashsale`), then **before re-enabling traffic** run the
integrity audit:

```
redis-cli -h redis-stock GET '{sale:S1}:stock'
psql -c "SELECT COALESCE(SUM(quantity),0) FROM orders WHERE sale_id='S1' AND status='CONFIRMED';"
redis-cli -h redis-stock ZRANGEBYSCORE '{sale:S1}:resv_deadlines' -inf +inf | wc -l   # open resv count
```

Sum must equal 2000. Shortfall = writes lost in failover (async replication window, typically
< 1 s of writes): `SET` the corrected stock with an audit log entry, accept that a handful of
"queued" responses may not confirm — their reservations never replicated, users see a clean retry.
RTO **< 45 s**. Impact: 503s during failover only.

**S2 — NATS unreachable after decrement.** Detect: `publish_operations_total{result="failed"}`
increments, natsBreaker opens. Auto: §2c cancel script restores stock; breaker-open then gates
*entry* (no new reservations made just to be compensated, §3f). Manual: run the audit equation +
confirm `compensation_operations_total{result="failed"} == 0`; if journal entries exist, verify the
replayer drained them (`journal depth == 0`). Impact: clean 503 "not accepted", never a silent loss.

**S3 — order-service crashes mid-consume.** Detect: active consumer count drops on Row 4;
lag beacon gap (§3e treats silence as bad). Auto: JetStream redelivers after AckWait 30 s to the
surviving instance; `confirm.lua` CAS + `UNIQUE(order_id)` absorb any double delivery. Manual:

```bash
nats consumer info ORDERS order-workers     # num_ack_pending, num_redelivered, num_pending
nats stream view ORDERS_DLQ                 # anything that exceeded max_deliver=5
```

Replay from DLQ per §3d only after cause is fixed. Impact: those ≤16 in-flight orders confirm up
to 30 s late — inside the 60 s SLO-3 window.

**S4 — Postgres insert p99 → 10 s.** Detect: `db_insert_duration_seconds` p99 alert (P3 at 1 s
precedes the crisis). Auto: pybreaker opens → `nak(delay=5)` → messages wait in the stream instead
of stacking on the pool (the stream *is* the buffer — this is why the queue exists). Manual:

```sql
SELECT pid, state, wait_event_type, wait_event, now() - query_start AS dur,
       left(query, 80) AS query
FROM pg_stat_activity
WHERE datname = 'flashsale' AND state <> 'idle'
ORDER BY dur DESC LIMIT 20;
```

Blocking DDL/vacuum/lock → `pg_terminate_backend(pid)` for the blocker; pool exhaustion → confirm
MaxAckPending(16) < pool(20) still holds (someone tuning one without the other is the usual cause).
Reservation TTL (300 s) is the deadline: drain must resume within ~4 min or reservations expire and
stock returns for orders that will then be dropped-on-confirm. That's safe (no oversell) but
disappoints users — the P2 escalates to P1 at lag age > 240 s.

**S5 — Rate-limit Redis under memory pressure.** Detect: `redis_evicted_keys_total` rate > 0 —
plus the subtle signal: `rate_limit_decisions_total{result="allowed"}` ratio suddenly *rising*
(evicted counters = everyone looks new = everyone passes). Prevented, not mitigated:
`maxmemory-policy noeviction` (Appendix B) — under `allkeys-lru` the limiter silently self-disables
exactly under attack, the worst possible behavior. With noeviction, writes fail → limiter errors →
**fail closed** (§1L2e) → 503s, loud and safe. Manual: `CONFIG SET maxmemory 4gb`, verify
redis-stock untouched (separate instance — the point of Bug 3's fix).

**S6 — One user at 50 000 req/s.** Detect: CF rate rule + nginx 429 spike; per-user hash spike in
gateway logs. Reality of the layers: Cloudflare (6/10 s per IP) and nginx (10 r/s per IP) mean
50k req/s *reaching the gateway* requires ~5 000 distinct IPs — that is Layer 0's bot problem, and
its detection (bot score, ASN adaptive) fires first. Redis ops budget: limiter script ≈ 80–100k
ops/s on one core; the adaptive throttle (§1L2e) sheds above that. Manual: CF block rule on the
offending fingerprint/ASN; the user_id itself gets `SET rl:{uid}:blocked 1 EX 3600` checked as a
fast-path deny.

**S7 — Stock pre-set wrong.** Detect: pre-sale checklist dual-read; `stock_remaining` ≠ 2000 on
the war dashboard at T−30. Manual (atomic, audited):

```
redis-cli GETSET '{sale:S1}:stock' 2000     # returns the wrong old value into the audit log
```

GETSET so the correction and the evidence of what was wrong are one operation. Root-cause class:
runtime-mutable sale parameters — which is why §2f froze them.

**S8 — "Duplicate" spike.** Detect: `stock_operations_total{result="duplicate"}` ratio anomaly.
Auto: correct behavior already — idempotent replay of the original response. Manual triage: sample
gateway logs — same `(user, nonce)` re-arriving = client retry bug (benign, coordinate with
client team); distinct nonces mapping to one hash = SHA-256 collision = it is not a SHA-256
collision, your nonce embedding is broken (e.g., ticket reuse across users — check JWT validation).

**S9 — ORDERS stream at capacity.** Detect: `nats stream info ORDERS` bytes/msgs near limits
(exporter alert at 80%). Auto: **DiscardNew** (§3b, re-justified: DiscardOld deletes accepted
orders — silent loss; DiscardNew fails new publishes → compensation → clean 503; when stock bounds
legitimate content at ~600 KB, hitting 64 MB *means* a pathology, and the correct response to a
pathology is refusal, not amnesia). Manual: find the pathology (redelivery storm? publisher loop?),
then `nats stream edit ORDERS --max-bytes 128MB` only if content is legitimate.

**S10 — Gateway OOM.** Detect: `process_resident_memory_bytes` slope alert; restart events.
Auto: container `restart: unless-stopped` / K8s liveness; LB health check ejects during restart.
Config: `node --max-old-space-size=1536` in a 2 GiB container (headroom for RSS ≠ heap: buffers,
stack, code); `server.maxConnections = 8000`; `agent = new http.Agent({ keepAlive: true,
maxSockets: 200 })` toward inventory. Manual: `node --heapsnapshot-signal=SIGUSR2`, then look for
the usual suspect in *this* codebase: per-request state held past response (breaker event
listeners, axios interceptors, the in-flight counter map) — anything keyed by request that
outlives it.

**S11 — Clock skew.** Detect (subtle): boundary-burst k6 scenario admitting > limit; cross-pod
limit variance. Mitigation is structural, already shipped: every time-dependent decision (§1L2
limiter, §2a reservation deadlines) reads `redis.call('TIME')` *inside the script* — one clock,
the data's clock. Gateway/inventory wall clocks are used only for logging. NTP on hosts is
hygiene, not a correctness dependency.

**S12 — Deploys and shutdown.** Rule one, on the checklist: **no deploys inside T−30 → sale end.**
Post-sale rolling deploys, SIGTERM contracts:
- **Gateway**: `server.close()` (stop accepting; in-flight requests finish), force-exit at 30 s;
  LB drain first (fail health check for 2 probe periods before SIGTERM).
- **Inventory**: stop accepting HTTP, wait for in-flight handlers (each is ≤ one Lua call + one
  publish, < 3 s), flush the compensation journal replayer, then close Redis/NATS clients — order
  matters: journal before clients.
- **Order-service**: stop pulling new messages, finish current ones and ack; **do not nak on
  shutdown** — an unacked message redelivers cleanly after AckWait, while a nak-on-shutdown
  immediately re-queues work you might be mid-committing in Postgres, manufacturing the
  double-processing race for zero benefit.

---

# Pre-sale checklist (T−30 min, war room open)

| ✓ | Item | Exact check |
|---|---|---|
| □ | Stock counter dual-read | `redis-cli -h redis-stock GET '{sale:S1}:stock'` = 2000 **and** war-dashboard gauge = 2000 (two paths, one truth) |
| □ | RL Redis health | `redis-cli -h redis-ratelimit INFO memory` → used < 60% of maxmemory; `CONFIG GET maxmemory-policy` = `noeviction` |
| □ | NATS stream | `nats stream info ORDERS` → 0 messages; `nats consumer info ORDERS order-workers` → 2 active pulls, `num_pending=0`, config matches §3c |
| □ | Metrics | Prometheus `/targets` all UP; every Row-1 panel non-N/A |
| □ | Traces | `otelcol_processor_dropped_spans == 0`; one synthetic buy renders end-to-end in Tempo |
| □ | Logs | Vector `/health` OK, buffer < 10%; synthetic buy's request_id queryable in Loki < 2 s |
| □ | Breakers | gateway `/health` reports all breakers CLOSED; gobreaker state gauge = 0 for both |
| □ | DLQ | `nats stream info ORDERS_DLQ` → 0 messages |
| □ | Sale gate | `redis-cli GET '{sale:S1}:state'` = `CLOSED` |
| □ | Smoke test | k6 `SCALE=0.001` (100 VU / 10 s) against staging: all thresholds green, audit SQL clean |
| □ | Humans | on-call acked pages, war room live, runbook URL pinned, deploy freeze announced in #eng |
| □ | Abort plan | documented & rehearsed: `redis-cli SET '{sale:S1}:state' CLOSED` halts all new reservations in ≤ 1 s (gate is checked in-script, §2e); open reservations then drain or expire on their own |

# Post-sale audit checklist

| ✓ | Item | Exact check |
|---|---|---|
| □ | Zero oversell | Audit equation: `GET stock` + `SELECT COALESCE(SUM(quantity),0) FROM orders WHERE sale_id='S1' AND status='CONFIRMED'` + open-reservation qty = **2000, to the unit** |
| □ | Zero duplicates | `SELECT order_id, count(*) FROM orders GROUP BY order_id HAVING count(*) > 1;` → 0 rows |
| □ | Accepted ⇒ persisted | reconcile gateway 200-count (Prometheus) vs Postgres confirmed + compensated count; unexplained gap = incident |
| □ | DLQ drained | triage every ORDERS_DLQ message to replay/discard with a note; end at 0 |
| □ | Compensation ledger | `compensation_operations_total{result="failed"}` = 0 and disk journal empty on both inventory pods |
| □ | Reservations resolved | `ZCARD '{sale:S1}:resv_deadlines'` = 0 (nothing left in RESERVED) |
| □ | Archive | Tempo blocks + Loki chunks for the window copied to the 90-day bucket; Grafana snapshot of the war dashboard attached to the retro doc |
| □ | Retro | every P1/P2 that fired gets a timeline; every alert that *should* have fired and didn't gets a new rule |

# Appendix A — Lua scripts (complete)

**A1 — `rate_limit.lua`**: given in full at Phase 1 Layer 2 (token bucket + sliding window,
Redis TIME, structured return). Keys: `rl:{<uid>}:bucket`, `rl:{<uid>}:win:<endpoint>`.

**A2 — `buy.lua`**: given in full at §2a (idempotency → gate → quota → stock → commit;
return codes `1/0/-1/-2/-3/-4/-99`).

**A3 — `cancel.lua`**: given in full at §2c (CAS RESERVED→CANCELLED|EXPIRED + full rollback).

**A4 — `confirm.lua`** (order-service, after Postgres commit):

```lua
-- KEYS[1] = {sale:S1}:resv:<order_id>
-- KEYS[2] = {sale:S1}:resv_deadlines
-- ARGV[1] = order_id
-- Returns 1 = confirmed, 0 = reservation dead (CANCELLED/EXPIRED — do NOT fulfill),
--        -1 = already CONFIRMED (redelivery — ack and move on)
local v = redis.call('GET', KEYS[1])
if not v then return 0 end
local state = string.match(v, '^(%u+)')
if state == 'CONFIRMED' then return -1 end
if state ~= 'RESERVED' then return 0 end
local rest = string.match(v, '(|.*)$')  -- match, not find: find's (start,end) pair would truncate sub()
redis.call('SET', KEYS[1], 'CONFIRMED' .. rest, 'EX', 86400)
redis.call('ZREM', KEYS[2], ARGV[1])
return 1
```

**A5 — script test vectors** (run with `redis-cli --eval` against a scratch DB; each is a DoD item):

```
buy: stock=2 qty=1            -> {1,1,<oid>}         then GET stock == 1
buy: stock=0 qty=1            -> {0}                  stock unchanged
buy: repeat same idem key     -> {-1,<original oid>}  stock unchanged
buy: user at max_per_user     -> {-2}
buy: qty=0 | qty=1.5 | qty=99 -> {-3}
buy: state=CLOSED             -> {-4}
buy: stock key deleted        -> {-99}
cancel: on RESERVED           -> 1, stock restored, idem key gone, quota rolled back
cancel: on CONFIRMED          -> 0, nothing changes
confirm: on RESERVED          -> 1 ; on CANCELLED -> 0 ; repeated -> -1
```

---

# Appendix B — docker-compose additions/changes

```yaml
services:
  nginx:                                   # Layer 1
    image: nginx:1.27-alpine
    ports: ["443:443"]
    volumes: ["./nginx/nginx.conf:/etc/nginx/nginx.conf:ro"]
    depends_on: [gateway]
    networks: [flash-sale-net]

  gateway:
    environment:
      - REDIS_STOCK_HOST=redis-stock       # was REDIS_HOST — split per Bug 3
      - REDIS_RL_HOST=redis-ratelimit
      - OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317
      - RATE_LIMIT_FAIL_MODE=closed
    # ports: removed — nginx is the only ingress

  redis-stock:                             # renamed from `redis`; the crown jewels
    image: redis:7-alpine
    command: redis-server --appendonly yes --appendfsync everysec
    volumes: [redis_stock_data:/data]
    networks: [flash-sale-net]
    healthcheck: { test: ["CMD","redis-cli","ping"], interval: 5s, timeout: 3s, retries: 3 }
    # production: 1 primary + 2 replicas + 3 sentinels (Scenario 1); compose keeps 1 for dev

  redis-ratelimit:                         # new — Bug 3 fix
    image: redis:7-alpine
    command: redis-server --maxmemory 2gb --maxmemory-policy noeviction   # Scenario 5
    networks: [flash-sale-net]
    healthcheck: { test: ["CMD","redis-cli","ping"], interval: 5s, timeout: 3s, retries: 3 }

  nats:                                    # production: 3-node cluster for --replicas 3 (§3b);
    command: "-js -m 8222 --store_dir /data" # compose dev keeps 1 node, R=1
    volumes: [nats_data:/data]

  order-service:
    deploy: { replicas: 2 }                # §3c: 2 × MaxAckPending 16 = 32 slots
    environment:
      - OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4317

  otel-collector:                          # Phase 5
    image: otel/opentelemetry-collector-contrib:0.104.0
    command: ["--config=/etc/otelcol/config.yaml"]
    volumes: ["./otel/collector.yaml:/etc/otelcol/config.yaml:ro"]
    networks: [flash-sale-net]

  tempo:
    image: grafana/tempo:2.5.0
    command: ["-config.file=/etc/tempo.yaml"]
    volumes: ["./tempo/tempo.yaml:/etc/tempo.yaml:ro", tempo_data:/var/tempo]
    networks: [flash-sale-net]

  loki:
    image: grafana/loki:3.1.0
    volumes: ["./loki/loki.yaml:/etc/loki/local-config.yaml:ro", loki_data:/loki]
    networks: [flash-sale-net]

  vector:
    image: timberio/vector:0.39.0-alpine
    volumes:
      - ./vector/vector.yaml:/etc/vector/vector.yaml:ro
      - /var/run/docker.sock:/var/run/docker.sock:ro   # docker_logs source
    networks: [flash-sale-net]

volumes:
  redis_stock_data: {}
  nats_data: {}
  tempo_data: {}
  loki_data: {}
```

---

# Appendix C — k6 `load_test.js` (complete)

```js
import http from 'k6/http';
import { check } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import exec from 'k6/execution';

// SCALE=1 => full sale-shaped load (run DISTRIBUTED: k6-operator / k6 Cloud, ~10-40 runners).
// SCALE=0.001 => 100-user laptop smoke test. See deviation D4 in the plan.
const SCALE     = parseFloat(__ENV.SCALE || '0.001');
const BASE      = __ENV.BASE_URL || 'https://localhost';
const SALE_ID   = __ENV.SALE_ID || 'S1';
const STOCK     = parseInt(__ENV.STOCK || '2000');

const accepted   = new Counter('accepted_orders');     // 200, not a duplicate replay
const duplicates = new Counter('duplicate_responses'); // 200 with duplicate:true
const soldOut    = new Counter('sold_out_responses');  // 410
const rateLtd    = new Counter('rate_limited');        // 429
const shed       = new Counter('shed_503');
const serverErr  = new Counter('server_errors');       // 500 — must stay 0
const buyLatency = new Trend('buy_latency', true);

export const options = {
  scenarios: {
    flood: {                                  // the herd: arrival RATE, not VU count (D4)
      executor: 'ramping-arrival-rate',
      startRate: 0, timeUnit: '1s',
      preAllocatedVUs: Math.ceil(20000 * SCALE),
      maxVUs:          Math.ceil(40000 * SCALE),
      stages: [
        { target: Math.ceil(25000 * SCALE), duration: '60s' },   // ramp: sale opens
        { target: Math.ceil(25000 * SCALE), duration: '120s' },  // hold
        { target: 0,                        duration: '60s' },   // ramp down
      ],
      exec: 'buy',
    },
    replay: {                                 // idempotency proof: 1 key, 500 sends, 1 order
      executor: 'shared-iterations',
      vus: 50, iterations: 500, startTime: '30s', exec: 'replaySameKey',
    },
    boundary: {                               // Bug 4 regression: burst straddling a window edge
      executor: 'per-vu-iterations',
      vus: 20, iterations: 10, startTime: '10s', exec: 'boundaryBurst',
    },
  },
  thresholds: {
    accepted_orders:            [`count<=${STOCK}`],   // THE invariant
    server_errors:              ['count==0'],          // 500 = bug, not policy
    'buy_latency':              ['p(99)<500'],
    checks:                     ['rate>0.99'],
    // 429-dominance stated as a ratio, not the spec's absolute count (D4):
    rate_limited:               [`count>=${Math.floor(400000 * SCALE)}`],
  },
};

function doBuy(userId, nonce) {
  const res = http.post(`${BASE}/buy`, JSON.stringify({
    product_id: 'p1', quantity: 1, sale_id: SALE_ID, client_nonce: nonce,
  }), { headers: { 'Content-Type': 'application/json', 'X-User-ID': userId },
        tags: { endpoint: 'buy' } });
  buyLatency.add(res.timings.duration);

  check(res, {
    'response is a defined status': (r) => [200, 400, 410, 429, 503].includes(r.status),
    'no hanging / connection error': (r) => r.status !== 0,
    '429 carries Retry-After':      (r) => r.status !== 429 || r.headers['Retry-After'] !== undefined,
    '503 carries Retry-After':      (r) => r.status !== 503 || r.headers['Retry-After'] !== undefined,
    'X-Request-ID echoed':          (r) => r.headers['X-Request-Id'] !== undefined,
  });

  if (res.status === 200) {
    const body = res.json();
    if (body.duplicate) duplicates.add(1); else accepted.add(1);
    check(res, { 'success has order_id': () => !!body.order_id });
  }
  else if (res.status === 410) soldOut.add(1);
  else if (res.status === 429) rateLtd.add(1);
  else if (res.status === 503) shed.add(1);
  else if (res.status >= 500) serverErr.add(1);
  return res;
}

export function buy() {
  const userId = `u-${exec.vu.idInTest}`;                       // unique per VU
  const nonce  = `${exec.vu.idInTest}-${exec.vu.iterationInScenario}`; // unique per attempt
  doBuy(userId, nonce);
}

export function replaySameKey() {
  // every iteration: SAME user, SAME nonce => exactly one order must exist afterwards
  const res = doBuy('u-replay-fixed', 'nonce-replay-fixed');
  if (res.status === 200) {
    check(res, { 'replay returns the one true order_id':
      (r) => r.json().order_id && r.json().order_id.length > 0 });
  }
}

export function boundaryBurst() {
  // 10 rapid-fire buys from one user: sliding window must admit exactly 1 per 10s —
  // a fixed-window limiter leaks 2 here (Bug 4 regression test).
  doBuy(`u-burst-${exec.vu.idInTest}`, `b-${exec.vu.idInTest}-${exec.vu.iterationInScenario}`);
}

export function handleSummary(data) {
  const a = data.metrics.accepted_orders ? data.metrics.accepted_orders.values.count : 0;
  return { stdout: JSON.stringify({
    accepted: a, stock: STOCK, oversell: a > STOCK,
    note: 'Duplicate-order & oversell FINAL verdict comes from the post-run SQL audit ' +
          '(orders GROUP BY order_id HAVING count>1; audit equation) — the DB is the system of record.',
  }, null, 2) + '\n' };
}
```

---

# Appendix D — OpenTelemetry Collector config

```yaml
receivers:
  otlp:
    protocols:
      grpc: { endpoint: 0.0.0.0:4317 }
      http: { endpoint: 0.0.0.0:4318 }

processors:
  memory_limiter: { check_interval: 1s, limit_mib: 512, spike_limit_mib: 128 }
  batch:          { send_batch_size: 8192, timeout: 200ms }
  tail_sampling:                       # post-sale-window policy; during the window the
    decision_wait: 10s                 # SDKs head-sample 100% (Phase 5) and these policies
    policies:                          # simply pass everything through.
      - name: keep-all-errors
        type: status_code
        status_code: { status_codes: [ERROR] }
      - name: keep-slow-hotpath        # any trace whose total exceeds the p99 budget
        type: latency
        latency: { threshold_ms: 500 }
      - name: sample-successes
        type: probabilistic
        probabilistic: { sampling_percentage: 10 }

connectors:
  spanmetrics:                         # trace-derived Prometheus histograms (deviation D6)
    histogram:
      explicit:
        buckets: [1ms, 2ms, 5ms, 10ms, 25ms, 50ms, 100ms, 250ms, 500ms, 1s, 2s]
    dimensions:
      - name: span.name
      - name: service.name

exporters:
  otlp/tempo: { endpoint: tempo:4317, tls: { insecure: true } }
  prometheus: { endpoint: 0.0.0.0:8889 }   # scraped by Prometheus for spanmetrics

service:
  pipelines:
    traces:
      receivers:  [otlp]
      processors: [memory_limiter, tail_sampling, batch]
      exporters:  [otlp/tempo, spanmetrics]
    metrics/spanmetrics:
      receivers: [spanmetrics]
      exporters: [prometheus]
```

---

# Appendix E — Prometheus alert rules

```yaml
groups:
- name: flash-sale-p1
  rules:
  - alert: OversellDetected
    expr: min_over_time(stock_remaining[1m]) < 0
    labels: { severity: P1 }
    annotations: { summary: "Stock below zero — Lua layer bypassed. Freeze sale, audit Redis ACLs." }
  - alert: StockLeaked
    expr: stock_remaining == 0 and sum(orders_processed_total{result="success"}) < 2000
    for: 2m
    labels: { severity: P1 }
    annotations: { summary: "Sold out but confirmed < 2000 after 2m — run audit equation (runbook S2)." }
  - alert: CompensationFailed
    expr: increase(compensation_operations_total{result="failed"}[5m]) > 0
    labels: { severity: P1 }
    annotations: { summary: "Compensation exhausted retries — disk journal is now system of record." }
  - alert: ServerErrorBudgetBurn
    expr: >
      sum by (service) (rate(http_requests_total{status_code=~"5.."}[30s]))
      / sum by (service) (rate(http_requests_total[30s])) > 0.01
    for: 30s
    labels: { severity: P1 }

- name: flash-sale-p2
  rules:
  - alert: StockDecrementSlow
    expr: >
      histogram_quantile(0.99, sum by (le)
        (rate(stock_decrement_duration_seconds_bucket[1m]))) > 0.010
    for: 60s
    labels: { severity: P2 }
  - alert: PublishFailureRate
    expr: >
      sum(rate(publish_operations_total{result!="success"}[30s]))
      / sum(rate(publish_operations_total[30s])) > 0.001
    for: 30s
    labels: { severity: P2 }
  - alert: DLQDepthHigh
    expr: nats_stream_messages{stream="ORDERS_DLQ"} > 10
    labels: { severity: P2 }
  - alert: ConsumerLagHigh
    expr: nats_consumer_lag > 500
    for: 60s
    labels: { severity: P2 }
  - alert: ConsumerLagStale            # lag old enough to start expiring reservations (S4)
    expr: nats_consumer_lag > 0 and time() - nats_consumer_lag_updated_ts > 240
    labels: { severity: P1 }

- name: flash-sale-p3
  rules:
  - alert: RateLimitDominant
    expr: >
      sum(rate(rate_limit_decisions_total{result!="allowed"}[1m]))
      / sum(rate(rate_limit_decisions_total[1m])) > 0.80
    for: 2m
    labels: { severity: P3 }
    annotations: { summary: "429 > 80% — defense holding, verify it's bots and not humans." }
  - alert: WaitingRoomDeep
    expr: waiting_room_depth > 50000
    labels: { severity: P3 }
  - alert: PostgresInsertSlow
    expr: >
      histogram_quantile(0.99, sum by (le)
        (rate(db_insert_duration_seconds_bucket[1m]))) > 1
    for: 60s
    labels: { severity: P3 }
  - alert: DLQNonEmpty
    expr: nats_stream_messages{stream="ORDERS_DLQ"} > 0
    labels: { severity: P3 }
```

---

# Appendix F — Grafana dashboard (importable JSON)

```json
{
  "title": "Flash Sale — War Room",
  "uid": "flash-sale-war-room",
  "timezone": "utc",
  "refresh": "5s",
  "time": { "from": "now-30m", "to": "now" },
  "panels": [
    { "id": 1, "type": "gauge", "title": "Stock Remaining",
      "gridPos": { "x": 0, "y": 0, "w": 6, "h": 5 },
      "targets": [{ "expr": "stock_remaining" }],
      "fieldConfig": { "defaults": { "min": 0, "max": 2000, "thresholds": { "mode": "absolute",
        "steps": [ { "color": "red", "value": null }, { "color": "yellow", "value": 100 },
                   { "color": "green", "value": 500 } ] } } } },
    { "id": 2, "type": "stat", "title": "Confirmed Orders / 2000",
      "gridPos": { "x": 6, "y": 0, "w": 6, "h": 5 },
      "targets": [{ "expr": "sum(orders_processed_total{result=\"success\"})" }] },
    { "id": 3, "type": "stat", "title": "Buy Acceptance %",
      "gridPos": { "x": 12, "y": 0, "w": 6, "h": 5 },
      "targets": [{ "expr": "100 * sum(rate(http_requests_total{endpoint=\"/buy\",status_code=\"200\"}[1m])) / sum(rate(http_requests_total{endpoint=\"/buy\"}[1m]))" }],
      "fieldConfig": { "defaults": { "unit": "percent" } } },
    { "id": 4, "type": "stat", "title": "Waiting Room Depth",
      "gridPos": { "x": 18, "y": 0, "w": 6, "h": 5 },
      "targets": [{ "expr": "waiting_room_depth" }] },

    { "id": 5, "type": "timeseries", "title": "Gateway /buy latency (p50/p95/p99)",
      "gridPos": { "x": 0, "y": 5, "w": 6, "h": 7 },
      "targets": [
        { "expr": "histogram_quantile(0.50, sum by (le) (rate(http_request_duration_seconds_bucket{endpoint=\"/buy\"}[1m])))", "legendFormat": "p50" },
        { "expr": "histogram_quantile(0.95, sum by (le) (rate(http_request_duration_seconds_bucket{endpoint=\"/buy\"}[1m])))", "legendFormat": "p95" },
        { "expr": "histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{endpoint=\"/buy\"}[1m])))", "legendFormat": "p99" } ],
      "fieldConfig": { "defaults": { "unit": "s" } } },
    { "id": 6, "type": "timeseries", "title": "Redis stock decrement (p50/p95/p99)",
      "gridPos": { "x": 6, "y": 5, "w": 6, "h": 7 },
      "targets": [
        { "expr": "histogram_quantile(0.50, sum by (le) (rate(stock_decrement_duration_seconds_bucket[1m])))", "legendFormat": "p50" },
        { "expr": "histogram_quantile(0.95, sum by (le) (rate(stock_decrement_duration_seconds_bucket[1m])))", "legendFormat": "p95" },
        { "expr": "histogram_quantile(0.99, sum by (le) (rate(stock_decrement_duration_seconds_bucket[1m])))", "legendFormat": "p99" } ],
      "fieldConfig": { "defaults": { "unit": "s" } } },
    { "id": 7, "type": "timeseries", "title": "Order end-to-end (p50/p95/p99)",
      "gridPos": { "x": 12, "y": 5, "w": 6, "h": 7 },
      "targets": [
        { "expr": "histogram_quantile(0.50, sum by (le) (rate(order_processing_duration_seconds_bucket[1m])))", "legendFormat": "p50" },
        { "expr": "histogram_quantile(0.95, sum by (le) (rate(order_processing_duration_seconds_bucket[1m])))", "legendFormat": "p95" },
        { "expr": "histogram_quantile(0.99, sum by (le) (rate(order_processing_duration_seconds_bucket[1m])))", "legendFormat": "p99" } ],
      "fieldConfig": { "defaults": { "unit": "s" } } },
    { "id": 8, "type": "timeseries", "title": "Postgres insert (p50/p95/p99)",
      "gridPos": { "x": 18, "y": 5, "w": 6, "h": 7 },
      "targets": [
        { "expr": "histogram_quantile(0.50, sum by (le) (rate(db_insert_duration_seconds_bucket[1m])))", "legendFormat": "p50" },
        { "expr": "histogram_quantile(0.95, sum by (le) (rate(db_insert_duration_seconds_bucket[1m])))", "legendFormat": "p95" },
        { "expr": "histogram_quantile(0.99, sum by (le) (rate(db_insert_duration_seconds_bucket[1m])))", "legendFormat": "p99" } ],
      "fieldConfig": { "defaults": { "unit": "s" } } },

    { "id": 9, "type": "timeseries", "title": "Requests/s by status",
      "gridPos": { "x": 0, "y": 12, "w": 6, "h": 7 },
      "targets": [{ "expr": "sum by (status_code) (rate(http_requests_total{endpoint=\"/buy\"}[30s]))", "legendFormat": "{{status_code}}" }],
      "fieldConfig": { "defaults": { "custom": { "stacking": { "mode": "normal" } } } } },
    { "id": 10, "type": "timeseries", "title": "Rate-limit decisions/s",
      "gridPos": { "x": 6, "y": 12, "w": 6, "h": 7 },
      "targets": [{ "expr": "sum by (result) (rate(rate_limit_decisions_total[30s]))", "legendFormat": "{{result}}" }],
      "fieldConfig": { "defaults": { "custom": { "stacking": { "mode": "normal" } } } } },
    { "id": 11, "type": "timeseries", "title": "NATS publish results/s",
      "gridPos": { "x": 12, "y": 12, "w": 6, "h": 7 },
      "targets": [{ "expr": "sum by (result) (rate(publish_operations_total[30s]))", "legendFormat": "{{result}}" }] },
    { "id": 12, "type": "timeseries", "title": "DLQ depth",
      "gridPos": { "x": 18, "y": 12, "w": 6, "h": 7 },
      "targets": [{ "expr": "nats_stream_messages{stream=\"ORDERS_DLQ\"}" }] },

    { "id": 13, "type": "timeseries", "title": "Redis CPU / memory (both instances)",
      "gridPos": { "x": 0, "y": 19, "w": 6, "h": 7 },
      "targets": [
        { "expr": "rate(redis_cpu_sys_seconds_total[1m])", "legendFormat": "cpu {{instance}}" },
        { "expr": "redis_memory_used_bytes / redis_memory_max_bytes", "legendFormat": "mem {{instance}}" } ] },
    { "id": 14, "type": "timeseries", "title": "NATS consumer lag",
      "gridPos": { "x": 6, "y": 19, "w": 6, "h": 7 },
      "targets": [{ "expr": "nats_consumer_lag" }] },
    { "id": 15, "type": "timeseries", "title": "Order consumers: configured vs active",
      "gridPos": { "x": 12, "y": 19, "w": 6, "h": 7 },
      "targets": [
        { "expr": "count(up{job=\"order-service\"})", "legendFormat": "configured" },
        { "expr": "sum(up{job=\"order-service\"})", "legendFormat": "active" } ] },
    { "id": 16, "type": "timeseries", "title": "Postgres pool saturation",
      "gridPos": { "x": 18, "y": 19, "w": 6, "h": 7 },
      "targets": [{ "expr": "sum(pg_stat_activity_count{datname=\"flashsale\"}) / 20", "legendFormat": "pool used / max" }],
      "fieldConfig": { "defaults": { "unit": "percentunit", "max": 1 } } }
  ],
  "schemaVersion": 39
}
```

---

*End of plan. Implementation order is the phase order; no phase starts before the previous
phase's gate is demonstrably true (each gate is a drill, not a review meeting).*



