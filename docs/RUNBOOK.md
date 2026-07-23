# Flash-Sale Engine — Operator Runbook

On-call, 2 a.m. edition. Every failure below degrades to *"some users see 503"*, **not** *"money is wrong"* — the architecture (atomic Lua reservation + CAS state machine + acked publishes) guarantees that. Your job in almost every incident is: **(1) run the integrity audit to confirm the invariant held, (2) restore capacity.**

> Source of truth: `docs/production-plan.md` (Phase 7 + checklists). This runbook consolidates it; it invents nothing.

## Conventions

- `PID` = the sale / product id. The live sale is **`S1`**, initial stock **`2000`**, `max_per_user = 2`.
  Set it once so you can paste commands:
  ```bash
  export PID=S1
  ```
- Commands use **service hostnames** on the compose network (`redis-stock`, `redis-ratelimit`, `nats`, `postgres`). From a shell inside the network, use the hostname; from the host, substitute the published port from the table below.
- Redis keys are hash-tagged `{sale:<PID>}` — **always single-quote them** so your shell doesn't glob `{}`.

## Service topology & ports

| Service | Role | Address |
|---|---|---|
| nginx | Edge / TLS / L1 rate limit | `:80` |
| gateway (Node) | Auth, L2 limiter, waiting room, circuit breaker | `:3000` |
| inventory (Go) | Atomic buy script, compensation, reaper | `:8080` |
| order-service (Python) | JetStream consumer → Postgres; metrics | metrics `:9100` |
| redis-stock | Crown jewels: stock counter, reservations, idem, sale state (AOF `everysec`) | `redis-stock` (compose host `:6379`; prod `:6390`) |
| redis-ratelimit | Rate-limit counters + waiting room; `noeviction` | `redis-ratelimit` (no host port in compose; `maxmemory 512mb`) |
| NATS JetStream | `ORDERS` (WorkQueue) + `ORDERS_DLQ` | client `:4222`, monitor `:8222` |
| Postgres | Durable order ledger, `UNIQUE(order_id)` | `:5432` (db `flashsale`, user `flashsale_user`) |
| Prometheus | Metrics / alert rules | `:9090` |
| Grafana | "Flash Sale War Room" dashboard | `:3001` |
| Alertmanager | Paging | `:9093` |
| Tempo | Traces | `:3200` |
| Loki | Logs | `:3100` |

---

## 1. Quick reference — symptom → section → severity

| Symptom / signal | Section | Sev |
|---|---|---|
| `redis_up{instance="redis-stock"} == 0`, redisBreaker open, all `/buy` → 503 | [S1](#s1--stock-redis-primary-fails) | **P1** |
| `publish_operations_total{result="failed"}` climbing, natsBreaker open | [S2](#s2--nats-unreachable-after-decrement) | P2 |
| `compensation_operations_total{result="failed"} > 0` | [S2](#s2--nats-unreachable-after-decrement) | **P1** |
| Active consumer count drops, lag beacon gap | [S3](#s3--order-service-crashes-mid-consume) | P2 |
| `db_insert_duration_seconds` p99 rising (1 s → 10 s), pool exhaustion | [S4](#s4--postgres-write-latency-spike--pool-exhaustion) | P3→P2→P1 |
| `redis_evicted_keys_total` rate > 0, "allowed" ratio suddenly rising | [S5](#s5--rate-limit-redis-eviction-under-memory-pressure) | P2 |
| nginx/CF 429 spike, one `user_id` hash dominating gateway logs | [S6](#s6--single-user-app-layer-ddos) | P3 |
| `stock_remaining` ≠ 2000 at T−30 (dual-read mismatch) | [S7](#s7--stock-pre-set-wrong) | P1 (pre-sale) |
| `stock_operations_total{result="duplicate"}` ratio anomaly | [S8](#s8--duplicate-idempotency-key-spike) | P3 |
| `nats stream info ORDERS` bytes/msgs near cap (80% alert) | [S9](#s9--jetstream-stream-fills-up) | P2 |
| `process_resident_memory_bytes` slope + gateway restarts | [S10](#s10--gateway-node-oom) | P2 |
| Boundary-burst admits > limit; cross-pod limiter variance | [S11](#s11--clock-skew) | P3 |
| Rolling deploy / SIGTERM behavior | [S12](#s12--graceful-shutdown--rolling-deploy) | — |
| Stock leaked: `stock_remaining == 0` and successes < 2000 for 2 m | run [audit](#2-the-integrity-audit) first | **P1** |
| **Kill switch — halt the sale now** | [Abort](#6-abort-procedure) | — |

Alert severity map (Phase 6): **P1 = page now**, **P2 = page ≤ 5 min**, **P3 = Slack**.

---

## 2. The integrity audit

**Run this first in ANY stock-related incident.** It is the closed-form proof that no oversell happened. All three terms must sum to the initial stock (**2000**).

```
GET {sale:PID}:stock  +  Σ confirmed qty (Postgres)  +  Σ open reservations  =  2000
```

```bash
# 1. Redis stock counter
redis-cli -h redis-stock GET '{sale:'"$PID"'}:stock'

# 2. Confirmed quantity in the durable ledger
psql -h postgres -U flashsale_user -d flashsale -tAc \
  "SELECT COALESCE(SUM(quantity),0) FROM orders WHERE sale_id='$PID' AND status='CONFIRMED';"

# 3. Open reservations (still RESERVED, not yet confirmed/expired)
redis-cli -h redis-stock ZCARD '{sale:'"$PID"'}:resv_deadlines'
```

- Term 3 (`ZCARD`) is the **count** of open reservations. Each reservation stores its qty in its value (`STATE|qty|user|idemHash`); when the sale allows qty > 1 per line, get the exact quantity by summing the values:
  ```bash
  for k in $(redis-cli -h redis-stock --scan --pattern '{sale:'"$PID"'}:resv:*'); do
    redis-cli -h redis-stock GET "$k"; done | awk -F'|' '$1=="RESERVED"{s+=$2} END{print s}'
  ```
- **Balances → the invariant held.** Any 503s were clean sheds; no money is wrong.
- **Short of 2000 →** writes were lost (e.g. a failover's async-replication window, typically < 1 s of writes). Correct with an audited `SET`/`GETSET` (see [S1](#s1--stock-redis-primary-fails) / [S7](#s7--stock-pre-set-wrong)); accept that a handful of "queued" responses may never confirm — those reservations never replicated, and those users get a clean retry.
- **Over 2000 (impossible by design) →** treat as a script/data-corruption P1; do not open traffic.

Reservation-state reference: value is `STATE|qty|user|idemHash` in `{sale:<PID>}:resv:<orderID>`; state machine is `RESERVED → CONFIRMED | CANCELLED | EXPIRED` (CAS forward-only — exactly one of confirm/cancel/expire wins).

---

## 3. The 12 scenarios

### S1 — Stock-Redis primary fails

- **Detection:** `redis_up{instance="redis-stock"} == 0`; redisBreaker open (gobreaker state gauge = 1); `stock_decrement_duration_seconds` p99 alert.
- **Automated response:** redisBreaker opens → all `/buy` return `503` + `Retry-After: 15`; gateway routes to the waiting room. The waiting room lives in **redis-ratelimit**, so it is unaffected (§1 L3 design).
- **Manual steps:**
  ```bash
  # Sentinel promotes a replica (target < 30 s)
  redis-cli -h <sentinel-host> -p 26379 sentinel failover flashsale
  ```
  **Before re-enabling traffic, run the [integrity audit](#2-the-integrity-audit).** If short of 2000 (lost writes in the failover window):
  ```bash
  # correct the counter with an audit trail
  redis-cli -h redis-stock SET '{sale:'"$PID"'}:stock' <corrected_value>
  ```
- **Customer impact:** 503s during the failover only; queued-but-unreplicated reservations see a clean retry.
- **RTO:** **< 45 s** (Sentinel promotion < 30 s + audit). This single event is the whole ≤ 15 s cumulative-outage error budget — two failovers in one window = SLO breach.

### S2 — NATS unreachable after decrement

- **Detection:** `publish_operations_total{result="failed"}` increments; natsBreaker opens.
- **Automated response:** the §2c **cancel** Lua script restores stock (CAS `RESERVED→CANCELLED` + `INCRBY` stock + `DECRBY` quota + `DEL` idem + `ZREM` deadline). Breaker-open then gates *entry* — the buy script is skipped so no reservation is created only to be compensated. If Redis **and** NATS are both down, the failure appends to a local disk journal and a background goroutine replays it.
- **Manual steps:**
  ```bash
  # 1. Prove no money was lost
  #    (run the integrity audit — section 2)
  # 2. THE P1 check — this must be zero:
  #    Prometheus: compensation_operations_total{result="failed"}
  # 3. If the journal was used, confirm the replayer drained it (both inventory pods):
  #    inventory /health should report journal depth == 0
  ```
  `compensation_operations_total{result="failed"} > 0` ⇒ **P1** — the disk journal is now the system of record until it drains.
- **Customer impact:** clean `503` "order not accepted, please retry"; **never** a silent loss.
- **RTO:** publishes resume on NATS reconnect (< 5 s); compensation is synchronous on the failure path.

### S3 — order-service crashes mid-consume

- **Detection:** active consumer count drops (war dashboard Row 4); lag-beacon gap (§3e treats silence on `control.orders.lag` as a bad signal).
- **Automated response:** JetStream redelivers the crashed instance's ≤ 16 unacked messages to the surviving instance after **AckWait 30 s**. Double delivery is absorbed by `confirm.lua` CAS + Postgres `UNIQUE(order_id)` (`ON CONFLICT DO NOTHING`).
- **Manual steps:**
  ```bash
  nats -s nats://nats:4222 consumer info ORDERS order-workers   # num_ack_pending, num_redelivered, num_pending
  nats -s nats://nats:4222 stream view ORDERS_DLQ               # anything that exceeded max_deliver=5
  ```
  Only replay from the DLQ ([§3d procedure](#s9--jetstream-stream-fills-up) / drain block below) **after** the crash cause is fixed.
- **Customer impact:** those ≤ 16 in-flight orders confirm up to 30 s late — inside the 60 s SLO-3 window.
- **RTO:** redelivery within AckWait (30 s); full drain target < 63 s with one instance dead.

### S4 — Postgres write latency spike / pool exhaustion

- **Detection:** `db_insert_duration_seconds` p99 alert. **P3 at 1 s precedes the crisis**; escalates.
- **Automated response:** `pybreaker` (`fail_max=10`, `reset_timeout=5`) opens → consumer `nak(delay=5)` → messages **wait in the stream** instead of stacking on the pool. The stream *is* the buffer.
- **Manual steps — the exact triage query:**
  ```sql
  SELECT pid, state, wait_event_type, wait_event, now() - query_start AS dur,
         left(query, 80) AS query
  FROM pg_stat_activity
  WHERE datname = 'flashsale' AND state <> 'idle'
  ORDER BY dur DESC LIMIT 20;
  ```
  ```sql
  -- blocking DDL / vacuum / lock holder → terminate the blocker
  SELECT pg_terminate_backend(<pid>);
  ```
  Pool exhaustion → confirm **MaxAckPending (16) < pool (20)** still holds (tuning one without the other is the usual cause).
- **Customer impact:** none while draining resumes within the **reservation TTL (300 s)** deadline. Past ~4 min, reservations expire and stock returns for orders that then drop-on-confirm — safe (no oversell) but disappointing.
- **RTO / escalation:** drain must resume within ~4 min. **P2 escalates to P1 at lag age > 240 s.**

### S5 — Rate-limit Redis eviction under memory pressure

- **Detection:** `redis_evicted_keys_total` rate > 0. The subtle tell: `rate_limit_decisions_total{result="allowed"}` ratio suddenly **rising** — evicted counters make everyone look new, so everyone passes.
- **Automated response (prevented, not mitigated):** redis-ratelimit runs **`maxmemory-policy noeviction`**. Rationale: under `allkeys-lru` the limiter would silently self-disable *exactly under attack* — the worst possible behavior. With `noeviction`, writes fail → limiter errors → gateway **fails closed** to 503 (§1 L2e) — loud and safe.
  ```bash
  redis-cli -h redis-ratelimit CONFIG GET maxmemory-policy   # must be: noeviction
  ```
- **Manual steps:**
  ```bash
  redis-cli -h redis-ratelimit CONFIG SET maxmemory 4gb      # give it headroom
  redis-cli -h redis-stock INFO memory                       # verify redis-stock UNTOUCHED (separate instance — the point of Bug 3's fix)
  ```
- **Customer impact:** brief 503 + waiting-room routing under pressure; fairness preserved.
- **RTO:** seconds (`CONFIG SET` is live).

### S6 — Single-user app-layer DDoS

- **Detection:** Cloudflare rate rule + nginx `429` spike; a single `user_id` hash dominating gateway logs.
- **Automated response:** the layer cake absorbs it — CF (6 req / 10 s per IP) and nginx (10 r/s per IP) mean 50k req/s *reaching the gateway* needs ~5 000 distinct IPs, which is Layer 0's bot detection (bot score / ASN adaptive) firing first. The limiter script does ≈ 80–100k ops/s on one core; the adaptive throttle (§1 L2e) sheds above that.
- **Manual steps:**
  ```bash
  # fast-path deny the offending user (checked before the limiter script)
  redis-cli -h redis-ratelimit SET 'rl:{'"<uid>"'}:blocked' 1 EX 3600
  ```
  Add a Cloudflare block rule on the offending fingerprint/ASN.
- **Customer impact:** the abuser is blocked; legitimate users unaffected.
- **RTO:** immediate on rule application.

### S7 — Stock pre-set wrong

- **Detection:** pre-sale checklist **dual-read** mismatch; `stock_remaining` ≠ 2000 on the war dashboard at T−30.
- **Automated response:** none — this is a pre-sale human check.
- **Manual steps — atomic, self-auditing:**
  ```bash
  redis-cli -h redis-stock GETSET '{sale:'"$PID"'}:stock' 2000
  ```
  `GETSET` so the correction **and** the evidence of what was wrong are one operation (the returned old value goes into the audit log). Do **not** set stock inside the sale-open transaction — that makes the open the night's last untested write.
- **Customer impact:** none if caught at T−30 (sale still `CLOSED`).
- **Root cause class:** runtime-mutable sale parameters — which is why §2f froze `max_per_user`/TTLs into env, not live Redis config.

### S8 — Duplicate idempotency-key spike

- **Detection:** `stock_operations_total{result="duplicate"}` ratio anomaly.
- **Automated response:** already correct — an idempotent replay returns the **original** response (`{-1, order_id}` → `200 {order_id, duplicate:true}`), not a 409. Nothing to fix mechanically.
- **Manual triage:** sample gateway logs by `(user, nonce)`:
  - **Same `(user, nonce)` re-arriving** = client retry bug. Benign; coordinate with the client team.
  - **Distinct nonces mapping to one idem hash** = it is *not* a SHA-256 collision — your nonce embedding is broken (e.g. ticket reused across users). Check JWT validation and the gateway's nonce-minting path.
- **Customer impact:** none (correct behavior).

### S9 — JetStream stream fills up

- **Detection:** `nats stream info ORDERS` shows bytes/msgs near the caps (`100000 msgs / 64 MB`); exporter alert at 80%.
- **Automated response:** stream is configured **`--discard new`**. Rationale (load-bearing): `DiscardOld` would silently delete the *oldest* message — an accepted order — to admit a new one, the exact catastrophe this design prevents. `DiscardNew` fails the **publish** instead, which flows into the §3a error path → compensation → clean 503. Legitimate content is stock-bounded at ~600 KB, so hitting 64 MB *means* a pathology; the correct response to a pathology is refusal, not amnesia.
- **Manual steps — find the pathology first (redelivery storm? publisher loop?):**
  ```bash
  nats -s nats://nats:4222 stream info ORDERS
  nats -s nats://nats:4222 consumer info ORDERS order-workers   # num_redelivered climbing?
  # ONLY if content is legitimate, raise the cap:
  nats -s nats://nats:4222 stream edit ORDERS --max-bytes 128MB
  ```
  **DLQ drain procedure** (idempotency makes replay safe):
  ```bash
  nats -s nats://nats:4222 stream view ORDERS_DLQ                        # inspect payload + headers
  nats -s nats://nats:4222 consumer info ORDERS order-workers            # confirm cause is gone
  nats -s nats://nats:4222 stream get ORDERS_DLQ <seq> --raw | nats pub orders.pending --stdin   # replay one
  nats -s nats://nats:4222 stream rmm ORDERS_DLQ <seq>                   # discard junk after triage
  ```
- **Customer impact:** clean 503s while at cap; no accepted order is ever deleted.
- **RTO:** seconds (`stream edit` is live) once the pathology is understood.

### S10 — Gateway Node OOM

- **Detection:** `process_resident_memory_bytes` slope alert; container restart events.
- **Automated response:** `restart: unless-stopped` (compose) / K8s liveness restarts the pod; the LB health check ejects it during restart.
- **Config (the guardrails):**
  ```
  node --max-old-space-size=1536      # 1536 MiB heap cap in a 2 GiB container
                                      # (headroom for RSS ≠ heap: buffers, stack, code)
  server.maxConnections = 8000
  agent = new http.Agent({ keepAlive: true, maxSockets: 200 })   # toward inventory
  ```
- **Manual steps:**
  ```bash
  # capture a heap snapshot from the live process
  kill -USR2 <node_pid>     # requires node --heapsnapshot-signal=SIGUSR2
  ```
  Look for the usual suspect in *this* codebase: **per-request state held past the response** — breaker event listeners, axios interceptors, the in-flight counter map — anything keyed by request that outlives it.
- **Customer impact:** brief 503s during restart; LB drains the bad pod.
- **RTO:** restart + health-check re-add (seconds).

### S11 — Clock skew

- **Detection (subtle):** the boundary-burst k6 scenario admits > limit; cross-pod limiter variance.
- **Automated response (structural, already shipped):** every time-dependent decision reads **`redis.call('TIME')` inside the Lua script** — the limiter (§1 L2) and the reservation deadlines (§2a `ZADD resv_deadlines`). One clock: the data's clock. Gateway/inventory wall clocks are used **only for logging**.
- **Manual steps:** NTP on hosts is hygiene, not a correctness dependency. If skew is suspected, verify Redis is the single time source and that no code path substituted a wall clock:
  ```bash
  redis-cli -h redis-stock TIME    # [ unix_seconds, microseconds ]
  ```
- **Customer impact:** none for correctness (script serialization is immune). Investigate logging timestamps only.

### S12 — Graceful shutdown / rolling deploy

**Rule one (on the pre-sale checklist): NO deploys inside `T−30 → sale end`.** Post-sale rolling deploys only. SIGTERM contracts per service:

- **Gateway (Node):** LB drain **first** (fail the health check for 2 probe periods), then SIGTERM → `server.close()` (stop accepting; in-flight requests finish) → force-exit at **30 s**.
  ```bash
  docker compose kill -s SIGTERM gateway    # after LB has drained it
  ```
- **Inventory (Go):** stop accepting HTTP → wait for in-flight handlers (each is ≤ one Lua call + one publish, < 3 s) → **flush the compensation-journal replayer** → then close Redis/NATS clients. **Order matters: journal before clients.**
- **Order-service (Python):** stop pulling new messages → finish current ones and **ack** → **do NOT nak on shutdown**. An unacked message redelivers cleanly after AckWait; a nak-on-shutdown immediately re-queues work you may be mid-committing in Postgres, manufacturing the double-processing race for zero benefit.

---

## 4. Pre-sale checklist (T−30 min, war room open)

Freeze everything. Nothing below should surprise you.

- [ ] **Stock counter dual-read** — both paths agree on 2000:
  ```bash
  redis-cli -h redis-stock GET '{sale:'"$PID"'}:stock'   # = 2000
  ```
  and the war-dashboard `stock_remaining` gauge = 2000.
- [ ] **RL Redis health** — headroom + eviction policy:
  ```bash
  redis-cli -h redis-ratelimit INFO memory                 # used < 60% of maxmemory
  redis-cli -h redis-ratelimit CONFIG GET maxmemory-policy  # = noeviction
  ```
- [ ] **NATS stream & consumer** — empty and correctly configured:
  ```bash
  nats -s nats://nats:4222 stream info ORDERS               # 0 messages
  nats -s nats://nats:4222 consumer info ORDERS order-workers
  # → 2 active pulls, num_pending=0, max_deliver=5, ack_wait=30s, max_ack_pending=16
  ```
- [ ] **Metrics** — Prometheus `/targets` all UP; every war-dashboard Row-1 panel non-N/A.
- [ ] **Traces** — `otelcol_processor_dropped_spans == 0`; one synthetic buy renders end-to-end in Tempo (`:3200`).
- [ ] **Logs** — Vector `/health` OK, buffer < 10%; a synthetic buy's `request_id` is queryable in Loki (`:3100`) within 2 s.
- [ ] **Breakers** — gateway `/health` reports all breakers CLOSED; gobreaker state gauge = 0 for both `redis-stock` and `nats-publish`.
- [ ] **DLQ empty:**
  ```bash
  nats -s nats://nats:4222 stream info ORDERS_DLQ           # 0 messages
  ```
- [ ] **Sale gate CLOSED:**
  ```bash
  redis-cli -h redis-stock GET '{sale:'"$PID"'}:state'      # = CLOSED
  ```
- [ ] **Smoke test** — k6 `SCALE=0.001` (100 VU / 10 s) against staging: all thresholds green, audit SQL clean.
- [ ] **Humans** — on-call acked pages, war room live, this runbook URL pinned, deploy freeze announced in #eng.
- [ ] **Abort plan rehearsed** — see [Abort procedure](#6-abort-procedure); confirm the team knows the one command.

**Opening the sale** (stock is already pre-set and verified — never set it here):
```
MULTI
SET {sale:PID}:state OPEN
PUBLISH sale_events '{"sale_id":"PID","state":"OPEN"}'
EXEC
```

---

## 5. Post-sale audit checklist

- [ ] **Zero oversell** — the [integrity audit](#2-the-integrity-audit) balances **to the unit = 2000**:
  ```
  GET stock  +  Σ confirmed qty  +  Σ open reservations  =  2000
  ```
- [ ] **Zero duplicates:**
  ```sql
  SELECT order_id, count(*) FROM orders GROUP BY order_id HAVING count(*) > 1;   -- must return 0 rows
  ```
- [ ] **Accepted ⇒ persisted** — reconcile gateway 200-count (Prometheus) vs Postgres confirmed + compensated count; any unexplained gap is an incident.
- [ ] **DLQ drained** — triage every `ORDERS_DLQ` message (replay or discard) with a note; end at 0:
  ```bash
  nats -s nats://nats:4222 stream info ORDERS_DLQ           # 0 messages
  ```
- [ ] **Compensation ledger clean** — `compensation_operations_total{result="failed"}` = 0 **and** the disk journal empty on **both** inventory pods.
- [ ] **Reservations resolved** — nothing left in RESERVED:
  ```bash
  redis-cli -h redis-stock ZCARD '{sale:'"$PID"'}:resv_deadlines'   # = 0
  ```
  (The reaper returns expired reservations to stock every 5 s; a nonzero value long after close means order-service never drained.)
- [ ] **Archive** — Tempo blocks + Loki chunks for the window copied to the 90-day bucket; Grafana snapshot of the war dashboard attached to the retro doc.
- [ ] **Retro** — every P1/P2 that fired gets a timeline; every alert that *should* have fired and didn't gets a new rule.

---

## 6. Abort procedure

**Kill switch — halt all new reservations immediately:**

```bash
redis-cli -h redis-stock SET '{sale:'"$PID"'}:state' CLOSED
```

- The gate is checked **in-script** (line 2 of `buy.lua`: `if GET state ~= 'OPEN' then return {-4}`), so new reservations stop in **≤ 1 s** — no code deploy, no restart needed.
- **Open reservations are not touched** — they drain naturally: each confirms (order-service `confirm.lua`) or expires (reaper returns stock at TTL + ≤ 5 s). The invariant continues to hold throughout.
- After aborting, run the [integrity audit](#2-the-integrity-audit) to confirm the balance, then decide whether to resume (`SET state OPEN` + `PUBLISH sale_events`) or stay closed.
- This is the same command as the pre-sale "sale gate CLOSED" state — reaching for it mid-sale is safe and reversible.
