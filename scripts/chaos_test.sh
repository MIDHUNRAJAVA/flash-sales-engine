#!/usr/bin/env bash
#
# chaos_test.sh — concurrency & idempotency chaos drills for the flash-sale
# engine, plus a reusable zero-oversell audit-equation checker.
#
# Runs against a RUNNING stack (docker compose or local native processes).
# Note: `set -e` is deliberately NOT used — many buys are EXPECTED to fail
# (410/429/409) and we do not want the script to abort on those.
set -uo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:3000}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:8080}"
REDIS_STOCK_HOST="${REDIS_STOCK_HOST:-localhost}"
REDIS_STOCK_PORT="${REDIS_STOCK_PORT:-6379}"

PGHOST="${PGHOST:-localhost}"
PGPORT="${PGPORT:-5432}"
PGDATABASE="${PGDATABASE:-flashsale}"
PGUSER="${PGUSER:-flashsale_user}"
PGPASSWORD="${PGPASSWORD:-change_me}"
export PGPASSWORD

PRODUCT_ID="${PRODUCT_ID:-p1}"

# ---------------------------------------------------------------------------
# Chaos drills covered here (pure HTTP + redis + psql):
#   1. Concurrency / no-oversell
#   2. Idempotent replay
#
# NOT covered here (require stopping the nats/order processes; documented in
# docs/RUNBOOK.md):
#   - NATS-kill compensation drill: kill nats mid-flight, verify reservations
#     that never reach the order service are compensated (stock returned).
#   - Reaper drill: let reservations expire past their deadline and verify the
#     reaper releases held stock back to the available counter.
# Those need orchestration of process lifecycles beyond HTTP, so they live in
# the runbook, not this script.
# ---------------------------------------------------------------------------

HAVE_JQ=0
if command -v jq >/dev/null 2>&1; then HAVE_JQ=1; fi

RED=$'\033[31m'; GREEN=$'\033[32m'; YEL=$'\033[33m'; BOLD=$'\033[1m'; RESET=$'\033[0m'
DRILL_FAILURES=0

hdr()  { printf '\n%s========== %s ==========%s\n' "$BOLD" "$1" "$RESET"; }
pass() { printf '  %s✓ PASS%s %s\n' "$GREEN" "$RESET" "$1"; }
fail() { printf '  %s✗ FAIL%s %s\n' "$RED" "$RESET" "$1"; DRILL_FAILURES=$((DRILL_FAILURES+1)); }
info() { printf '  %s·%s %s\n' "$YEL" "$RESET" "$1"; }

redis_cli() { redis-cli -h "$REDIS_STOCK_HOST" -p "$REDIS_STOCK_PORT" "$@" 2>/dev/null; }

seed() {
    local pid="$1" qty="$2"
    curl -s -X POST "$INVENTORY_URL/seed" \
        -H 'Content-Type: application/json' \
        -d "{\"productID\":\"$pid\",\"quantity\":$qty}" >/dev/null
}

# psql_scalar <sql>  -> single value, tuples-only + no-align.
psql_scalar() {
    psql -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE" \
        -tA -c "$1" 2>/dev/null | tr -d '[:space:]'
}

# ---------------------------------------------------------------------------
# audit_equation <pid> <initial_stock>
#
# Verifies the zero-oversell conservation law:
#     initial_stock == redis_stock + confirmed_qty + open_reservations
#
# Each unit lives in exactly one bucket: sellable (redis stock counter), sold &
# durable (Postgres CONFIRMED), or in-flight (redis reservation zset). If the
# three terms sum to MORE than initial -> OVERSELL. LESS than initial -> a leak
# (units stuck: a reservation that neither confirmed nor was released). Prints
# every term. Returns 0 on balance, 1 otherwise.
# ---------------------------------------------------------------------------
audit_equation() {
    local pid="$1" initial="$2"

    local stock resv confirmed
    stock=$(redis_cli GET "{sale:${pid}}:stock");  stock="${stock:-0}"
    resv=$(redis_cli ZCARD "{sale:${pid}}:resv_deadlines"); resv="${resv:-0}"
    confirmed=$(psql_scalar "SELECT COALESCE(SUM(quantity),0) FROM orders WHERE product_id='${pid}' AND status='CONFIRMED';")
    confirmed="${confirmed:-0}"

    local total=$(( stock + confirmed + resv ))

    info "audit equation for '${pid}':"
    info "    redis stock (sellable)          = ${stock}"
    info "    confirmed qty (postgres)        = ${confirmed}"
    info "    open reservations (ZCARD)       = ${resv}"
    info "    -------------------------------------------"
    info "    sum                             = ${total}"
    info "    initial stock (seeded)          = ${initial}"

    if [ "$total" -eq "$initial" ]; then
        pass "conservation holds: ${stock}+${confirmed}+${resv} == ${initial}"
        return 0
    elif [ "$total" -gt "$initial" ]; then
        fail "OVERSELL: sum ${total} > initial ${initial}"
        return 1
    else
        fail "LEAK: sum ${total} < initial ${initial} (units stuck in limbo)"
        return 1
    fi
}

# fire_buys <base_url> <pid> <count> <mode>
#   mode=distinct -> distinct user+nonce per request
#   mode=samenonce-> same user + same nonce for every request (idempotency)
# Backgrounds each curl and waits for all to finish.
fire_buys() {
    local base="$1" pid="$2" count="$3" mode="$4"
    local i user nonce
    for i in $(seq 1 "$count"); do
        if [ "$mode" = "samenonce" ]; then
            user="replay_user"; nonce="replay-nonce-fixed"
        else
            user="chaos_u_${i}"; nonce="chaos_n_${i}"
        fi
        curl -s -o /dev/null -X POST "$base/buy" \
            -H 'Content-Type: application/json' \
            -d "{\"userID\":\"$user\",\"productID\":\"$pid\",\"quantity\":1,\"clientNonce\":\"$nonce\"}" &
    done
    wait
}

# ---- preflight -------------------------------------------------------------
for bin in redis-cli psql curl; do
    if ! command -v "$bin" >/dev/null 2>&1; then
        echo "$bin not found on PATH; required for chaos drills." >&2
        exit 2
    fi
done

printf '%sflash-sale chaos test%s\n' "$BOLD" "$RESET"
echo "  gateway=$GATEWAY_URL inventory=$INVENTORY_URL"
echo "  redis=$REDIS_STOCK_HOST:$REDIS_STOCK_PORT"
echo "  pg=$PGHOST:$PGPORT/$PGDATABASE user=$PGUSER"

# ===========================================================================
# DRILL 1 — Concurrency / no-oversell
# ===========================================================================
hdr "DRILL 1: concurrency / no-oversell"
PID1="${PRODUCT_ID}-chaos-conc-$$"
INIT1=50
CONCURRENCY1=500

seed "$PID1" "$INIT1"
sleep 0.3
info "seeded ${PID1} with stock=${INIT1}; firing ${CONCURRENCY1} concurrent direct-inventory buys"

# Direct to INVENTORY_URL to bypass the gateway rate limiter — the point is to
# stress the inventory reservation path, not the token bucket.
fire_buys "$INVENTORY_URL" "$PID1" "$CONCURRENCY1" "distinct"
sleep 0.5

STOCK1=$(redis_cli GET "{sale:${PID1}}:stock"); STOCK1="${STOCK1:-<nil>}"
if [ "$STOCK1" = "0" ]; then
    pass "redis stock == 0 (exactly drained, never negative)"
elif [[ "$STOCK1" =~ ^-?[0-9]+$ ]] && [ "$STOCK1" -lt 0 ]; then
    fail "redis stock is NEGATIVE ($STOCK1) — oversell occurred"
else
    fail "redis stock == $STOCK1 (expected 0 after $CONCURRENCY1 buys vs stock $INIT1)"
fi

# confirmed + open reservations must account for exactly the initial stock.
audit_equation "$PID1" "$INIT1"

DUPES1=$(psql_scalar "SELECT COUNT(*) FROM (SELECT order_id FROM orders WHERE product_id='${PID1}' GROUP BY order_id HAVING COUNT(*)>1) d;")
DUPES1="${DUPES1:-0}"
if [ "$DUPES1" = "0" ]; then
    pass "no duplicate order_ids in postgres"
else
    fail "$DUPES1 duplicate order_id group(s) in postgres"
fi

# ===========================================================================
# DRILL 2 — Idempotent replay under concurrency
# ===========================================================================
hdr "DRILL 2: idempotent replay (same nonce x200 concurrent)"
PID2="${PRODUCT_ID}-chaos-idem-$$"
INIT2=100
REPLAYS=200

seed "$PID2" "$INIT2"
sleep 0.3
info "seeded ${PID2} with stock=${INIT2}; firing ${REPLAYS} concurrent buys with ONE fixed nonce"

fire_buys "$INVENTORY_URL" "$PID2" "$REPLAYS" "samenonce"
sleep 0.5

STOCK2=$(redis_cli GET "{sale:${PID2}}:stock"); STOCK2="${STOCK2:-<nil>}"
DECREMENTED=$(( INIT2 - ${STOCK2:-0} ))
if [ "$STOCK2" = "$((INIT2 - 1))" ]; then
    pass "exactly 1 unit decremented ($INIT2 -> $STOCK2) despite $REPLAYS concurrent replays"
else
    fail "expected stock $((INIT2 - 1)) (1 decrement), got $STOCK2 (decremented $DECREMENTED)"
fi

# The full equation should also hold with a single confirmed/reserved unit.
audit_equation "$PID2" "$INIT2"

# ===========================================================================
# summary
# ===========================================================================
hdr "summary"
if [ "$DRILL_FAILURES" -eq 0 ]; then
    printf '%sALL CHAOS DRILLS PASSED%s\n' "$GREEN" "$RESET"
    exit 0
else
    printf '%s%d CHAOS ASSERTION(S) FAILED%s\n' "$RED" "$DRILL_FAILURES" "$RESET"
    exit 1
fi
