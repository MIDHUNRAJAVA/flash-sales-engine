#!/usr/bin/env bash
#
# smoke.sh — fast functional matrix for the flash-sale engine.
#
# Runs against a RUNNING stack (docker compose or local native processes).
# Curl-based integration test: seeds a fresh product, then asserts the core
# behaviours of the buy path. Prints PASS/FAIL per check and exits non-zero
# if any check fails.
#
# Config via env (defaults match docker-compose service exposure):
#   GATEWAY_URL       (default http://localhost:3000)
#   INVENTORY_URL     (default http://localhost:8080)
#   REDIS_STOCK_PORT  (default 6379; local native tests use 6390)
#   PRODUCT_ID        (default p1)
#   SALE_STOCK        (default 2000)   -- NOTE: this smoke seeds a SMALL stock
#                                          derived per-check so drain is cheap.
set -euo pipefail

GATEWAY_URL="${GATEWAY_URL:-http://localhost:3000}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:8080}"
REDIS_STOCK_HOST="${REDIS_STOCK_HOST:-localhost}"
REDIS_STOCK_PORT="${REDIS_STOCK_PORT:-6379}"
PRODUCT_ID="${PRODUCT_ID:-p1}"
MAX_PER_USER=2

# Use a distinct product id per run so a re-run starts from a clean quota/idem
# state without needing a flush. Falls back to PRODUCT_ID if desired.
PID="${PRODUCT_ID}-smoke-$$"

# ---- helpers ---------------------------------------------------------------

HAVE_JQ=0
if command -v jq >/dev/null 2>&1; then HAVE_JQ=1; fi

RED=$'\033[31m'; GREEN=$'\033[32m'; BOLD=$'\033[1m'; RESET=$'\033[0m'
FAILURES=0

pass() { printf '  %s✓%s %s\n' "$GREEN" "$RESET" "$1"; }
fail() { printf '  %s✗ %s%s\n' "$RED" "$1" "$RESET"; FAILURES=$((FAILURES+1)); }
hdr()  { printf '\n%s== %s ==%s\n' "$BOLD" "$1" "$RESET"; }

redis_stock() {
    redis-cli -h "$REDIS_STOCK_HOST" -p "$REDIS_STOCK_PORT" \
        GET "{sale:${PID}}:stock" 2>/dev/null | tr -d '[:space:]'
}

# json_field <json> <field>  -> extracts a scalar field; jq if present else grep.
json_field() {
    local json="$1" field="$2"
    if [ "$HAVE_JQ" -eq 1 ]; then
        printf '%s' "$json" | jq -r --arg f "$field" '.[$f] // empty' 2>/dev/null
    else
        # crude: match "field":"value" or "field":value
        printf '%s' "$json" \
            | grep -oE "\"$field\"[[:space:]]*:[[:space:]]*(\"[^\"]*\"|[^,}]*)" \
            | head -n1 \
            | sed -E "s/\"$field\"[[:space:]]*:[[:space:]]*//; s/^\"//; s/\"$//"
    fi
}

# buy <gateway|inventory> <user> <nonce>  -> sets globals HTTP_CODE, BODY
buy() {
    local target="$1" user="$2" nonce="$3" base
    if [ "$target" = "gateway" ]; then base="$GATEWAY_URL"; else base="$INVENTORY_URL"; fi
    local resp
    resp=$(curl -s -w '\n%{http_code}' -X POST "$base/buy" \
        -H 'Content-Type: application/json' \
        -d "{\"userID\":\"$user\",\"productID\":\"$PID\",\"quantity\":1,\"clientNonce\":\"$nonce\"}")
    HTTP_CODE="${resp##*$'\n'}"
    BODY="${resp%$'\n'*}"
}

seed() {
    local qty="$1"
    curl -s -X POST "$INVENTORY_URL/seed" \
        -H 'Content-Type: application/json' \
        -d "{\"productID\":\"$PID\",\"quantity\":$qty}" >/dev/null
}

# ---- preflight -------------------------------------------------------------

if ! command -v redis-cli >/dev/null 2>&1; then
    echo "redis-cli not found on PATH; required for stock assertions." >&2
    exit 2
fi

printf '%sflash-sale smoke test%s\n' "$BOLD" "$RESET"
echo "  gateway=$GATEWAY_URL inventory=$INVENTORY_URL"
echo "  redis=$REDIS_STOCK_HOST:$REDIS_STOCK_PORT product=$PID"

# ---- (a) a buy returns 200 with an order_id --------------------------------
# Seed enough that the quota/rate-limit checks below have headroom.
hdr "(a) happy-path buy returns 200 + order_id"
seed 10
sleep 0.2
STOCK_BEFORE=$(redis_stock)

buy gateway "u_alice" "nonce-A1"
ORDER_ID_A=$(json_field "$BODY" "order_id")
if [ "$HTTP_CODE" = "200" ] && [ -n "$ORDER_ID_A" ]; then
    pass "buy -> 200, order_id=$ORDER_ID_A"
else
    fail "expected 200 + order_id, got HTTP=$HTTP_CODE body=$BODY"
fi

STOCK_AFTER_A=$(redis_stock)
if [ "$STOCK_AFTER_A" = "$((STOCK_BEFORE - 1))" ]; then
    pass "stock decremented by 1 ($STOCK_BEFORE -> $STOCK_AFTER_A)"
else
    fail "stock expected $((STOCK_BEFORE - 1)), got $STOCK_AFTER_A"
fi

# ---- (b) replaying same clientNonce -> duplicate:true, same order_id, no decrement
hdr "(b) idempotent replay of same clientNonce"
STOCK_BEFORE_B=$(redis_stock)
buy gateway "u_alice" "nonce-A1"
DUP=$(json_field "$BODY" "duplicate")
ORDER_ID_B=$(json_field "$BODY" "order_id")

if [ "$HTTP_CODE" = "200" ] && [ "$DUP" = "true" ]; then
    pass "replay -> 200 duplicate:true"
else
    fail "expected 200 duplicate:true, got HTTP=$HTTP_CODE body=$BODY"
fi
if [ -n "$ORDER_ID_A" ] && [ "$ORDER_ID_B" = "$ORDER_ID_A" ]; then
    pass "same order_id returned ($ORDER_ID_B)"
else
    fail "expected same order_id=$ORDER_ID_A, got $ORDER_ID_B"
fi
STOCK_AFTER_B=$(redis_stock)
if [ "$STOCK_AFTER_B" = "$STOCK_BEFORE_B" ]; then
    pass "stock NOT decremented on replay ($STOCK_AFTER_B)"
else
    fail "stock changed on replay: $STOCK_BEFORE_B -> $STOCK_AFTER_B"
fi

# ---- (c) same user, distinct 3rd nonce -> 409 quota_exceeded ---------------
# alice already owns 1 unit (nonce-A1). A 2nd distinct nonce reaches the quota
# limit (MAX_PER_USER=2); a 3rd distinct nonce must be rejected with 409.
hdr "(c) per-user quota: 3rd distinct nonce -> 409"
# 2nd purchase (should succeed, brings alice to quota=2). Space it out so the
# gateway rate limiter does not intercept this legitimate call.
sleep 1.2
buy gateway "u_alice" "nonce-A2"
if [ "$HTTP_CODE" = "200" ]; then
    pass "2nd distinct purchase -> 200 (alice now at quota=$MAX_PER_USER)"
else
    fail "2nd purchase expected 200, got HTTP=$HTTP_CODE body=$BODY"
fi
sleep 1.2
buy gateway "u_alice" "nonce-A3"
REASON=$(json_field "$BODY" "reason")
if [ "$HTTP_CODE" = "409" ]; then
    pass "3rd distinct nonce -> 409 (${REASON:-quota_exceeded})"
else
    fail "expected 409 quota_exceeded, got HTTP=$HTTP_CODE body=$BODY"
fi

# ---- (d) gateway rate-limit: rapid second buy for same user -> 429 ---------
# Fire two buys back-to-back with no delay; the gateway token bucket should
# reject the second. Use a fresh user so quota is not the cause of rejection.
hdr "(d) gateway rate-limit: rapid 2nd buy -> 429"
buy gateway "u_bob" "nonce-B1" &
BUY1_PID=$!
buy gateway "u_bob" "nonce-B2"
RL_CODE_2="$HTTP_CODE"
wait "$BUY1_PID" 2>/dev/null || true
# The immediate second call in this shell captured RL_CODE_2. To be robust
# regardless of scheduling, do an explicit rapid pair in-shell:
buy gateway "u_carol" "nonce-C1"; FIRST="$HTTP_CODE"
buy gateway "u_carol" "nonce-C2"; SECOND="$HTTP_CODE"
if [ "$SECOND" = "429" ] || [ "$RL_CODE_2" = "429" ]; then
    pass "rapid second buy rate-limited -> 429 (first=$FIRST)"
else
    fail "expected a 429 on rapid second buy, got first=$FIRST second=$SECOND (bob2=$RL_CODE_2)"
fi

# ---- (e) drain remaining stock -> subsequent buy 410 (out of stock) --------
hdr "(e) drain to zero -> out-of-stock 410"
# Reseed a small, exactly-drainable stock on a fresh sub-product to isolate.
DRAIN_QTY=5
seed "$DRAIN_QTY"
sleep 0.2
# Distinct users so per-user quota never blocks the drain; go direct to
# inventory to bypass the gateway rate limiter.
drained=0
for i in $(seq 1 "$DRAIN_QTY"); do
    buy inventory "u_drain_$i" "nonce-D$i"
    if [ "$HTTP_CODE" = "200" ]; then drained=$((drained+1)); fi
done
STOCK_DRAINED=$(redis_stock)
if [ "$drained" = "$DRAIN_QTY" ] && [ "$STOCK_DRAINED" = "0" ]; then
    pass "drained all $DRAIN_QTY units, stock=0"
else
    fail "drain incomplete: succeeded=$drained stock=$STOCK_DRAINED"
fi

buy inventory "u_drain_last" "nonce-D-last"
if [ "$HTTP_CODE" = "410" ]; then
    pass "buy after sold-out -> 410 out_of_stock"
else
    fail "expected 410 out_of_stock, got HTTP=$HTTP_CODE body=$BODY"
fi

# ---- summary ---------------------------------------------------------------
hdr "summary"
if [ "$FAILURES" -eq 0 ]; then
    printf '%sALL CHECKS PASSED%s\n' "$GREEN" "$RESET"
    exit 0
else
    printf '%s%d CHECK(S) FAILED%s\n' "$RED" "$FAILURES" "$RESET"
    exit 1
fi
