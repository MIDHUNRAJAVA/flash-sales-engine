#!/usr/bin/env bash
#
# run-local.sh — run the flash-sale engine natively, WITHOUT Docker.
#
# Docker requires sudo in this environment, so this script wires up the whole
# stack from local binaries: two redis-server instances, nats-server (with
# JetStream), a locally-running Postgres, and the three services.
#
# It is idempotent: re-running `up` skips anything already listening, and PIDs
# + logs live under a repo-root `.run/` directory.
#
#   NOTE: add `.run/` to .gitignore — it holds runtime PIDs, logs, the compiled
#         inventory binary, the Python venv, and redis/nats state.
#
# Usage:
#   scripts/run-local.sh up       # start everything
#   scripts/run-local.sh down     # stop everything we started
#   scripts/run-local.sh status   # show which components respond
#
# Observability (OTel Collector, Grafana, Prometheus, Tempo, Loki) is
# Docker-only — use `make up` for that. This script runs just the app stack.

set -uo pipefail

# ---- paths ------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
RUN_DIR="${REPO_ROOT}/.run"

# ---- ports / config ---------------------------------------------------------
REDIS_STOCK_PORT=6390
REDIS_RL_PORT=6391
NATS_PORT=4222
NATS_MON_PORT=8222
INVENTORY_PORT=8080
GATEWAY_PORT=3000
ORDER_METRICS_PORT=9100

# Postgres: assumes a locally running server reachable at 127.0.0.1:5432 with a
# superuser. Override via env if yours differs.
PG_SUPER_HOST="127.0.0.1"
PG_SUPER_PORT=5432
PGSUPER="${PGSUPER:-postgres}"
PGPASSWORD="${PGPASSWORD:-postgres}"
LOCAL_DB="flashsale_local"
APP_DB_USER="${PGUSER:-postgres}"
APP_DB_PASS="${PGPASSWORD:-postgres}"

# ---- pretty output ----------------------------------------------------------
info()  { printf '\033[36m[run-local]\033[0m %s\n' "$*"; }
ok()    { printf '\033[32m[  ok    ]\033[0m %s\n' "$*"; }
warn()  { printf '\033[33m[ warn   ]\033[0m %s\n' "$*"; }
err()   { printf '\033[31m[ error  ]\033[0m %s\n' "$*" >&2; }

have() { command -v "$1" >/dev/null 2>&1; }

# Wait up to N seconds for an HTTP endpoint to return 2xx/3xx.
wait_http() {
	local url="$1" name="$2" tries="${3:-30}" i
	for ((i = 1; i <= tries; i++)); do
		if curl -fsS -o /dev/null "$url" 2>/dev/null; then
			ok "$name is up ($url)"
			return 0
		fi
		sleep 1
	done
	warn "$name did not become healthy at $url after ${tries}s (check logs in .run/)"
	return 1
}

# ---- individual components --------------------------------------------------

start_redis() {
	local port="$1" name="$2"
	shift 2
	local extra_args=("$@")
	local data_dir="${RUN_DIR}/redis-${name}"

	if redis-cli -p "$port" ping >/dev/null 2>&1; then
		ok "redis[$name] already listening on :$port — skipping"
		return 0
	fi
	if ! have redis-server; then
		err "redis-server not found on PATH — cannot start redis[$name]"
		return 1
	fi

	mkdir -p "$data_dir"
	info "starting redis[$name] on :$port (dir $data_dir)"
	redis-server \
		--port "$port" \
		--dir "$data_dir" \
		--daemonize yes \
		--logfile "${RUN_DIR}/redis-${name}.log" \
		"${extra_args[@]}"

	local i
	for ((i = 1; i <= 15; i++)); do
		if redis-cli -p "$port" ping >/dev/null 2>&1; then
			ok "redis[$name] up on :$port"
			return 0
		fi
		sleep 1
	done
	warn "redis[$name] did not answer PING on :$port (see ${RUN_DIR}/redis-${name}.log)"
	return 1
}

start_nats() {
	if curl -fsS -o /dev/null "http://localhost:${NATS_MON_PORT}/healthz" 2>/dev/null; then
		ok "nats already healthy on :${NATS_MON_PORT} — skipping"
		return 0
	fi
	if ! have nats-server; then
		err "nats-server not found on PATH — cannot start NATS"
		return 1
	fi

	mkdir -p "${RUN_DIR}/nats"
	info "starting nats-server (JetStream) on :${NATS_PORT} (mon :${NATS_MON_PORT})"
	nats-server -js -sd "${RUN_DIR}/nats" -p "${NATS_PORT}" -m "${NATS_MON_PORT}" \
		>"${RUN_DIR}/nats.log" 2>&1 &
	echo $! >"${RUN_DIR}/nats.pid"

	wait_http "http://localhost:${NATS_MON_PORT}/healthz" "nats" 15
}

ensure_pg_db() {
	if ! have psql; then
		warn "psql not found — cannot ensure database '${LOCAL_DB}'."
		warn "The app assumes Postgres at ${PG_SUPER_HOST}:${PG_SUPER_PORT}; order-service will fail to connect."
		return 0
	fi

	# Can we even reach the server as the superuser?
	if ! PGPASSWORD="${PGPASSWORD}" psql -h "${PG_SUPER_HOST}" -p "${PG_SUPER_PORT}" \
		-U "${PGSUPER}" -d postgres -tAc 'SELECT 1' >/dev/null 2>&1; then
		warn "Cannot reach Postgres at ${PG_SUPER_HOST}:${PG_SUPER_PORT} as '${PGSUPER}'."
		warn "This script assumes a locally running Postgres with superuser '${PGSUPER}'/'${PGPASSWORD}'."
		warn "Start one (or fix PGSUPER/PGPASSWORD) — order-service will fail to connect until then. Continuing."
		return 0
	fi

	local exists
	exists="$(PGPASSWORD="${PGPASSWORD}" psql -h "${PG_SUPER_HOST}" -p "${PG_SUPER_PORT}" \
		-U "${PGSUPER}" -d postgres -tAc \
		"SELECT 1 FROM pg_database WHERE datname='${LOCAL_DB}'" 2>/dev/null)"

	if [[ "${exists}" == "1" ]]; then
		ok "database '${LOCAL_DB}' already exists"
	else
		info "creating database '${LOCAL_DB}'"
		# Ignore a race where it already exists between the check and create.
		PGPASSWORD="${PGPASSWORD}" psql -h "${PG_SUPER_HOST}" -p "${PG_SUPER_PORT}" \
			-U "${PGSUPER}" -d postgres -c "CREATE DATABASE ${LOCAL_DB}" >/dev/null 2>&1 \
			&& ok "database '${LOCAL_DB}' created" \
			|| warn "CREATE DATABASE ${LOCAL_DB} failed (may already exist) — continuing"
	fi
}

start_inventory() {
	if ! have go; then
		err "go not found on PATH — cannot build inventory-service"
		return 1
	fi
	info "building inventory-service -> ${RUN_DIR}/inventory"
	if ! ( cd "${REPO_ROOT}/inventory-service" && go build -o "${RUN_DIR}/inventory" main.go ); then
		err "inventory-service build failed"
		return 1
	fi

	if curl -fsS -o /dev/null "http://localhost:${INVENTORY_PORT}/health" 2>/dev/null; then
		ok "something already answering :${INVENTORY_PORT}/health — skipping inventory start"
		return 0
	fi

	info "starting inventory-service on :${INVENTORY_PORT}"
	(
		cd "${REPO_ROOT}/inventory-service"
		REDIS_HOST=localhost \
		REDIS_PORT="${REDIS_STOCK_PORT}" \
		NATS_URL="nats://localhost:${NATS_PORT}" \
		PORT="${INVENTORY_PORT}" \
		GIN_MODE=release \
		MAX_PER_USER=2 \
		exec "${RUN_DIR}/inventory"
	) >"${RUN_DIR}/inventory.log" 2>&1 &
	echo $! >"${RUN_DIR}/inventory.pid"

	wait_http "http://localhost:${INVENTORY_PORT}/health" "inventory-service" 30
}

start_order() {
	if ! have python3; then
		err "python3 not found on PATH — cannot run order-service"
		return 1
	fi

	local venv="${RUN_DIR}/venv"
	if [[ ! -x "${venv}/bin/python" ]]; then
		info "creating venv at ${venv}"
		if ! python3 -m venv "${venv}"; then
			err "failed to create venv at ${venv}"
			return 1
		fi
	else
		ok "reusing venv at ${venv}"
	fi

	info "installing order-service requirements (quiet)"
	"${venv}/bin/pip" install --quiet --upgrade pip >/dev/null 2>&1 || true
	if ! "${venv}/bin/pip" install --quiet -r "${REPO_ROOT}/order-service/requirements.txt"; then
		warn "pip install for order-service reported errors — continuing (see output above)"
	fi

	info "starting order-service (metrics :${ORDER_METRICS_PORT})"
	(
		cd "${REPO_ROOT}/order-service"
		DB_HOST=127.0.0.1 \
		DB_PORT="${PG_SUPER_PORT}" \
		DB_NAME="${LOCAL_DB}" \
		DB_USER="${APP_DB_USER}" \
		DB_PASS="${APP_DB_PASS}" \
		REDIS_HOST=localhost \
		REDIS_PORT="${REDIS_STOCK_PORT}" \
		NATS_URL="nats://localhost:${NATS_PORT}" \
		METRICS_PORT="${ORDER_METRICS_PORT}" \
		exec "${venv}/bin/python" main.py
	) >"${RUN_DIR}/order.log" 2>&1 &
	echo $! >"${RUN_DIR}/order.pid"
	ok "order-service started (pid $(cat "${RUN_DIR}/order.pid"))"
}

start_gateway() {
	if ! have node || ! have npm; then
		err "node/npm not found on PATH — cannot run gateway-service"
		return 1
	fi

	if [[ -d "${REPO_ROOT}/gateway-service/node_modules" ]]; then
		ok "gateway node_modules present — skipping npm install"
	else
		info "installing gateway-service deps (npm install)"
		if ! ( cd "${REPO_ROOT}/gateway-service" && npm install --no-fund --no-audit ); then
			err "npm install failed for gateway-service"
			return 1
		fi
	fi

	if curl -fsS -o /dev/null "http://localhost:${GATEWAY_PORT}/health" 2>/dev/null; then
		ok "something already answering :${GATEWAY_PORT}/health — skipping gateway start"
		return 0
	fi

	info "starting gateway-service on :${GATEWAY_PORT}"
	(
		cd "${REPO_ROOT}/gateway-service"
		REDIS_RL_HOST=localhost \
		REDIS_RL_PORT="${REDIS_RL_PORT}" \
		REDIS_QUEUE_HOST=localhost \
		REDIS_QUEUE_PORT="${REDIS_RL_PORT}" \
		INVENTORY_SERVICE_URL="http://localhost:${INVENTORY_PORT}/buy" \
		NATS_URL="nats://localhost:${NATS_PORT}" \
		PORT="${GATEWAY_PORT}" \
		exec node index.js
	) >"${RUN_DIR}/gateway.log" 2>&1 &
	echo $! >"${RUN_DIR}/gateway.pid"

	wait_http "http://localhost:${GATEWAY_PORT}/health" "gateway-service" 30
}

# ---- subcommands ------------------------------------------------------------

cmd_up() {
	mkdir -p "${RUN_DIR}"
	info "runtime dir: ${RUN_DIR} (add '.run/' to .gitignore)"

	start_redis "${REDIS_STOCK_PORT}" stock --appendonly yes
	start_redis "${REDIS_RL_PORT}" ratelimit --maxmemory 256mb --maxmemory-policy noeviction
	start_nats
	ensure_pg_db
	start_inventory
	start_order
	start_gateway

	echo ""
	ok "native stack is up. Endpoints:"
	echo "    gateway         : http://localhost:${GATEWAY_PORT}   (health: /health)"
	echo "    inventory       : http://localhost:${INVENTORY_PORT}   (health: /health, buy: /buy)"
	echo "    order metrics   : http://localhost:${ORDER_METRICS_PORT}/metrics"
	echo "    redis (stock)   : localhost:${REDIS_STOCK_PORT}"
	echo "    redis (ratelim) : localhost:${REDIS_RL_PORT}"
	echo "    nats            : nats://localhost:${NATS_PORT}   (mon: http://localhost:${NATS_MON_PORT})"
	echo ""
	warn "OTel Collector / Grafana / Prometheus / Tempo / Loki are Docker-only (use 'make up')."
	echo "Logs: ${RUN_DIR}/*.log   |   Stop: scripts/run-local.sh down"
}

# Kill exactly the PID recorded in a .pid file. Never pkill -f broad patterns.
kill_pidfile() {
	local name="$1" pidfile="${RUN_DIR}/$1.pid" pid
	[[ -f "${pidfile}" ]] || { info "no pidfile for ${name} — skipping"; return 0; }

	pid="$(cat "${pidfile}" 2>/dev/null)"
	if [[ -z "${pid}" || ! "${pid}" =~ ^[0-9]+$ ]]; then
		warn "pidfile for ${name} has no valid pid — removing"
		rm -f "${pidfile}"
		return 0
	fi

	if ! kill -0 "${pid}" 2>/dev/null; then
		info "${name} (pid ${pid}) not running — cleaning up pidfile"
		rm -f "${pidfile}"
		return 0
	fi

	info "stopping ${name} (pid ${pid})"
	kill "${pid}" 2>/dev/null
	local i
	for ((i = 1; i <= 10; i++)); do
		kill -0 "${pid}" 2>/dev/null || break
		sleep 1
	done
	if kill -0 "${pid}" 2>/dev/null; then
		warn "${name} (pid ${pid}) still alive — sending SIGKILL"
		kill -9 "${pid}" 2>/dev/null
	fi
	rm -f "${pidfile}"
	ok "${name} stopped"
}

shutdown_redis() {
	local port="$1" name="$2"
	if redis-cli -p "${port}" ping >/dev/null 2>&1; then
		info "shutting down redis[${name}] on :${port}"
		redis-cli -p "${port}" shutdown nosave >/dev/null 2>&1 || true
		ok "redis[${name}] stopped"
	else
		info "redis[${name}] on :${port} not running — skipping"
	fi
}

cmd_down() {
	if [[ ! -d "${RUN_DIR}" ]]; then
		warn "no ${RUN_DIR} directory — nothing to stop"
		return 0
	fi

	# Stop services first, then infra.
	kill_pidfile gateway
	kill_pidfile order
	kill_pidfile inventory
	kill_pidfile nats

	shutdown_redis "${REDIS_STOCK_PORT}" stock
	shutdown_redis "${REDIS_RL_PORT}" ratelimit

	ok "native stack stopped."
}

check() {
	local label="$1" cmd="$2"
	if eval "${cmd}" >/dev/null 2>&1; then
		ok "${label}: UP"
	else
		warn "${label}: down"
	fi
}

cmd_status() {
	info "component status:"
	check "redis[stock]   :${REDIS_STOCK_PORT}" "redis-cli -p ${REDIS_STOCK_PORT} ping"
	check "redis[ratelim] :${REDIS_RL_PORT}" "redis-cli -p ${REDIS_RL_PORT} ping"
	check "nats           :${NATS_MON_PORT}" "curl -fsS -o /dev/null http://localhost:${NATS_MON_PORT}/healthz"
	check "inventory      :${INVENTORY_PORT}" "curl -fsS -o /dev/null http://localhost:${INVENTORY_PORT}/health"
	check "gateway        :${GATEWAY_PORT}" "curl -fsS -o /dev/null http://localhost:${GATEWAY_PORT}/health"
	check "order metrics  :${ORDER_METRICS_PORT}" "curl -fsS -o /dev/null http://localhost:${ORDER_METRICS_PORT}/metrics"
}

# ---- dispatch ---------------------------------------------------------------
main() {
	local sub="${1:-}"
	case "${sub}" in
		up)     cmd_up ;;
		down)   cmd_down ;;
		status) cmd_status ;;
		*)
			err "usage: $0 {up|down|status}"
			exit 2
			;;
	esac
}

main "$@"
