# Flash-Sale Engine — developer Makefile
# Two ways to run the stack:
#   * Docker (needs sudo in this env):  make up / make down / make logs
#   * Native (no Docker):               make run-local / make stop-local
#
# Override any variable inline, e.g.:  make seed PID=p2 QTY=500

.DEFAULT_GOAL := help

# ---- tunables ---------------------------------------------------------------
PID      ?= p1
QTY      ?= 2000
SCALE    ?= 0.01

PGHOST     ?= localhost
PGPORT     ?= 5432
PGDATABASE ?= flashsale
PGUSER     ?= flashsale_user
PGPASSWORD ?= change_me

GATEWAY_URL ?= http://localhost:8080

.PHONY: help up down clean logs build seed smoke load audit run-local stop-local

help: ## List available targets
	@echo "Flash-Sale Engine — make targets:"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'
	@echo ""
	@echo "Vars: PID=$(PID) QTY=$(QTY) SCALE=$(SCALE) PGHOST=$(PGHOST) PGDATABASE=$(PGDATABASE)"

# ---- Docker stack -----------------------------------------------------------
up: ## Build & start the full Docker stack (detached)
	@echo ">> docker compose up -d --build"
	docker compose up -d --build

down: ## Stop the Docker stack (keep volumes)
	@echo ">> docker compose down"
	docker compose down

clean: ## Stop the Docker stack and remove volumes + orphans
	@echo ">> docker compose down -v --remove-orphans"
	docker compose down -v --remove-orphans

logs: ## Tail logs from all Docker services
	@echo ">> docker compose logs -f --tail=100"
	docker compose logs -f --tail=100

# ---- Build (native toolchains) ---------------------------------------------
build: ## Install/build all three services locally (Node, Go, Python)
	@echo ">> [gateway] npm install"
	cd gateway-service && npm install
	@echo ">> [inventory] go build ./..."
	cd inventory-service && go build ./...
	@echo ">> [order] pip install -r requirements.txt"
	@echo "   NOTE: order-service wants a virtualenv. 'make run-local' creates .run/venv"
	@echo "         and installs into it. This bare pip install targets the active env."
	cd order-service && pip install -r requirements.txt

# ---- Operations -------------------------------------------------------------
seed: ## Seed inventory: POST /seed (override PID / QTY)
	@echo ">> seeding productID=$(PID) quantity=$(QTY) -> $(GATEWAY_URL)/seed"
	curl -fsS -X POST "$(GATEWAY_URL)/seed" \
		-H 'Content-Type: application/json' \
		-d '{"productID":"$(PID)","quantity":$(QTY)}' \
		&& echo "" || (echo "seed failed"; exit 1)

smoke: ## Run the smoke test (scripts/smoke.sh, authored elsewhere)
	@echo ">> bash scripts/smoke.sh"
	bash scripts/smoke.sh

load: ## Run k6 load test (override SCALE, default 0.01)
	@echo ">> k6 run -e SCALE=$(SCALE) load_test.js"
	k6 run -e SCALE=$(SCALE) load_test.js

audit: ## Run audit queries (scripts/audit.sql) against Postgres
	@echo ">> psql $(PGUSER)@$(PGHOST):$(PGPORT)/$(PGDATABASE) < scripts/audit.sql"
	PGPASSWORD="$(PGPASSWORD)" psql \
		-h "$(PGHOST)" -p "$(PGPORT)" -U "$(PGUSER)" -d "$(PGDATABASE)" \
		-v ON_ERROR_STOP=1 -f scripts/audit.sql

# ---- Native (no-Docker) orchestration --------------------------------------
run-local: ## Start the whole stack natively (no Docker) via scripts/run-local.sh
	@echo ">> bash scripts/run-local.sh up"
	bash scripts/run-local.sh up

stop-local: ## Stop the native stack started by run-local
	@echo ">> bash scripts/run-local.sh down"
	bash scripts/run-local.sh down
