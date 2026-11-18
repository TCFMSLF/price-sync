.PHONY: up down reset logs ps test test-integration lint format seed erp-api erp-publisher erp-publisher-once catalog-seed catalog-consumer catalog-api

up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v

logs:
	docker compose logs -f

ps:
	docker compose ps

test:
	uv run pytest -q

test-integration:
	RUN_INTEGRATION=1 uv run pytest -q -m integration

seed:
	uv run --package erp-sim python -m erp_sim.seed

erp-api:
	uv run --package erp-sim uvicorn erp_sim.api:app --reload --port 8000

erp-publisher:
	uv run --package erp-sim python -m erp_sim.publisher

erp-publisher-once:
	uv run --package erp-sim python -m erp_sim.publisher --once

catalog-seed:
	uv run --package catalog python -m catalog.seed

catalog-consumer:
	uv run --package catalog python -m catalog.consumer

catalog-api:
	uv run --package catalog uvicorn catalog.api:app --reload --port 8001

lint:
	uv run ruff check .

format:
	uv run ruff format .
