.PHONY: up down reset logs ps test test-integration lint format seed

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

lint:
	uv run ruff check .

format:
	uv run ruff format .
