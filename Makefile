.PHONY: up down reset logs ps test lint format

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

lint:
	uv run ruff check .

format:
	uv run ruff format .
