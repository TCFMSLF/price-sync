# Price Sync

Механизм «Триггер на изменение цен в ERP»: изменение цены в ERP публикуется событием в Kafka-топик price-updates, consumer обновляет цену в онлайн-каталоге.

## Цель

Обеспечить синхронность цен между ERP и e-commerce и снизить риск рассинхронов.

## Ожидаемый результат

- события не теряются: outbox, at-least-once, идемпотентный consumer;
- end-to-end latency p95 около 200 мс на локальном демо;
- цена в каталоге обновляется в реальном времени.

## Стек

- Python 3.12;
- FastAPI;
- PostgreSQL 16;
- Apache Kafka 3.x в режиме KRaft;
- Docker Compose;
- uv, pytest, ruff.

## Быстрый старт

```bash
uv python install 3.12
uv sync --all-packages
uv run pytest -q
```

Локальное окружение Kafka и PostgreSQL добавляется следующим коммитом.

## Структура

```text
contracts/     схема события и примеры
services/      erp-sim и catalog
```

## Контракт события

Схема: `contracts/price-changed.schema.json`.

Ключ сообщения: `productId`. Ключ дедупликации: `eventId`.

## Команда

| Участник | Зона ответственности |
|---|---|
| Балде | ERP-имитация, outbox, публикация в Kafka |
| Зверева | Consumer, дедупликация, обновление каталога |
