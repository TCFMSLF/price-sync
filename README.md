# Price Sync

Механизм «Триггер на изменение цен в ERP»: изменение цены в ERP публикуется событием в Kafka-топик price-updates, consumer обновляет цену в онлайн-каталоге.

## Цель

Обеспечить синхронность цен между ERP и e-commerce и снизить риск рассинхронов.

## Ожидаемый результат

- события не теряются: outbox, at-least-once, идемпотентный consumer;
- end-to-end latency p95 около 85 мс на локальном демо;
- цена в каталоге обновляется в реальном времени

Проверка задержки: `uv run python scripts/demo.py measure --count 30`.

## Запуск

```bash
make install
make demo-reset
```

В отдельных терминалах:

```bash
make erp-api
make erp-publisher
make catalog-api
make catalog-consumer
```

ERP API: http://localhost:8000, каталог: http://localhost:8001,
Kafka UI: http://localhost:8080, PostgreSQL: localhost:5433.

Повторный запуск с чистого состояния: `make demo-reset`. Команда удаляет
тома PostgreSQL и Kafka, затем заново применяет seed.


