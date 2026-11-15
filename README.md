# Синхронизация цен: Kafka → Онлайн-каталог

Сервис обработки событий об изменении цен из Apache Kafka (`price-updates`) и идемпотентного обновления онлайн-каталога.

## Стек технологий
- **Python 3**
- **Apache Kafka** (`confluent-kafka`)
- **SQLite** (через интерфейс `CatalogAdapter`, легко заменяется на PostgreSQL / Redis)
- **Pydantic** (валидация контракта событий)
- **Pytest** (автотесты)
- **Docker Compose** (локальный брокер Kafka в режиме KRaft)

## Схема события (контракт топика `price-updates`)
Ключ сообщения (`key`): `product_id`.

```json
{
  "event_id": "550e8400-e29b-41d4-a716-446655440000",
  "event_type": "PriceUpdated",
  "event_version": 1,
  "occurred_at": "2026-11-18T10:15:30.123456+00:00",
  "payload": {
    "product_id": "SM-10045",
    "old_price": 4999.0,
    "new_price": 4299.0,
    "currency": "RUB"
  }
}