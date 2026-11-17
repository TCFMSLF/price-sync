# Синхронизация цен: Kafka → Онлайн-каталог

Сервис обработки событий об изменении цен из Apache Kafka (`price-updates`, consumer group `catalog-price-sync`) и идемпотентного обновления онлайн-каталога в PostgreSQL.

## Стек технологий
- **Python 3** (`Decimal` для точной работы с ценами)
- **Apache Kafka** (`confluent-kafka`, топик `price-updates`, 3 партиции, replication factor 1)
- **PostgreSQL 16** (`psycopg`, колонка `price NUMERIC(12, 2)`) через интерфейс `CatalogAdapter`
- **Pydantic** (валидация контракта `contracts/price-changed.schema.json`)
- **Pytest** (автотесты)
- **Docker Compose** (Kafka в режиме KRaft + PostgreSQL)

## Контракт события (`contracts/price-changed.schema.json`)
Ключ сообщения (`key`): `productId` (`SKU-001`, `SKU-002`, `SKU-003`).

```json
{
  "eventId": "550e8400-e29b-41d4-a716-446655440000",
  "eventType": "price.changed",
  "schemaVersion": 1,
  "occurredAt": "2026-11-18T10:15:30.123456Z",
  "correlationId": "660e8400-e29b-41d4-a716-446655440001",
  "source": "erp-sim",
  "payload": {
    "productId": "SKU-001",
    "oldPrice": "1999.00",
    "newPrice": "1799.00",
    "currency": "RUB",
    "priceVersion": 1
  }
}