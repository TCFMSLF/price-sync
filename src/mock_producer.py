import json
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer


def send_test_event(
    product_id: str = "SKU-001",
    old_price: str = "1999.00",
    new_price: str = "1799.00",
    price_version: int = 1,
    duplicate_event_id: str | None = None,
) -> str:
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    event_id = duplicate_event_id or str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())

    event = {
        "eventId": event_id,
        "eventType": "price.changed",
        "schemaVersion": 1,
        "occurredAt": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "correlationId": correlation_id,
        "source": "erp-sim",
        "payload": {
            "productId": product_id,
            "oldPrice": old_price,
            "newPrice": new_price,
            "currency": "RUB",
            "priceVersion": price_version,
        },
    }

    producer.produce(
        topic="price-updates",
        key=product_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
    )
    producer.flush()
    print(
        f"Отправлено: eventId={event_id}, productId={product_id}, "
        f"newPrice={new_price}, priceVersion={price_version}"
    )
    return event_id


if __name__ == "__main__":
    # Отправляем обновления по 3 демо-товарам (попадут в 3 партиции по ключу productId)
    eid1 = send_test_event("SKU-001", old_price="1999.00", new_price="1799.00", price_version=1)
    send_test_event("SKU-002", old_price="3490.00", new_price="2990.00", price_version=1)
    send_test_event("SKU-003", old_price="5990.00", new_price="5490.00", price_version=1)
    # И дубликат первого события для проверки дедупликации
    send_test_event("SKU-001", old_price="1999.00", new_price="1799.00", price_version=1, duplicate_event_id=eid1)