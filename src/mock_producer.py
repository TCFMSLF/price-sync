import json
import uuid
from datetime import datetime, timezone
from confluent_kafka import Producer


def send_test_event(
    product_id: str = "SM-10045",
    old_price: float = 4999.0,
    new_price: float = 4299.0,
    duplicate_id: str | None = None,
) -> str:
    producer = Producer({"bootstrap.servers": "localhost:9092"})
    event_id = duplicate_id or str(uuid.uuid4())

    event = {
        "event_id": event_id,
        "event_type": "PriceUpdated",
        "event_version": 1,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "payload": {
            "product_id": product_id,
            "old_price": old_price,
            "new_price": new_price,
            "currency": "RUB",
        },
    }

    producer.produce(
        topic="price-updates",
        key=product_id.encode("utf-8"),
        value=json.dumps(event).encode("utf-8"),
    )
    producer.flush()
    print(f"Отправлено событие: event_id={event_id}, product={product_id}, new_price={new_price}")
    return event_id


if __name__ == "__main__":
    eid = send_test_event(product_id="SM-10045", old_price=4999.0, new_price=4299.0)
    send_test_event(product_id="SM-10045", old_price=4999.0, new_price=4299.0, duplicate_id=eid)