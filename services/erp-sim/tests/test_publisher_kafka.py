import json
import time
from decimal import Decimal
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Producer
from erp_sim.config import settings
from erp_sim.models import OutboxEvent, Product
from erp_sim.publisher import KafkaEventPublisher, publish_pending
from erp_sim.service import change_price
from sqlalchemy import select
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration


def test_publisher_sends_event_to_kafka(
    db_session: Session,
    sample_product: Product,
    kafka_topic: str,
):
    change_price(db_session, "SKU-001", Decimal("1199.00"), 1, uuid4())
    db_session.expire_all()
    event = db_session.scalar(select(OutboxEvent))
    assert event is not None

    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "publisher-test",
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 10000,
        }
    )
    publisher = KafkaEventPublisher(producer, kafka_topic)
    result = publish_pending(db_session, publisher, batch_size=10)
    assert result.published == 1

    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": f"publisher-test-{uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([kafka_topic])
    try:
        message = None
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            candidate = consumer.poll(0.5)
            if candidate is not None and not candidate.error():
                message = candidate
                break
        assert message is not None
        assert message.key() == b"SKU-001"
        assert json.loads(message.value()) == event.payload
        headers = {key: value for key, value in (message.headers() or [])}
        assert headers["eventId"] == str(event.id).encode()
    finally:
        consumer.close()

    db_session.expire_all()
    stored = db_session.get(OutboxEvent, event.id)
    assert stored is not None
    assert stored.published_at is not None
