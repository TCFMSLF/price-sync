import time
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from catalog.consumer import consume_once
from catalog.models import ProcessedEvent, Product
from catalog.schemas import PriceChangedEvent, PriceChangedPayload
from confluent_kafka import Consumer, Producer, TopicPartition
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

pytestmark = pytest.mark.integration


def make_event(price_version: int = 2, new_price: str = "1199.00") -> PriceChangedEvent:
    return PriceChangedEvent(
        eventId=uuid4(),
        eventType="price.changed",
        schemaVersion=1,
        occurredAt=datetime.now(UTC),
        correlationId=uuid4(),
        source="test",
        payload=PriceChangedPayload(
            productId="SKU-001",
            priceVersion=price_version,
            oldPrice="1299.00",
            newPrice=new_price,
            currency="RUB",
        ),
    )


def publish(topic: str, event: PriceChangedEvent) -> None:
    producer = Producer(
        {
            "bootstrap.servers": "localhost:9092",
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 10000,
        }
    )
    producer.produce(topic=topic, key="SKU-001", value=event.model_dump_json())
    producer.flush(10)


def make_consumer(topic: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"catalog-test-{uuid4()}",
            "client.id": "catalog-test",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    consumer.subscribe([topic])
    return consumer


def consume_until_result(consumer, session_factory):
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        result = consume_once(consumer, session_factory, timeout=1.0, max_attempts=1, retry_delay=0)
        if result is not None:
            return result
    return None


def test_consumer_reads_event_from_kafka(
    session_factory: sessionmaker,
    db_session: Session,
    sample_product: Product,
    kafka_topic: str,
):
    event = make_event()
    publish(kafka_topic, event)
    consumer = make_consumer(kafka_topic)
    try:
        result = consume_until_result(consumer, session_factory)
        assert result is not None
        assert result.status == "applied"

        db_session.expire_all()
        product = db_session.get(Product, "SKU-001")
        assert product is not None
        assert product.price == Decimal("1199.00")
        assert product.price_version == 2
        processed = db_session.get(ProcessedEvent, event.eventId)
        assert processed is not None

        committed = consumer.committed([TopicPartition(kafka_topic, 0)], timeout=10)[0]
        assert committed.offset == 1
    finally:
        consumer.close()


def test_consumer_deduplicates_repeated_event(
    session_factory: sessionmaker,
    db_session: Session,
    sample_product: Product,
    kafka_topic: str,
):
    event = make_event()
    publish(kafka_topic, event)
    publish(kafka_topic, event)
    consumer = make_consumer(kafka_topic)
    try:
        first = consume_until_result(consumer, session_factory)
        second = consume_until_result(consumer, session_factory)

        assert first is not None and first.status == "applied"
        assert second is not None and second.status == "duplicate"
        count = db_session.scalar(select(func.count()).select_from(ProcessedEvent))
        assert count == 1
        db_session.expire_all()
        product = db_session.get(Product, "SKU-001")
        assert product is not None
        assert product.price == Decimal("1199.00")
    finally:
        consumer.close()
