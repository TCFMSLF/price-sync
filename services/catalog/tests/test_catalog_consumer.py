import json
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from catalog import consumer as consumer_module
from catalog.consumer import IncomingMessage, process_message
from catalog.models import FailedEvent, ProcessedEvent
from catalog.schemas import PriceChangedEvent, PriceChangedPayload
from catalog.service import ProductNotFoundError
from catalog.service import process_event as real_process_event
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

pytestmark = pytest.mark.integration


def make_event(
    product_id: str = "SKU-001",
    price_version: int = 2,
    new_price: str = "1199.00",
    occurred_at: datetime | None = None,
) -> PriceChangedEvent:
    return PriceChangedEvent(
        eventId=uuid4(),
        eventType="price.changed",
        schemaVersion=1,
        occurredAt=occurred_at or datetime.now(UTC),
        correlationId=uuid4(),
        source="test",
        payload=PriceChangedPayload(
            productId=product_id,
            priceVersion=price_version,
            oldPrice="1299.00",
            newPrice=new_price,
            currency="RUB",
        ),
    )


def make_incoming(
    event: PriceChangedEvent,
    key: str | None = None,
    offset: int = 1,
) -> IncomingMessage:
    return IncomingMessage(
        topic="price-updates",
        partition=0,
        kafka_offset=offset,
        key=key if key is not None else event.payload.productId,
        value=event.model_dump_json(),
    )


def test_process_message_applies_event(session_factory: sessionmaker, sample_product):
    result = process_message(
        session_factory, make_incoming(make_event()), max_attempts=3, retry_delay=0
    )

    assert result.status == "applied"
    assert result.event_id is not None


def test_process_message_is_idempotent(session_factory: sessionmaker, sample_product):
    incoming = make_incoming(make_event())

    first = process_message(session_factory, incoming, max_attempts=3, retry_delay=0)
    second = process_message(session_factory, incoming, max_attempts=3, retry_delay=0)

    assert first.status == "applied"
    assert second.status == "duplicate"


def test_process_message_records_stale_event(
    session_factory: sessionmaker, sample_product, db_session: Session
):
    incoming = make_incoming(make_event(price_version=1, new_price="1199.00"))

    result = process_message(session_factory, incoming, max_attempts=3, retry_delay=0)

    assert result.status == "stale"
    processed = db_session.scalar(select(ProcessedEvent))
    assert processed is not None
    assert processed.applied is False


def test_process_message_records_invalid_json(
    session_factory: sessionmaker, sample_product, db_session: Session
):
    incoming = IncomingMessage(
        topic="price-updates",
        partition=0,
        kafka_offset=5,
        key="SKU-001",
        value="{not json",
    )

    result = process_message(session_factory, incoming, max_attempts=3, retry_delay=0)

    assert result.status == "failed"
    assert result.event_id is None
    failed = db_session.scalar(select(FailedEvent))
    assert failed is not None
    assert failed.event_id is None
    assert failed.raw_value == "{not json"
    assert failed.attempts == 1
    assert failed.kafka_offset == 5


def test_process_message_records_invalid_contract(
    session_factory: sessionmaker, sample_product, db_session: Session
):
    incoming = IncomingMessage(
        topic="price-updates",
        partition=0,
        kafka_offset=6,
        key="SKU-001",
        value=json.dumps({"eventType": "price.changed"}),
    )

    result = process_message(session_factory, incoming, max_attempts=3, retry_delay=0)

    assert result.status == "failed"
    assert result.event_id is None
    failed = db_session.scalar(select(FailedEvent))
    assert failed is not None
    assert failed.payload is None
    assert failed.attempts == 1


def test_process_message_retries_transient_error_then_succeeds(
    session_factory: sessionmaker, sample_product, monkeypatch
):
    calls = []

    def flaky(session, event, message_key):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("db temporarily unavailable")
        return real_process_event(session, event, message_key)

    monkeypatch.setattr(consumer_module, "process_event", flaky)

    result = process_message(
        session_factory, make_incoming(make_event()), max_attempts=3, retry_delay=0
    )

    assert result.status == "applied"
    assert len(calls) == 2


def test_process_message_records_failed_event_after_retries(
    session_factory: sessionmaker, sample_product, db_session: Session, monkeypatch
):
    calls = []

    def always_fails(session, event, message_key):
        calls.append(1)
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(consumer_module, "process_event", always_fails)

    result = process_message(
        session_factory, make_incoming(make_event()), max_attempts=3, retry_delay=0
    )

    assert result.status == "failed"
    assert len(calls) == 3
    failed = db_session.scalar(select(FailedEvent))
    assert failed is not None
    assert failed.attempts == 3
    assert "db unavailable" in failed.error


def test_process_message_does_not_retry_permanent_error(
    session_factory: sessionmaker, sample_product, db_session: Session, monkeypatch
):
    calls = []

    def not_found(session, event, message_key):
        calls.append(1)
        raise ProductNotFoundError(event.payload.productId)

    monkeypatch.setattr(consumer_module, "process_event", not_found)

    result = process_message(
        session_factory, make_incoming(make_event()), max_attempts=3, retry_delay=0
    )

    assert result.status == "failed"
    assert len(calls) == 1
    failed = db_session.scalar(select(FailedEvent))
    assert failed is not None
    assert failed.attempts == 1


def test_process_message_does_not_duplicate_failed_position(
    session_factory: sessionmaker, sample_product, db_session: Session
):
    incoming = IncomingMessage(
        topic="price-updates",
        partition=0,
        kafka_offset=9,
        key="SKU-001",
        value="{not json",
    )

    process_message(session_factory, incoming, max_attempts=3, retry_delay=0)
    process_message(session_factory, incoming, max_attempts=3, retry_delay=0)

    count = db_session.scalar(select(func.count()).select_from(FailedEvent))
    assert count == 1


def test_process_message_records_latency(
    session_factory: sessionmaker, sample_product, db_session: Session
):
    event = make_event(occurred_at=datetime.now(UTC) - timedelta(seconds=2))

    process_message(session_factory, make_incoming(event), max_attempts=3, retry_delay=0)

    processed = db_session.get(ProcessedEvent, event.eventId)
    assert processed is not None
    assert 1900 <= processed.latency_ms <= 5000
