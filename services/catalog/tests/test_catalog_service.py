from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from catalog.models import ProcessedEvent, Product
from catalog.schemas import PriceChangedEvent, PriceChangedPayload
from catalog.service import InvalidMessageError, ProductNotFoundError, process_event
from sqlalchemy import func, select
from sqlalchemy.orm import Session

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


def test_process_event_updates_price_and_version(db_session: Session, sample_product: Product):
    event = make_event()

    result = process_event(db_session, event, "SKU-001")

    assert result.event_id == event.eventId
    assert result.status == "applied"
    db_session.expire_all()
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price == Decimal("1199.00")
    assert product.price_version == 2
    processed = db_session.get(ProcessedEvent, event.eventId)
    assert processed is not None
    assert processed.applied is True


def test_process_event_records_latency(db_session: Session, sample_product: Product):
    occurred_at = datetime.now(UTC) - timedelta(seconds=2)
    event = make_event(occurred_at=occurred_at)

    process_event(db_session, event, "SKU-001")

    processed = db_session.get(ProcessedEvent, event.eventId)
    assert processed is not None
    assert 1900 <= processed.latency_ms <= 5000


def test_process_event_clamps_negative_latency(db_session: Session, sample_product: Product):
    event = make_event(occurred_at=datetime.now(UTC) + timedelta(seconds=30))

    process_event(db_session, event, "SKU-001")

    processed = db_session.get(ProcessedEvent, event.eventId)
    assert processed is not None
    assert processed.latency_ms == 0


def test_process_event_is_idempotent_by_event_id(db_session: Session, sample_product: Product):
    event = make_event()

    first = process_event(db_session, event, "SKU-001")
    second = process_event(db_session, event, "SKU-001")

    assert first.status == "applied"
    assert second.status == "duplicate"
    assert db_session.scalar(select(func.count()).select_from(ProcessedEvent)) == 1
    db_session.expire_all()
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price == Decimal("1199.00")
    assert product.price_version == 2


def test_process_event_records_stale_event_without_update(
    db_session: Session, sample_product: Product
):
    event = make_event(price_version=1, new_price="1199.00")

    result = process_event(db_session, event, "SKU-001")

    assert result.status == "stale"
    db_session.expire_all()
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price == Decimal("1299.00")
    assert product.price_version == 1
    processed = db_session.get(ProcessedEvent, event.eventId)
    assert processed is not None
    assert processed.applied is False


def test_process_event_applies_newer_version(db_session: Session, sample_product: Product):
    event = make_event(price_version=3, new_price="999.00")

    result = process_event(db_session, event, "SKU-001")

    assert result.status == "applied"
    db_session.expire_all()
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price_version == 3
    assert product.price == Decimal("999.00")


def test_process_event_rejects_message_key_mismatch(db_session: Session, sample_product: Product):
    event = make_event()

    with pytest.raises(InvalidMessageError):
        process_event(db_session, event, "SKU-002")

    assert db_session.get(ProcessedEvent, event.eventId) is None


def test_process_event_rejects_unknown_product(db_session: Session):
    event = make_event(product_id="SKU-404")

    with pytest.raises(ProductNotFoundError):
        process_event(db_session, event, "SKU-404")

    assert db_session.get(ProcessedEvent, event.eventId) is None
