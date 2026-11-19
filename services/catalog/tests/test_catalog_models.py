from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from catalog.models import FailedEvent, ProcessedEvent, Product
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration


def test_product_defaults(db_session: Session):
    product = Product(
        id="SKU-001",
        name="Кофе в зернах, 1 кг",
        price=Decimal("1299.00"),
        currency="RUB",
    )
    db_session.add(product)
    db_session.commit()
    db_session.expire_all()

    loaded = db_session.get(Product, "SKU-001")
    assert loaded is not None
    assert loaded.price == Decimal("1299.00")
    assert loaded.price_version == 1
    assert loaded.updated_at is not None


def test_product_rejects_negative_price(db_session: Session):
    product = Product(id="SKU-002", name="Чай", price=Decimal("-1.00"), currency="RUB")
    db_session.add(product)

    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_product_rejects_invalid_currency(db_session: Session):
    product = Product(id="SKU-003", name="Какао", price=Decimal("499.00"), currency="rub")
    db_session.add(product)

    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_processed_event_roundtrip(db_session: Session):
    event_id = uuid4()
    now = datetime.now(UTC)
    db_session.add(
        ProcessedEvent(
            event_id=event_id,
            product_id="SKU-001",
            price_version=2,
            occurred_at=now,
            processed_at=now,
            latency_ms=12,
            applied=True,
        )
    )
    db_session.commit()
    db_session.expire_all()

    loaded = db_session.get(ProcessedEvent, event_id)
    assert loaded is not None
    assert loaded.product_id == "SKU-001"
    assert loaded.price_version == 2
    assert loaded.latency_ms == 12
    assert loaded.applied is True


def test_failed_event_roundtrip(db_session: Session):
    now = datetime.now(UTC)
    db_session.add(
        FailedEvent(
            event_id=uuid4(),
            product_id="SKU-001",
            payload={"eventType": "price.changed"},
            raw_value='{"eventType": "price.changed"}',
            error="invalid contract",
            attempts=1,
            topic="price-updates",
            partition_no=0,
            kafka_offset=7,
            failed_at=now,
        )
    )
    db_session.commit()
    db_session.expire_all()

    loaded = db_session.query(FailedEvent).one()
    assert loaded.error == "invalid contract"
    assert loaded.partition_no == 0
    assert loaded.kafka_offset == 7


def test_failed_event_position_is_unique(db_session: Session):
    now = datetime.now(UTC)

    def add_failed() -> None:
        db_session.add(
            FailedEvent(
                event_id=None,
                product_id=None,
                payload=None,
                raw_value="broken",
                error="invalid json",
                attempts=1,
                topic="price-updates",
                partition_no=0,
                kafka_offset=3,
                failed_at=now,
            )
        )

    add_failed()
    db_session.commit()
    add_failed()

    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()
