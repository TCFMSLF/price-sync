from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from erp_sim.models import OutboxEvent, Product
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration


def test_create_product(db_session: Session):
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
    assert loaded.currency == "RUB"
    assert loaded.price_version == 1


def test_product_rejects_negative_price(db_session: Session):
    product = Product(
        id="SKU-002",
        name="Чай черный, 500 г",
        price=Decimal("-1.00"),
        currency="RUB",
    )
    db_session.add(product)

    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_product_rejects_invalid_currency(db_session: Session):
    product = Product(
        id="SKU-003",
        name="Какао, 250 г",
        price=Decimal("499.00"),
        currency="rub",
    )
    db_session.add(product)

    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def test_product_rejects_zero_price_version(db_session: Session):
    product = Product(
        id="SKU-004",
        name="Печенье, 300 г",
        price=Decimal("299.00"),
        currency="RUB",
        price_version=0,
    )
    db_session.add(product)

    with pytest.raises(IntegrityError):
        db_session.commit()
    db_session.rollback()


def make_outbox_event(**overrides) -> OutboxEvent:
    values = {
        "id": uuid4(),
        "aggregate_type": "product",
        "aggregate_id": "SKU-001",
        "event_type": "price.changed",
        "payload": {
            "eventType": "price.changed",
            "payload": {"productId": "SKU-001", "priceVersion": 2},
        },
        "created_at": datetime.now(UTC),
    }
    values.update(overrides)
    return OutboxEvent(**values)


def test_outbox_defaults(db_session: Session):
    event = make_outbox_event()
    db_session.add(event)
    db_session.commit()
    db_session.expire_all()

    loaded = db_session.get(OutboxEvent, event.id)
    assert loaded is not None
    assert loaded.published_at is None
    assert loaded.attempts == 0
    assert loaded.last_error is None


def test_outbox_payload_roundtrip(db_session: Session):
    payload = {
        "eventId": "8f14e45f-ea8b-4c1d-9f2a-6b7c8d9e0f12",
        "payload": {"productId": "SKU-001", "priceVersion": 2},
    }
    event = make_outbox_event(payload=payload)
    db_session.add(event)
    db_session.commit()
    db_session.expire_all()

    loaded = db_session.get(OutboxEvent, event.id)
    assert loaded is not None
    assert loaded.payload == payload


def test_unpublished_outbox_query(db_session: Session):
    now = datetime.now(UTC)
    unpublished = make_outbox_event(created_at=now)
    published = make_outbox_event(created_at=now, published_at=now)
    db_session.add_all([unpublished, published])
    db_session.commit()
    db_session.expire_all()

    result = db_session.scalars(select(OutboxEvent).where(OutboxEvent.published_at.is_(None))).all()
    assert [event.id for event in result] == [unpublished.id]
