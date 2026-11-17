import json
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest
from erp_sim.models import OutboxEvent, Product
from erp_sim.schemas import PriceChangedEvent
from erp_sim.service import (
    PriceUnchangedError,
    PriceVersionConflictError,
    ProductNotFoundError,
    change_price,
)
from jsonschema import validate
from sqlalchemy import func, select
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = ROOT / "contracts" / "price-changed.schema.json"


def test_change_price_updates_product_and_creates_outbox(
    db_session: Session, sample_product: Product
):
    correlation_id = uuid4()

    result = change_price(db_session, "SKU-001", Decimal("1199.00"), 1, correlation_id)

    db_session.expire_all()
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price == Decimal("1199.00")
    assert product.price_version == 2

    outbox = db_session.scalar(select(OutboxEvent))
    assert outbox is not None
    assert outbox.id == result.event_id
    assert outbox.aggregate_type == "product"
    assert outbox.aggregate_id == "SKU-001"
    assert outbox.event_type == "price.changed"
    assert outbox.created_at == result.occurred_at
    assert outbox.published_at is None
    assert outbox.attempts == 0

    event = PriceChangedEvent.model_validate(outbox.payload)
    assert event.payload.oldPrice == Decimal("1299.00")
    assert event.payload.newPrice == Decimal("1199.00")
    assert event.payload.priceVersion == 2
    assert event.correlationId == correlation_id
    assert outbox.payload["payload"]["oldPrice"] == "1299.00"
    assert outbox.payload["payload"]["newPrice"] == "1199.00"

    schema = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    validate(instance=outbox.payload, schema=schema)


def test_change_price_rejects_unknown_product(db_session: Session):
    with pytest.raises(ProductNotFoundError):
        change_price(db_session, "SKU-404", Decimal("1199.00"), 1, uuid4())

    total = db_session.scalar(select(func.count()).select_from(OutboxEvent))
    assert total == 0


def test_change_price_rejects_stale_version(db_session: Session, sample_product: Product):
    with pytest.raises(PriceVersionConflictError) as error:
        change_price(db_session, "SKU-001", Decimal("1199.00"), 0, uuid4())

    assert error.value.current_version == 1

    db_session.rollback()
    db_session.expire_all()
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price == Decimal("1299.00")
    assert product.price_version == 1
    assert db_session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


def test_change_price_rejects_same_price(db_session: Session, sample_product: Product):
    with pytest.raises(PriceUnchangedError):
        change_price(db_session, "SKU-001", Decimal("1299.00"), 1, uuid4())

    db_session.rollback()
    db_session.expire_all()
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price_version == 1
    assert db_session.scalar(select(func.count()).select_from(OutboxEvent)) == 0


def test_second_change_with_same_version_is_rejected(db_session: Session, sample_product: Product):
    change_price(db_session, "SKU-001", Decimal("1199.00"), 1, uuid4())

    with pytest.raises(PriceVersionConflictError):
        change_price(db_session, "SKU-001", Decimal("1099.00"), 1, uuid4())

    assert db_session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def test_commit_failure_rolls_back(db_session: Session, sample_product: Product, monkeypatch):
    def fail_commit() -> None:
        raise RuntimeError("db commit failed")

    monkeypatch.setattr(db_session, "commit", fail_commit)

    with pytest.raises(RuntimeError):
        change_price(db_session, "SKU-001", Decimal("1199.00"), 1, uuid4())

    monkeypatch.undo()
    db_session.expire_all()
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price == Decimal("1299.00")
    assert product.price_version == 1
    assert db_session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
