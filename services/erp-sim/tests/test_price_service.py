import json
import threading
import time
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
from sqlalchemy import func, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

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
    assert outbox.payload["eventId"] == str(outbox.id)
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


def test_change_price_refreshes_product_loaded_in_same_session(
    session_factory: sessionmaker, sample_product: Product
):
    with session_factory() as stale_session, session_factory() as other_session:
        stale_product = stale_session.get(Product, "SKU-001")
        assert stale_product is not None
        assert stale_product.price_version == 1

        change_price(other_session, "SKU-001", Decimal("1199.00"), 1, uuid4())

        with pytest.raises(PriceVersionConflictError) as error:
            change_price(stale_session, "SKU-001", Decimal("1099.00"), 1, uuid4())

        assert error.value.current_version == 2

    with session_factory() as check_session:
        product = check_session.get(Product, "SKU-001")
        assert product is not None
        assert product.price == Decimal("1199.00")
        assert product.price_version == 2
        assert check_session.scalar(select(func.count()).select_from(OutboxEvent)) == 1


def _wait_for_row_lock_wait(engine: Engine, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with engine.connect() as connection:
            waiting = connection.scalar(
                text(
                    "select count(*) from pg_stat_activity "
                    "where wait_event_type = 'Lock' and query ilike '%products%'"
                )
            )
        if waiting:
            return
        time.sleep(0.01)
    raise AssertionError("second transaction did not wait for the row lock")


def test_concurrent_change_with_same_version_creates_one_event(
    session_factory: sessionmaker,
    db_engine: Engine,
    db_session: Session,
    sample_product: Product,
):
    first_commit_started = threading.Event()
    allow_commit = threading.Event()
    errors: list[Exception] = []

    with session_factory() as first_session:
        original_commit = first_session.commit

        def delayed_commit() -> None:
            first_commit_started.set()
            if not allow_commit.wait(timeout=5):
                raise TimeoutError("first commit was not released")
            original_commit()

        first_session.commit = delayed_commit  # type: ignore[method-assign]

        def run_first_change() -> None:
            try:
                change_price(first_session, "SKU-001", Decimal("1199.00"), 1, uuid4())
            except Exception as error:  # noqa: BLE001
                errors.append(error)

        first_thread = threading.Thread(target=run_first_change)
        first_thread.start()
        assert first_commit_started.wait(timeout=5)

        def run_second_change() -> None:
            with session_factory() as second_session:
                try:
                    change_price(second_session, "SKU-001", Decimal("1099.00"), 1, uuid4())
                except Exception as error:  # noqa: BLE001
                    errors.append(error)

        second_thread = threading.Thread(target=run_second_change)
        second_thread.start()
        try:
            _wait_for_row_lock_wait(db_engine)
        finally:
            allow_commit.set()
            first_thread.join(timeout=5)
            second_thread.join(timeout=5)

        assert not first_thread.is_alive()
        assert not second_thread.is_alive()

    assert len(errors) == 1
    assert isinstance(errors[0], PriceVersionConflictError)
    assert errors[0].current_version == 2

    db_session.expire_all()
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price == Decimal("1199.00")
    assert product.price_version == 2
    assert db_session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
