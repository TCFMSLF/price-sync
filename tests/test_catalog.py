from datetime import datetime, timedelta, timezone
from decimal import Decimal
import uuid
from src.catalog_adapter import SQLiteCatalogAdapter
from src.models import PriceChangedEvent, PricePayload


def make_event(
    event_id: str,
    product_id: str = "SKU-001",
    new_price: str = "1799.00",
    price_version: int = 1,
    occurred_at: datetime | None = None,
) -> PriceChangedEvent:
    return PriceChangedEvent(
        eventId=event_id,
        eventType="price.changed",
        schemaVersion=1,
        occurredAt=occurred_at or datetime.now(timezone.utc),
        correlationId=str(uuid.uuid4()),
        source="erp-sim",
        payload=PricePayload(
            productId=product_id,
            oldPrice=Decimal("1999.00"),
            newPrice=Decimal(new_price),
            currency="RUB",
            priceVersion=price_version,
        ),
    )


def test_apply_new_price_success(tmp_path):
    adapter = SQLiteCatalogAdapter(str(tmp_path / "catalog.db"))
    event = make_event("evt-1", new_price="1799.00", price_version=1)

    applied = adapter.apply_price_update(event)

    assert applied is True
    product = adapter.get_product("SKU-001")
    assert product is not None
    assert product["price"] == Decimal("1799.00")
    assert product["price_version"] == 1


def test_duplicate_event_is_ignored(tmp_path):
    adapter = SQLiteCatalogAdapter(str(tmp_path / "catalog.db"))
    event_first = make_event("evt-dup", new_price="1799.00", price_version=1)
    event_duplicate = make_event("evt-dup", new_price="1000.00", price_version=2)

    assert adapter.apply_price_update(event_first) is True
    assert adapter.apply_price_update(event_duplicate) is False

    # Цена должна остаться от первого события (1799.00)
    product = adapter.get_product("SKU-001")
    assert product["price"] == Decimal("1799.00")


def test_stale_price_version_does_not_overwrite_newer_price(tmp_path):
    adapter = SQLiteCatalogAdapter(str(tmp_path / "catalog.db"))
    now = datetime.now(timezone.utc)

    newer_event = make_event("evt-v2", new_price="2500.00", price_version=2, occurred_at=now)
    older_event = make_event(
        "evt-v1",
        new_price="1200.00",
        price_version=1,
        occurred_at=now + timedelta(seconds=5),
    )

    assert adapter.apply_price_update(newer_event) is True
    # Событие с меньшей priceVersion=1 не должно перезаписать priceVersion=2
    adapter.apply_price_update(older_event)

    product = adapter.get_product("SKU-001")
    assert product["price"] == Decimal("2500.00")
    assert product["price_version"] == 2