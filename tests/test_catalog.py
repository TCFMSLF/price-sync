from datetime import datetime, timedelta, timezone
from src.catalog_adapter import SQLiteCatalogAdapter
from src.models import PricePayload, PriceUpdatedEvent


def make_event(
    event_id: str,
    product_id: str = "SM-10045",
    new_price: float = 4299.0,
    occurred_at: datetime | None = None,
) -> PriceUpdatedEvent:
    return PriceUpdatedEvent(
        event_id=event_id,
        event_type="PriceUpdated",
        event_version=1,
        occurred_at=occurred_at or datetime.now(timezone.utc),
        payload=PricePayload(
            product_id=product_id,
            old_price=4999.0,
            new_price=new_price,
            currency="RUB",
        ),
    )


def test_apply_new_price_success(tmp_path):
    adapter = SQLiteCatalogAdapter(str(tmp_path / "catalog.db"))
    event = make_event("evt-1", new_price=4299.0)

    applied = adapter.apply_price_update(event)

    assert applied is True
    product = adapter.get_product("SM-10045")
    assert product is not None
    assert product["price"] == 4299.0


def test_duplicate_event_is_ignored(tmp_path):
    adapter = SQLiteCatalogAdapter(str(tmp_path / "catalog.db"))
    event_first = make_event("evt-dup", new_price=4299.0)
    event_duplicate = make_event("evt-dup", new_price=1000.0)

    assert adapter.apply_price_update(event_first) is True
    assert adapter.apply_price_update(event_duplicate) is False

    product = adapter.get_product("SM-10045")
    assert product["price"] == 4299.0


def test_stale_event_does_not_overwrite_newer_price(tmp_path):
    adapter = SQLiteCatalogAdapter(str(tmp_path / "catalog.db"))
    now = datetime.now(timezone.utc)

    newer_event = make_event("evt-new", new_price=5000.0, occurred_at=now)
    older_event = make_event(
        "evt-old",
        new_price=3000.0,
        occurred_at=now - timedelta(minutes=5),
    )

    assert adapter.apply_price_update(newer_event) is True
    adapter.apply_price_update(older_event)

    product = adapter.get_product("SM-10045")
    assert product["price"] == 5000.0