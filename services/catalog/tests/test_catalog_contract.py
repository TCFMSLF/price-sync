import json
from decimal import Decimal
from pathlib import Path

import pytest
from catalog.schemas import PriceChangedEvent
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[3]
EXAMPLE_PATH = ROOT / "contracts" / "examples" / "price-changed.v1.json"


def load_example() -> dict:
    return json.loads(EXAMPLE_PATH.read_text(encoding="utf-8"))


def test_contract_example_parses():
    event = PriceChangedEvent.model_validate(load_example())

    assert event.eventType == "price.changed"
    assert event.schemaVersion == 1
    assert event.payload.priceVersion == 2
    assert event.payload.oldPrice == Decimal("1999.00")
    assert event.payload.newPrice == Decimal("1799.00")


@pytest.mark.parametrize("price", ["1799.0", "1799", 1799.0, "10000000000.00"])
def test_contract_rejects_non_canonical_price(price):
    event = load_example()
    event["payload"]["newPrice"] = price

    with pytest.raises(ValidationError):
        PriceChangedEvent.model_validate(event)


def test_contract_rejects_naive_occurred_at():
    event = load_example()
    event["occurredAt"] = "2026-11-11T18:00:00"

    with pytest.raises(ValidationError):
        PriceChangedEvent.model_validate(event)
