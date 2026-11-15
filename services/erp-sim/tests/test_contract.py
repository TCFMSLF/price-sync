import json
from decimal import Decimal
from pathlib import Path

import pytest
from jsonschema import ValidationError, validate

ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = ROOT / "contracts" / "price-changed.schema.json"
EXAMPLE_PATH = ROOT / "contracts" / "examples" / "price-changed.v1.json"


def load_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_example(event: dict) -> None:
    validate(instance=event, schema=load_json(SCHEMA_PATH))


def test_schema_rejects_numeric_price():
    event = load_json(EXAMPLE_PATH)
    event["payload"]["newPrice"] = 1799.00

    with pytest.raises(ValidationError):
        validate_example(event)


def test_schema_rejects_missing_price_version():
    event = load_json(EXAMPLE_PATH)
    del event["payload"]["priceVersion"]

    with pytest.raises(ValidationError):
        validate_example(event)


def test_schema_rejects_non_positive_price_version():
    event = load_json(EXAMPLE_PATH)
    event["payload"]["priceVersion"] = 0

    with pytest.raises(ValidationError):
        validate_example(event)


def test_schema_rejects_string_price_version():
    event = load_json(EXAMPLE_PATH)
    event["payload"]["priceVersion"] = "2"

    with pytest.raises(ValidationError):
        validate_example(event)


def test_example_matches_schema():
    validate_example(load_json(EXAMPLE_PATH))


def test_schema_rejects_unknown_field():
    event = load_json(EXAMPLE_PATH)
    event["payload"]["unknown"] = "value"

    with pytest.raises(ValidationError):
        validate_example(event)


def test_schema_rejects_invalid_currency():
    event = load_json(EXAMPLE_PATH)
    event["payload"]["currency"] = "rub"

    with pytest.raises(ValidationError):
        validate_example(event)


def test_pydantic_event_parses_decimal_prices():
    from erp_sim.schemas import PriceChangedEvent

    event = PriceChangedEvent.model_validate(load_json(EXAMPLE_PATH))

    assert event.payload.oldPrice == Decimal("1999.00")
    assert event.payload.newPrice == Decimal("1799.00")
    assert event.payload.priceVersion == 2


def test_pydantic_event_serializes_prices_as_strings():
    from erp_sim.schemas import PriceChangedEvent

    event = PriceChangedEvent.model_validate(load_json(EXAMPLE_PATH))
    dumped = event.model_dump(mode="json")

    assert dumped["payload"]["oldPrice"] == "1999.00"
    assert dumped["payload"]["newPrice"] == "1799.00"
    validate_example(dumped)
