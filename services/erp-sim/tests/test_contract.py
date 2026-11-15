import json
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from erp_sim.schemas import PriceChangedEvent
from jsonschema import ValidationError, validate
from pydantic import ValidationError as PydanticValidationError

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


def test_schema_rejects_price_version_above_integer_range():
    event = load_json(EXAMPLE_PATH)
    event["payload"]["priceVersion"] = 2**31

    with pytest.raises(ValidationError):
        validate_example(event)


def test_schema_rejects_price_above_numeric_range():
    event = load_json(EXAMPLE_PATH)
    event["payload"]["newPrice"] = "10000000000.00"

    with pytest.raises(ValidationError):
        validate_example(event)


def test_schema_accepts_max_numeric_price():
    event = load_json(EXAMPLE_PATH)
    event["payload"]["newPrice"] = "9999999999.99"

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
    event = PriceChangedEvent.model_validate(load_json(EXAMPLE_PATH))

    assert event.payload.oldPrice == Decimal("1999.00")
    assert event.payload.newPrice == Decimal("1799.00")
    assert event.payload.priceVersion == 2


def test_pydantic_event_serializes_prices_as_strings():
    event = PriceChangedEvent.model_validate(load_json(EXAMPLE_PATH))
    dumped = event.model_dump(mode="json")

    assert dumped["payload"]["oldPrice"] == "1999.00"
    assert dumped["payload"]["newPrice"] == "1799.00"
    validate_example(dumped)


@pytest.mark.parametrize(
    "price",
    ["1799.0", "1799", "1799.000", 1799.0, "10000000000.00"],
)
def test_pydantic_rejects_non_canonical_price(price):
    event = load_json(EXAMPLE_PATH)
    event["payload"]["newPrice"] = price

    with pytest.raises(PydanticValidationError):
        PriceChangedEvent.model_validate(event)


def test_pydantic_rejects_string_price_version():
    event = load_json(EXAMPLE_PATH)
    event["payload"]["priceVersion"] = "2"

    with pytest.raises(PydanticValidationError):
        PriceChangedEvent.model_validate(event)


def test_pydantic_rejects_price_version_above_integer_range():
    event = load_json(EXAMPLE_PATH)
    event["payload"]["priceVersion"] = 2**31

    with pytest.raises(PydanticValidationError):
        PriceChangedEvent.model_validate(event)


def test_pydantic_rejects_naive_occurred_at():
    event = load_json(EXAMPLE_PATH)
    event["occurredAt"] = "2026-11-11T18:00:00"

    with pytest.raises(PydanticValidationError):
        PriceChangedEvent.model_validate(event)


def test_pydantic_normalizes_occurred_at_to_utc():
    event = load_json(EXAMPLE_PATH)
    event["occurredAt"] = "2026-11-11T21:00:00+03:00"

    parsed = PriceChangedEvent.model_validate(event)
    dumped = parsed.model_dump(mode="json")
    serialized = dumped["occurredAt"].replace("Z", "+00:00")

    assert datetime.fromisoformat(serialized) == datetime(2026, 11, 11, 18, 0, tzinfo=UTC)
