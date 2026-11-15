import re
from datetime import UTC, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AwareDatetime,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
)

PRICE_PATTERN = re.compile(r"^\d{1,10}\.\d{2}$")
MAX_PRICE_VERSION = 2**31 - 1


def _parse_price(value: object) -> Decimal:
    if not isinstance(value, str) or PRICE_PATTERN.fullmatch(value) is None:
        raise ValueError("price must be a string with up to 10 integer digits and 2 decimals")
    return Decimal(value)


Price = Annotated[
    Decimal,
    BeforeValidator(_parse_price),
    Field(ge=0, max_digits=12, decimal_places=2),
]
PriceVersion = Annotated[int, Field(strict=True, ge=1, le=MAX_PRICE_VERSION)]


class PriceChangedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    productId: str = Field(min_length=1)
    priceVersion: PriceVersion
    oldPrice: Price
    newPrice: Price
    currency: str = Field(pattern=r"^[A-Z]{3}$")


class PriceChangedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    eventId: UUID
    eventType: Literal["price.changed"]
    schemaVersion: Literal[1]
    occurredAt: AwareDatetime
    correlationId: UUID
    source: str = Field(min_length=1)
    payload: PriceChangedPayload

    @field_validator("occurredAt")
    @classmethod
    def _normalize_occurred_at(cls, value: datetime) -> datetime:
        return value.astimezone(UTC)
