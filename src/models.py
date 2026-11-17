from datetime import datetime, timezone
from decimal import Decimal
from typing import Literal
from pydantic import BaseModel, Field, field_validator


class PricePayload(BaseModel):
    productId: str = Field(min_length=1)
    oldPrice: Decimal | None = Field(default=None, ge=Decimal("0.00"))
    newPrice: Decimal = Field(gt=Decimal("0.00"))
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    priceVersion: int = Field(default=1, ge=1)


class PriceChangedEvent(BaseModel):
    eventId: str = Field(min_length=1)
    eventType: Literal["price.changed"]
    schemaVersion: int = Field(default=1, ge=1)
    occurredAt: datetime
    correlationId: str = Field(min_length=1)
    source: str = Field(min_length=1)
    payload: PricePayload

    @field_validator("occurredAt")
    @classmethod
    def ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)