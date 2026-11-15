from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class PriceChangedPayload(BaseModel):
    model_config = ConfigDict(extra="forbid")

    productId: str = Field(min_length=1)
    priceVersion: int = Field(ge=1)
    oldPrice: Decimal = Field(ge=0, max_digits=12, decimal_places=2)
    newPrice: Decimal = Field(ge=0, max_digits=12, decimal_places=2)
    currency: str = Field(pattern=r"^[A-Z]{3}$")


class PriceChangedEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    eventId: UUID
    eventType: Literal["price.changed"]
    schemaVersion: Literal[1]
    occurredAt: datetime
    correlationId: UUID
    source: str = Field(min_length=1)
    payload: PriceChangedPayload
