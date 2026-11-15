from datetime import datetime, timezone
from pydantic import BaseModel, Field, field_validator


class PricePayload(BaseModel):
    product_id: str
    old_price: float | None = None
    new_price: float = Field(gt=0)
    currency: str = "RUB"


class PriceUpdatedEvent(BaseModel):
    event_id: str
    event_type: str
    event_version: int = 1
    occurred_at: datetime
    payload: PricePayload

    @field_validator("occurred_at")
    @classmethod
    def ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)