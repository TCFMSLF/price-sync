from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal


@dataclass
class PricePayload:
    productId: str
    newPrice: Decimal
    currency: str
    oldPrice: Decimal | None = None
    priceVersion: int = 1


@dataclass
class PriceChangedEvent:
    eventId: str
    eventType: str
    schemaVersion: int
    occurredAt: datetime
    correlationId: str
    source: str
    payload: PricePayload

    @classmethod
    def model_validate(cls, data: dict) -> "PriceChangedEvent":
        p = data["payload"]
        old_p = Decimal(str(p["oldPrice"])) if p.get("oldPrice") is not None else None
        occurred = datetime.fromisoformat(data["occurredAt"].replace("Z", "+00:00"))
        if occurred.tzinfo is None:
            occurred = occurred.replace(tzinfo=UTC)
        else:
            occurred = occurred.astimezone(UTC)

        payload = PricePayload(
            productId=p["productId"],
            oldPrice=old_p,
            newPrice=Decimal(str(p["newPrice"])),
            currency=p["currency"],
            priceVersion=int(p.get("priceVersion", 1)),
        )
        return cls(
            eventId=data["eventId"],
            eventType=data["eventType"],
            schemaVersion=int(data.get("schemaVersion", 1)),
            occurredAt=occurred,
            correlationId=data.get("correlationId", ""),
            source=data.get("source", "erp-sim"),
            payload=payload,
        )
