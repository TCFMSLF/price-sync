from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from erp_sim.models import OutboxEvent, Product
from erp_sim.schemas import PriceChangedEvent, PriceChangedPayload


class ProductNotFoundError(Exception):
    pass


class PriceVersionConflictError(Exception):
    def __init__(self, current_version: int) -> None:
        super().__init__(f"current price version is {current_version}")
        self.current_version = current_version


class PriceUnchangedError(Exception):
    pass


@dataclass(frozen=True)
class PriceChangeResult:
    product_id: str
    old_price: Decimal
    new_price: Decimal
    currency: str
    price_version: int
    event_id: UUID
    occurred_at: datetime


def change_price(
    session: Session,
    product_id: str,
    new_price: Decimal,
    expected_version: int,
    correlation_id: UUID,
) -> PriceChangeResult:
    product = session.scalar(select(Product).where(Product.id == product_id).with_for_update())
    if product is None:
        raise ProductNotFoundError(product_id)
    if product.price_version != expected_version:
        raise PriceVersionConflictError(product.price_version)
    if product.price == new_price:
        raise PriceUnchangedError(product_id)

    occurred_at = datetime.now(UTC)
    event = PriceChangedEvent(
        eventId=uuid4(),
        eventType="price.changed",
        schemaVersion=1,
        occurredAt=occurred_at,
        correlationId=correlation_id,
        source="erp-sim",
        payload=PriceChangedPayload(
            productId=product.id,
            priceVersion=product.price_version + 1,
            oldPrice=format(product.price, ".2f"),
            newPrice=format(new_price, ".2f"),
            currency=product.currency,
        ),
    )
    old_price = product.price
    product.price = new_price
    product.price_version = event.payload.priceVersion
    product.updated_at = occurred_at
    session.add(
        OutboxEvent(
            id=event.eventId,
            aggregate_type="product",
            aggregate_id=product.id,
            event_type=event.eventType,
            payload=event.model_dump(mode="json"),
            created_at=occurred_at,
        )
    )
    try:
        session.commit()
    except Exception:
        session.rollback()
        raise
    return PriceChangeResult(
        product_id=product.id,
        old_price=old_price,
        new_price=new_price,
        currency=product.currency,
        price_version=event.payload.priceVersion,
        event_id=event.eventId,
        occurred_at=occurred_at,
    )
