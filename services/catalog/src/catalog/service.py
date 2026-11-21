from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from catalog.models import ProcessedEvent, Product
from catalog.schemas import PriceChangedEvent


class ProductNotFoundError(Exception):
    pass


class InvalidMessageError(Exception):
    pass


@dataclass(frozen=True)
class ProcessEventResult:
    event_id: UUID
    status: Literal["applied", "duplicate", "stale"]


def process_event(
    session: Session,
    event: PriceChangedEvent,
    message_key: str,
) -> ProcessEventResult:
    if message_key != event.payload.productId:
        raise InvalidMessageError(
            f"message key {message_key!r} does not match productId {event.payload.productId!r}"
        )

    existing = session.get(ProcessedEvent, event.eventId)
    if existing is not None:
        return ProcessEventResult(event_id=event.eventId, status="duplicate")

    product = session.scalar(
        select(Product)
        .where(Product.id == event.payload.productId)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if product is None:
        session.rollback()
        raise ProductNotFoundError(event.payload.productId)

    processed_at = datetime.now(UTC)
    latency_ms = max(0, int((processed_at - event.occurredAt).total_seconds() * 1000))

    # Проверка валюты: если валюта события не совпадает с товаром, цена и версия не меняются
    applied = (
        event.payload.currency == product.currency
        and event.payload.priceVersion > product.price_version
    )
    if applied:
        product.price = event.payload.newPrice
        product.price_version = event.payload.priceVersion
        product.updated_at = processed_at

    session.add(
        ProcessedEvent(
            event_id=event.eventId,
            product_id=event.payload.productId,
            price_version=event.payload.priceVersion,
            occurred_at=event.occurredAt,
            processed_at=processed_at,
            latency_ms=latency_ms,
            applied=applied,
        )
    )
    try:
        session.commit()
    except Exception:
        session.rollback()
        raise

    return ProcessEventResult(
        event_id=event.eventId,
        status="applied" if applied else "stale",
    )
