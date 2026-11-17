import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session

from erp_sim.models import OutboxEvent

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    def publish_batch(self, events: Sequence[OutboxEvent]) -> dict[UUID, str | None]:
        """Publish events and return a map of event id to error text."""


@dataclass(frozen=True)
class PublishResult:
    published: int
    failed: int


def publish_pending(
    session: Session,
    publisher: EventPublisher,
    batch_size: int,
) -> PublishResult:
    events = list(
        session.scalars(
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        )
    )
    if not events:
        return PublishResult(published=0, failed=0)

    try:
        results = publisher.publish_batch(events)
    except Exception as error:
        logger.warning("publisher batch failed: %s", error)
        results = {event.id: str(error) for event in events}

    published_at = datetime.now(UTC)
    published = 0
    failed = 0
    for event in events:
        error_text = results.get(event.id, "delivery result missing")
        if error_text is None:
            event.published_at = published_at
            event.last_error = None
            published += 1
        else:
            event.attempts += 1
            event.last_error = error_text[:2000]
            logger.warning("outbox event %s not published: %s", event.id, error_text)
            failed += 1

    try:
        session.commit()
    except Exception:
        session.rollback()
        raise

    return PublishResult(published=published, failed=failed)
