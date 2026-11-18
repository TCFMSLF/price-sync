import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from catalog.models import FailedEvent
from catalog.schemas import PriceChangedEvent
from catalog.service import InvalidMessageError, ProductNotFoundError, process_event

logger = logging.getLogger(__name__)
ERROR_LIMIT = 2000


@dataclass(frozen=True)
class IncomingMessage:
    topic: str
    partition: int
    kafka_offset: int
    key: str | None
    value: str


@dataclass(frozen=True)
class ProcessMessageResult:
    event_id: UUID | None
    status: Literal["applied", "duplicate", "stale", "failed"]


def record_failed_event(
    session_factory: sessionmaker,
    incoming: IncomingMessage,
    event_id: UUID | None,
    product_id: str | None,
    payload: dict | None,
    error: str,
    attempts: int,
) -> None:
    with session_factory() as session:
        statement = (
            insert(FailedEvent)
            .values(
                event_id=event_id,
                product_id=product_id,
                payload=payload,
                raw_value=incoming.value,
                error=error[:ERROR_LIMIT],
                attempts=attempts,
                topic=incoming.topic,
                partition_no=incoming.partition,
                kafka_offset=incoming.kafka_offset,
                failed_at=datetime.now(UTC),
            )
            .on_conflict_do_nothing(constraint="uq_failed_events_position")
        )
        session.execute(statement)
        session.commit()


def _process_with_session(
    session_factory: sessionmaker,
    event: PriceChangedEvent,
    message_key: str,
):
    with session_factory() as session:
        return process_event(session, event, message_key)


def _record_failure(
    session_factory: sessionmaker,
    incoming: IncomingMessage,
    event: PriceChangedEvent | None,
    error: Exception,
    attempts: int,
) -> ProcessMessageResult:
    record_failed_event(
        session_factory=session_factory,
        incoming=incoming,
        event_id=event.eventId if event is not None else None,
        product_id=event.payload.productId if event is not None else None,
        payload=event.model_dump(mode="json") if event is not None else None,
        error=str(error),
        attempts=attempts,
    )
    logger.warning(
        "event at %s/%d/%d failed: %s",
        incoming.topic,
        incoming.partition,
        incoming.kafka_offset,
        error,
    )
    return ProcessMessageResult(
        event_id=event.eventId if event is not None else None,
        status="failed",
    )


def process_message(
    session_factory: sessionmaker,
    incoming: IncomingMessage,
    max_attempts: int,
    retry_delay: float,
) -> ProcessMessageResult:
    max_attempts = max(1, max_attempts)
    try:
        event = PriceChangedEvent.model_validate_json(incoming.value)
    except ValidationError as error:
        return _record_failure(session_factory, incoming, None, error, attempts=1)

    try:
        result = _process_with_session(session_factory, event, incoming.key or "")
        return ProcessMessageResult(event_id=result.event_id, status=result.status)
    except (InvalidMessageError, ProductNotFoundError) as error:
        return _record_failure(session_factory, incoming, event, error, attempts=1)
    except Exception as error:
        last_error = error

    for _attempt in range(2, max_attempts + 1):
        time.sleep(retry_delay)
        try:
            result = _process_with_session(session_factory, event, incoming.key or "")
            return ProcessMessageResult(event_id=result.event_id, status=result.status)
        except (InvalidMessageError, ProductNotFoundError) as permanent:
            return _record_failure(session_factory, incoming, event, permanent, attempts=1)
        except Exception as retry_error:
            last_error = retry_error

    return _record_failure(
        session_factory,
        incoming,
        event,
        last_error,
        attempts=max_attempts,
    )
