import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from confluent_kafka import Consumer, TopicPartition
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from catalog.config import settings
from catalog.db import SessionLocal, init_db
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
        "event at %s/%d/%d failed permanently: %s",
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

    # При сбое БД не коммитим смещение в Kafka и не сбрасываем в failed_events,
    # а пробрасываем ошибку для автоматического повтора после восстановления БД
    raise last_error


def _seek_back(consumer, incoming: IncomingMessage, delay: float) -> None:
    topic_partition = TopicPartition(incoming.topic, incoming.partition, incoming.kafka_offset)
    while True:
        try:
            consumer.seek(topic_partition)
            return
        except Exception:
            logger.exception(
                "seek failed for %s/%d/%d; retrying",
                incoming.topic,
                incoming.partition,
                incoming.kafka_offset,
            )
            time.sleep(delay)


def consume_once(
    consumer,
    session_factory: sessionmaker,
    timeout: float = 1.0,
    max_attempts: int | None = None,
    retry_delay: float | None = None,
) -> ProcessMessageResult | None:
    attempts = settings.consumer_max_attempts if max_attempts is None else max_attempts
    delay = settings.consumer_retry_delay_seconds if retry_delay is None else retry_delay

    message = consumer.poll(timeout)
    if message is None:
        return None
    if message.error() is not None:
        logger.warning("consumer error: %s", message.error())
        return None

    key_bytes = message.key()
    incoming = IncomingMessage(
        topic=message.topic(),
        partition=message.partition(),
        kafka_offset=message.offset(),
        key=key_bytes.decode("utf-8", errors="replace") if key_bytes is not None else None,
        value=(message.value() or b"").decode("utf-8", errors="replace"),
    )

    try:
        result = process_message(
            session_factory,
            incoming,
            max_attempts=attempts,
            retry_delay=delay,
        )
        consumer.commit(message=message, asynchronous=False)
    except Exception:
        logger.exception(
            "failed to process %s/%d/%d; offset not committed, retrying",
            incoming.topic,
            incoming.partition,
            incoming.kafka_offset,
        )
        time.sleep(delay)
        _seek_back(consumer, incoming, delay)
        return None

    return result


def run_forever(consumer, session_factory: sessionmaker) -> None:
    try:
        while True:
            try:
                consume_once(
                    consumer,
                    session_factory,
                    timeout=settings.consumer_poll_timeout_seconds,
                )
            except Exception:
                logger.exception("consumer loop error; retrying")
                time.sleep(settings.consumer_retry_delay_seconds)
    except KeyboardInterrupt:
        logger.info("consumer stopped")
    finally:
        consumer.close()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    init_db()
    consumer = Consumer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "group.id": settings.kafka_consumer_group,
            "client.id": settings.kafka_consumer_group,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
        }
    )
    consumer.subscribe([settings.kafka_topic_price_updates])
    logger.info(
        "catalog consumer started: topic=%s group=%s",
        settings.kafka_topic_price_updates,
        settings.kafka_consumer_group,
    )
    run_forever(consumer, SessionLocal)


if __name__ == "__main__":
    main()
