import argparse
import json
import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol
from uuid import UUID

from confluent_kafka import Producer
from sqlalchemy import select
from sqlalchemy.orm import Session

from erp_sim.config import settings
from erp_sim.db import SessionLocal
from erp_sim.models import OutboxEvent

logger = logging.getLogger(__name__)


class EventPublisher(Protocol):
    def publish_batch(self, events: Sequence[OutboxEvent]) -> dict[UUID, str | None]:
        """Publish events and return a map of event id to error text."""


@dataclass(frozen=True)
class PublishResult:
    published: int
    failed: int


class KafkaEventPublisher:
    def __init__(
        self,
        producer: Producer,
        topic: str,
        flush_timeout: float = 10.0,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._flush_timeout = flush_timeout

    def publish_batch(self, events: Sequence[OutboxEvent]) -> dict[UUID, str | None]:
        results: dict[UUID, str | None] = {}

        for event in events:

            def on_delivery(error, _message, event_id=event.id):
                results[event_id] = None if error is None else str(error)

            try:
                self._producer.produce(
                    topic=self._topic,
                    key=event.aggregate_id,
                    value=json.dumps(
                        event.payload,
                        ensure_ascii=False,
                        separators=(",", ":"),
                    ),
                    headers=[
                        ("eventId", str(event.id)),
                        ("eventType", event.event_type),
                        ("schemaVersion", str(event.payload["schemaVersion"])),
                        ("correlationId", event.payload["correlationId"]),
                        ("content-type", "application/json"),
                    ],
                    on_delivery=on_delivery,
                )
            except Exception as error:
                results[event.id] = str(error)
            self._producer.poll(0)

        self._producer.flush(self._flush_timeout)
        return {event.id: results.get(event.id, "delivery timed out") for event in events}


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


def run_forever(publisher: EventPublisher) -> None:
    delay = settings.outbox_poll_interval_seconds
    try:
        while True:
            with SessionLocal() as session:
                try:
                    result = publish_pending(
                        session,
                        publisher,
                        batch_size=settings.outbox_batch_size,
                    )
                except Exception:
                    session.rollback()
                    logger.exception("outbox batch failed")
                    result = None

            if result is None or result.failed:
                delay = min(
                    max(delay * 2, settings.outbox_poll_interval_seconds),
                    settings.outbox_retry_max_seconds,
                )
            else:
                delay = settings.outbox_poll_interval_seconds
            time.sleep(delay)
    except KeyboardInterrupt:
        logger.info("publisher stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish ERP outbox events to Kafka")
    parser.add_argument("--once", action="store_true", help="publish one batch and exit")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    producer = Producer(
        {
            "bootstrap.servers": settings.kafka_bootstrap_servers,
            "client.id": "erp-outbox-publisher",
            "acks": "all",
            "enable.idempotence": True,
            "message.timeout.ms": 10000,
        }
    )
    publisher = KafkaEventPublisher(producer, settings.kafka_topic_price_updates)
    try:
        if args.once:
            with SessionLocal() as session:
                result = publish_pending(
                    session,
                    publisher,
                    batch_size=settings.outbox_batch_size,
                )
            logger.info("outbox: published=%d failed=%d", result.published, result.failed)
            return
        run_forever(publisher)
    finally:
        producer.flush(10)


if __name__ == "__main__":
    main()
