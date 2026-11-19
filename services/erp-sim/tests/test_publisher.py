from collections.abc import Sequence
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from erp_sim.models import OutboxEvent, Product
from erp_sim.publisher import PublishResult, publish_pending
from erp_sim.service import change_price
from sqlalchemy import select
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration


class FakePublisher:
    def __init__(
        self,
        errors: dict[UUID, str] | None = None,
        exception: Exception | None = None,
    ) -> None:
        self.errors = errors or {}
        self.exception = exception
        self.batches: list[list[UUID]] = []

    def publish_batch(self, events: Sequence[OutboxEvent]) -> dict[UUID, str | None]:
        self.batches.append([event.id for event in events])
        if self.exception is not None:
            raise self.exception
        return {event.id: self.errors.get(event.id) for event in events}


def create_event(
    session: Session,
    new_price: str = "1199.00",
    expected_version: int = 1,
) -> OutboxEvent:
    change_price(session, "SKU-001", Decimal(new_price), expected_version, uuid4())
    session.expire_all()
    event = session.scalar(select(OutboxEvent).order_by(OutboxEvent.created_at.desc()))
    assert event is not None
    return event


def test_publish_pending_marks_event_published(db_session: Session, sample_product: Product):
    event = create_event(db_session)
    publisher = FakePublisher()

    result = publish_pending(db_session, publisher, batch_size=10)

    assert result == PublishResult(published=1, failed=0)
    assert publisher.batches == [[event.id]]
    stored = db_session.get(OutboxEvent, event.id)
    assert stored is not None
    assert stored.published_at is not None
    assert stored.last_error is None
    assert stored.attempts == 0


def test_publish_pending_records_failure(db_session: Session, sample_product: Product):
    event = create_event(db_session)
    publisher = FakePublisher(errors={event.id: "kafka unavailable"})

    result = publish_pending(db_session, publisher, batch_size=10)

    assert result == PublishResult(published=0, failed=1)
    stored = db_session.get(OutboxEvent, event.id)
    assert stored is not None
    assert stored.published_at is None
    assert stored.attempts == 1
    assert stored.last_error == "kafka unavailable"


def test_publish_pending_retries_failed_event(db_session: Session, sample_product: Product):
    event = create_event(db_session)
    publish_pending(
        db_session,
        FakePublisher(errors={event.id: "kafka unavailable"}),
        batch_size=10,
    )

    result = publish_pending(db_session, FakePublisher(), batch_size=10)

    assert result == PublishResult(published=1, failed=0)
    stored = db_session.get(OutboxEvent, event.id)
    assert stored is not None
    assert stored.published_at is not None
    assert stored.attempts == 1
    assert stored.last_error is None


def test_publish_pending_skips_already_published(db_session: Session, sample_product: Product):
    event = create_event(db_session)
    event.published_at = datetime.now(UTC)
    db_session.commit()
    publisher = FakePublisher()

    result = publish_pending(db_session, publisher, batch_size=10)

    assert result == PublishResult(published=0, failed=0)
    assert publisher.batches == []


def test_publish_pending_respects_batch_size(db_session: Session, sample_product: Product):
    first = create_event(db_session)
    second = create_event(db_session, new_price="1099.00", expected_version=2)
    publisher = FakePublisher()

    first_result = publish_pending(db_session, publisher, batch_size=1)
    second_result = publish_pending(db_session, publisher, batch_size=1)

    assert first_result == PublishResult(published=1, failed=0)
    assert second_result == PublishResult(published=1, failed=0)
    assert len(publisher.batches) == 2
    assert all(len(batch) == 1 for batch in publisher.batches)
    assert {batch[0] for batch in publisher.batches} == {first.id, second.id}


def test_publish_pending_records_publisher_exception(db_session: Session, sample_product: Product):
    event = create_event(db_session)
    publisher = FakePublisher(exception=RuntimeError("producer unavailable"))

    result = publish_pending(db_session, publisher, batch_size=10)

    assert result == PublishResult(published=0, failed=1)
    stored = db_session.get(OutboxEvent, event.id)
    assert stored is not None
    assert stored.published_at is None
    assert stored.attempts == 1
    assert stored.last_error == "producer unavailable"


def test_publish_pending_truncates_long_error(db_session: Session, sample_product: Product):
    event = create_event(db_session)
    error = "x" * 3000
    publisher = FakePublisher(errors={event.id: error})

    publish_pending(db_session, publisher, batch_size=10)

    stored = db_session.get(OutboxEvent, event.id)
    assert stored is not None
    assert stored.last_error == error[:2000]


def test_publish_pending_rolls_back_on_commit_failure(
    db_session: Session, sample_product: Product, monkeypatch
):
    event = create_event(db_session)
    publisher = FakePublisher()

    def fail_commit() -> None:
        raise RuntimeError("db commit failed")

    monkeypatch.setattr(db_session, "commit", fail_commit)

    with pytest.raises(RuntimeError):
        publish_pending(db_session, publisher, batch_size=10)

    monkeypatch.undo()
    db_session.expire_all()
    stored = db_session.get(OutboxEvent, event.id)
    assert stored is not None
    assert stored.published_at is None
    assert stored.attempts == 0


def test_publish_pending_marks_only_successful_events(db_session: Session, sample_product: Product):
    first = create_event(db_session)
    second = create_event(db_session, new_price="1099.00", expected_version=2)
    publisher = FakePublisher(errors={second.id: "kafka unavailable"})

    result = publish_pending(db_session, publisher, batch_size=10)

    assert result == PublishResult(published=1, failed=1)
    first_stored = db_session.get(OutboxEvent, first.id)
    second_stored = db_session.get(OutboxEvent, second.id)
    assert first_stored is not None
    assert second_stored is not None
    assert first_stored.published_at is not None
    assert first_stored.attempts == 0
    assert first_stored.last_error is None
    assert second_stored.published_at is None
    assert second_stored.attempts == 1
    assert second_stored.last_error == "kafka unavailable"


class SilentProducer:
    def __init__(self) -> None:
        self.produced: list[dict] = []

    def produce(self, **kwargs) -> None:
        self.produced.append(kwargs)

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float) -> int:
        return 0


def test_publish_pending_records_kafka_delivery_timeout(
    db_session: Session, sample_product: Product
):
    from erp_sim.publisher import KafkaEventPublisher

    event = create_event(db_session)
    producer = SilentProducer()
    publisher = KafkaEventPublisher(producer, "price-updates")

    result = publish_pending(db_session, publisher, batch_size=10)

    assert result == PublishResult(published=0, failed=1)
    stored = db_session.get(OutboxEvent, event.id)
    assert stored is not None
    assert stored.published_at is None
    assert stored.attempts == 1
    assert stored.last_error == "delivery timed out"
