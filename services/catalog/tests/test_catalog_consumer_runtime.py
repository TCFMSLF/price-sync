from uuid import uuid4

from catalog import consumer as consumer_module
from catalog.consumer import ProcessMessageResult, consume_once, run_forever


class FakeMessage:
    def __init__(self, offset: int) -> None:
        self._offset = offset

    def topic(self) -> str:
        return "price-updates"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes:
        return b"SKU-001"

    def value(self) -> bytes:
        return b"{}"

    def error(self):
        return None


class FakeConsumer:
    def __init__(self, messages: list[FakeMessage], stop_after: bool = False) -> None:
        self.messages = messages
        self.stop_after = stop_after
        self.commits: list[FakeMessage] = []
        self.seeks: list[tuple[str, int, int]] = []
        self.closed = False

    def poll(self, timeout: float):
        if self.messages:
            return self.messages.pop(0)
        if self.stop_after:
            raise KeyboardInterrupt
        return None

    def commit(self, message, asynchronous: bool = True) -> None:
        self.commits.append(message)

    def seek(self, topic_partition) -> None:
        self.seeks.append(
            (
                topic_partition.topic,
                topic_partition.partition,
                topic_partition.offset,
            )
        )

    def close(self) -> None:
        self.closed = True


def test_consume_once_commits_offset_after_processing(monkeypatch):
    message = FakeMessage(offset=3)
    consumer = FakeConsumer([message])
    monkeypatch.setattr(
        consumer_module,
        "process_message",
        lambda *args, **kwargs: ProcessMessageResult(event_id=uuid4(), status="applied"),
    )

    result = consume_once(consumer, object(), timeout=0.1, max_attempts=1, retry_delay=0)

    assert result is not None
    assert result.status == "applied"
    assert consumer.commits == [message]


def test_consume_once_returns_none_without_message():
    consumer = FakeConsumer([])

    result = consume_once(consumer, object(), timeout=0.1, max_attempts=1, retry_delay=0)

    assert result is None
    assert consumer.commits == []


def test_consume_once_does_not_commit_when_recording_fails(monkeypatch):
    consumer = FakeConsumer([FakeMessage(offset=4)])

    def fail(*args, **kwargs):
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(consumer_module, "process_message", fail)

    result = consume_once(consumer, object(), timeout=0.1, max_attempts=1, retry_delay=0)

    assert result is None
    assert consumer.commits == []
    assert consumer.seeks == [("price-updates", 0, 4)]


def test_consume_once_retries_same_offset_when_commit_fails(monkeypatch):
    class CommitFailingConsumer(FakeConsumer):
        def commit(self, message, asynchronous: bool = True) -> None:
            raise RuntimeError("kafka unavailable")

    consumer = CommitFailingConsumer([FakeMessage(offset=8)])
    monkeypatch.setattr(
        consumer_module,
        "process_message",
        lambda *args, **kwargs: ProcessMessageResult(event_id=uuid4(), status="applied"),
    )

    result = consume_once(consumer, object(), timeout=0.1, max_attempts=1, retry_delay=0)

    assert result is None
    assert consumer.seeks == [("price-updates", 0, 8)]


def test_consume_once_replaces_non_utf8_key(monkeypatch):
    class BinaryKeyMessage(FakeMessage):
        def key(self) -> bytes:
            return b"\xffSKU-001"

    consumer = FakeConsumer([BinaryKeyMessage(offset=10)])
    received: list[str | None] = []

    def capture(session_factory, incoming, max_attempts, retry_delay):
        received.append(incoming.key)
        return ProcessMessageResult(event_id=uuid4(), status="failed")

    monkeypatch.setattr(consumer_module, "process_message", capture)

    consume_once(consumer, object(), timeout=0.1, max_attempts=1, retry_delay=0)

    assert received == ["\ufffdSKU-001"]
    assert len(consumer.commits) == 1


def test_consume_once_continues_after_failed_message(monkeypatch):
    consumer = FakeConsumer([FakeMessage(offset=5), FakeMessage(offset=6)])
    statuses = iter(["failed", "applied"])
    monkeypatch.setattr(
        consumer_module,
        "process_message",
        lambda *args, **kwargs: ProcessMessageResult(event_id=uuid4(), status=next(statuses)),
    )

    first = consume_once(consumer, object(), timeout=0.1, max_attempts=1, retry_delay=0)
    second = consume_once(consumer, object(), timeout=0.1, max_attempts=1, retry_delay=0)

    assert first is not None and first.status == "failed"
    assert second is not None and second.status == "applied"
    assert len(consumer.commits) == 2


def test_run_forever_closes_consumer_on_keyboard_interrupt(monkeypatch):
    message = FakeMessage(offset=7)
    consumer = FakeConsumer([message], stop_after=True)
    monkeypatch.setattr(
        consumer_module,
        "process_message",
        lambda *args, **kwargs: ProcessMessageResult(event_id=uuid4(), status="applied"),
    )

    run_forever(consumer, object())

    assert consumer.commits == [message]
    assert consumer.closed is True


def test_run_forever_survives_transient_poll_error(monkeypatch):
    class FlakyConsumer(FakeConsumer):
        def __init__(self) -> None:
            super().__init__([])
            self.failures = 1

        def poll(self, timeout: float):
            if self.failures:
                self.failures -= 1
                raise RuntimeError("kafka temporarily unavailable")
            raise KeyboardInterrupt

    consumer = FlakyConsumer()
    monkeypatch.setattr(consumer_module.time, "sleep", lambda delay: None)

    run_forever(consumer, object())

    assert consumer.closed is True


def test_main_initializes_database(monkeypatch):
    calls: list[str] = []

    class StubConsumer:
        def __init__(self, config) -> None:
            self.config = config

        def subscribe(self, topics) -> None:
            calls.append("subscribe")

    monkeypatch.setattr(consumer_module, "init_db", lambda: calls.append("init"))
    monkeypatch.setattr(consumer_module, "Consumer", StubConsumer)
    monkeypatch.setattr(
        consumer_module,
        "run_forever",
        lambda consumer, session_factory: calls.append("run"),
    )

    consumer_module.main()

    assert calls == ["init", "subscribe", "run"]
