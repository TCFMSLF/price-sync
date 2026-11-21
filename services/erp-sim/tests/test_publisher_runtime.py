import sys

import pytest
from erp_sim import publisher
from erp_sim.publisher import PublishResult


class FakeSession:
    def __init__(self) -> None:
        self.rolled_back = False

    def __enter__(self) -> "FakeSession":
        return self

    def __exit__(self, *args) -> bool:
        return False

    def rollback(self) -> None:
        self.rolled_back = True


class FakeProducer:
    def __init__(self, config: dict) -> None:
        self.config = config

    def flush(self, timeout: float) -> int:
        return 0


def run_loop(monkeypatch, outcomes: list) -> tuple[list[float], list[FakeSession]]:
    sessions: list[FakeSession] = []

    def session_factory() -> FakeSession:
        session = FakeSession()
        sessions.append(session)
        return session

    remaining = list(outcomes)

    def fake_publish_pending(session, event_publisher, batch_size):
        outcome = remaining.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    sleeps: list[float] = []

    def fake_sleep(delay: float) -> None:
        sleeps.append(delay)
        if not remaining:
            raise KeyboardInterrupt

    monkeypatch.setattr(publisher, "SessionLocal", session_factory)
    monkeypatch.setattr(publisher, "publish_pending", fake_publish_pending)
    monkeypatch.setattr(publisher.time, "sleep", fake_sleep)
    monkeypatch.setattr(publisher.settings, "outbox_poll_interval_seconds", 1.0)
    monkeypatch.setattr(publisher.settings, "outbox_retry_max_seconds", 30.0)

    publisher.run_forever(object())
    return sleeps, sessions


def test_run_forever_caps_retry_delay(monkeypatch):
    sleeps, _ = run_loop(monkeypatch, [PublishResult(published=0, failed=1)] * 7)

    assert sleeps == [2, 4, 8, 16, 30, 30, 30]


def test_run_forever_resets_delay_after_success(monkeypatch):
    sleeps, sessions = run_loop(
        monkeypatch,
        [
            RuntimeError("db unavailable"),
            PublishResult(published=0, failed=1),
            PublishResult(published=1, failed=0),
            PublishResult(published=0, failed=1),
        ],
    )

    assert sleeps == [2, 4, 1, 2]
    assert sessions[0].rolled_back is True
    assert sessions[1].rolled_back is False


def test_main_once_exits_nonzero_when_delivery_fails(monkeypatch):
    install_main_fakes(monkeypatch, PublishResult(published=0, failed=1))

    with pytest.raises(SystemExit) as excinfo:
        publisher.main()

    assert excinfo.value.code == 1


@pytest.mark.parametrize("result", [PublishResult(published=1, failed=0), PublishResult(0, 0)])
def test_main_once_exits_zero_without_failures(monkeypatch, result):
    install_main_fakes(monkeypatch, result)

    assert publisher.main() is None


def install_main_fakes(monkeypatch, result: PublishResult) -> None:
    monkeypatch.setattr(sys, "argv", ["publisher", "--once"])
    monkeypatch.setattr(publisher, "Producer", FakeProducer)
    monkeypatch.setattr(publisher, "SessionLocal", FakeSession)
    monkeypatch.setattr(publisher, "publish_pending", lambda *args, **kwargs: result)
