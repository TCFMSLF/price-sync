import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from src.catalog_adapter import CatalogAdapter, SQLiteCatalogAdapter
from src.consumer import process_message
from src.models import PriceChangedEvent
from src.monitoring import AlertConfig, PriceSyncMonitor


class FakeMessage:
    def __init__(self, payload: dict, offset: int = 10) -> None:
        self._raw = json.dumps(payload).encode("utf-8")
        self._offset = offset

    def value(self) -> bytes:
        return self._raw

    def topic(self) -> str:
        return "price-updates"

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset


class FakeConsumer:
    def __init__(self, high_watermark: int = 11) -> None:
        self.committed_offsets: list[int] = []
        self._high_watermark = high_watermark

    def commit(self, message=None, asynchronous=False) -> None:
        self.committed_offsets.append(message.offset())

    def get_watermark_offsets(self, partition, cached=True):
        return (0, self._high_watermark)


class FailingCatalogAdapter(CatalogAdapter):
    def apply_price_update(self, event: PriceChangedEvent) -> bool:
        raise RuntimeError("БД временно недоступна")

    def get_product(self, product_id: str) -> dict | None:
        return None


def make_raw_event(event_id: str = "evt-100", latency_ms: float = 10.0) -> dict:
    occurred_at = datetime.now(timezone.utc) - timedelta(milliseconds=latency_ms)
    return {
        "eventId": event_id,
        "eventType": "price.changed",
        "schemaVersion": 1,
        "occurredAt": occurred_at.isoformat().replace("+00:00", "Z"),
        "correlationId": str(uuid.uuid4()),
        "source": "erp-sim",
        "payload": {
            "productId": "SKU-002",
            "oldPrice": "3490.00",
            "newPrice": "2990.00",
            "currency": "RUB",
            "priceVersion": 1,
        },
    }


def test_process_message_commits_after_db_success(tmp_path):
    adapter = SQLiteCatalogAdapter(str(tmp_path / "catalog.db"))
    consumer = FakeConsumer(high_watermark=11)
    monitor = PriceSyncMonitor()
    msg = FakeMessage(make_raw_event("evt-ok"), offset=10)

    ok = process_message(msg, adapter, consumer, monitor)

    assert ok is True
    assert consumer.committed_offsets == [10]
    assert adapter.get_product("SKU-002")["price"] == Decimal("2990.00")
    assert monitor.stats.processed_count == 1
    assert monitor.stats.last_consumer_lag == 0


def test_process_message_does_not_commit_on_db_failure():
    adapter = FailingCatalogAdapter()
    consumer = FakeConsumer()
    monitor = PriceSyncMonitor()
    msg = FakeMessage(make_raw_event("evt-fail"), offset=42)

    ok = process_message(msg, adapter, consumer, monitor)

    assert ok is False
    assert consumer.committed_offsets == []
    assert monitor.stats.errors_count == 1


def test_monitoring_alerts_on_high_latency_and_lag(tmp_path):
    received_alerts: list[str] = []
    config = AlertConfig(max_latency_ms=200.0, max_consumer_lag=5)
    monitor = PriceSyncMonitor(config=config, notifier=received_alerts.append)

    adapter = SQLiteCatalogAdapter(str(tmp_path / "catalog.db"))
    consumer = FakeConsumer(high_watermark=20)
    msg = FakeMessage(make_raw_event("evt-slow", latency_ms=350.0), offset=10)

    ok = process_message(msg, adapter, consumer, monitor)

    assert ok is True
    assert monitor.stats.last_consumer_lag == 9
    assert monitor.stats.alerts_count == 2
    assert len(received_alerts) == 2