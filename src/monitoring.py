import json
import logging
import os
import urllib.request
from dataclasses import dataclass, field
from typing import Callable

logger = logging.getLogger("price-monitoring")


@dataclass
class AlertConfig:
    """Настраиваемые пороги и канал уведомлений (п. 5.7 ИЗ)."""
    max_latency_ms: float = float(os.getenv("MAX_LATENCY_MS", "200.0"))
    max_consumer_lag: int = int(os.getenv("MAX_CONSUMER_LAG", "100"))
    webhook_url: str | None = os.getenv("ALERT_WEBHOOK_URL") or None


@dataclass
class MonitoringStats:
    processed_count: int = 0
    duplicates_count: int = 0
    errors_count: int = 0
    alerts_count: int = 0
    last_latency_ms: float = 0.0
    last_consumer_lag: int = 0
    alert_history: list[str] = field(default_factory=list)


class PriceSyncMonitor:
    def __init__(
        self,
        config: AlertConfig | None = None,
        notifier: Callable[[str], None] | None = None,
    ) -> None:
        self.config = config or AlertConfig()
        self.stats = MonitoringStats()
        self._custom_notifier = notifier

    def send_alert(self, message: str) -> None:
        self.stats.alerts_count += 1
        self.stats.alert_history.append(message)
        logger.warning("ALERT: %s", message)

        if self._custom_notifier:
            self._custom_notifier(message)

        if self.config.webhook_url:
            try:
                req = urllib.request.Request(
                    self.config.webhook_url,
                    data=json.dumps({"text": message}).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                urllib.request.urlopen(req, timeout=2.0)
            except Exception as exc:
                logger.error("Не удалось отправить webhook-уведомление: %s", exc)

    def record_success(self, latency_ms: float, consumer_lag: int, applied: bool) -> None:
        if applied:
            self.stats.processed_count += 1
        else:
            self.stats.duplicates_count += 1

        self.stats.last_latency_ms = latency_ms
        self.stats.last_consumer_lag = consumer_lag

        if latency_ms > self.config.max_latency_ms:
            self.send_alert(
                f"Превышена целевая задержка синхронизации: {latency_ms:.1f} ms > {self.config.max_latency_ms:.1f} ms"
            )

        if consumer_lag > self.config.max_consumer_lag:
            self.send_alert(
                f"Рост consumer lag: lag={consumer_lag} сообщений > порога {self.config.max_consumer_lag}"
            )

    def record_error(self) -> None:
        self.stats.errors_count += 1