import json
import logging
import os
from datetime import UTC, datetime

from .catalog_adapter import CatalogAdapter, PostgresCatalogAdapter
from .models import PriceChangedEvent
from .monitoring import PriceSyncMonitor

try:
    from confluent_kafka import Consumer, KafkaError, TopicPartition
except ImportError:
    Consumer = None
    KafkaError = None

    class TopicPartition:  # type: ignore[no-redef]
        """Заглушка для корректного расчёта consumer lag в тестах без confluent_kafka."""

        def __init__(self, topic: str, partition: int) -> None:
            self.topic = topic
            self.partition = partition


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("price-consumer")


def calculate_consumer_lag(consumer, msg) -> int:
    """Вычисляет отставание consumer от конца партиции (consumer lag)."""
    try:
        _, high = consumer.get_watermark_offsets(
            TopicPartition(msg.topic(), msg.partition()),
            cached=True,
        )
        if high is not None and high >= 0:
            return max(0, high - msg.offset() - 1)
    except Exception:
        pass
    return 0


def process_message(
    msg,
    adapter: CatalogAdapter,
    consumer,
    monitor: PriceSyncMonitor,
) -> bool:
    """
    Обрабатывает одно сообщение из Kafka:
    1) Валидирует JSON по контракту price.changed
    2) Идемпотентно обновляет цену в каталоге (с дедупликацией по eventId)
    3) Подтверждает offset только после успеха транзакции в БД
    4) Фиксирует latency и consumer lag
    """
    try:
        raw_data = json.loads(msg.value().decode("utf-8"))
        event = PriceChangedEvent.model_validate(raw_data)

        applied = adapter.apply_price_update(event)

        # Ручной commit offset строго после успешной фиксации изменений в БД
        consumer.commit(message=msg, asynchronous=False)

        latency_ms = (datetime.now(UTC) - event.occurredAt).total_seconds() * 1000
        consumer_lag = calculate_consumer_lag(consumer, msg)

        monitor.record_success(
            latency_ms=latency_ms,
            consumer_lag=consumer_lag,
            applied=applied,
        )

        if applied:
            logger.info(
                "Цена обновлена: product=%s new_price=%.2f %s (v%d) | "
                "latency=%.1f ms | lag=%d | corr=%s",
                event.payload.productId,
                event.payload.newPrice,
                event.payload.currency,
                event.payload.priceVersion,
                latency_ms,
                consumer_lag,
                event.correlationId,
            )
        else:
            logger.warning(
                "Дубликат события пропущен: eventId=%s | lag=%d",
                event.eventId,
                consumer_lag,
            )
        return True

    except Exception as exc:
        monitor.record_error()
        logger.exception("Ошибка обработки сообщения (offset не подтверждён): %s", exc)
        return False


def on_assign(_consumer, partitions) -> None:
    logger.info("Ребалансировка: назначены партиции %s", partitions)


def on_revoke(_consumer, partitions) -> None:
    logger.info("Ребалансировка: отозваны партиции %s", partitions)


def run_consumer(
    bootstrap_servers: str | None = None,
    topic: str = "price-updates",
    group_id: str = "catalog-price-sync",
    adapter: CatalogAdapter | None = None,
    monitor: PriceSyncMonitor | None = None,
) -> None:
    if Consumer is None:
        raise RuntimeError("Для работы consumer требуется библиотека confluent-kafka")

    bootstrap_servers = bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    adapter = adapter or PostgresCatalogAdapter()
    monitor = monitor or PriceSyncMonitor()

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    logger.info(
        "Consumer запущен (group=%s). Ожидание сообщений из топика '%s'...",
        group_id,
        topic,
    )

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                code = msg.error().code()
                if KafkaError and code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.info("Топик '%s' ещё не создан, ждём первое сообщение...", topic)
                elif KafkaError and code != KafkaError._PARTITION_EOF:
                    logger.error("Ошибка Kafka: %s", msg.error())
                continue

            process_message(msg, adapter, consumer, monitor)

    except KeyboardInterrupt:
        logger.info("Остановка consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()
