import json
import logging
from datetime import datetime, timezone
from confluent_kafka import Consumer, KafkaError, TopicPartition
from .catalog_adapter import CatalogAdapter, SQLiteCatalogAdapter
from .models import PriceUpdatedEvent
from .monitoring import PriceSyncMonitor

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("price-consumer")


def calculate_consumer_lag(consumer: Consumer, msg) -> int:
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
    consumer: Consumer,
    monitor: PriceSyncMonitor,
) -> bool:
    """
    Обрабатывает одно сообщение из Kafka:
    1) Валидирует JSON
    2) Обновляет цену в каталоге (с дедупликацией)
    3) Подтверждает offset только после успеха БД
    4) Фиксирует latency и consumer lag
    """
    try:
        raw_data = json.loads(msg.value().decode("utf-8"))
        event = PriceUpdatedEvent.model_validate(raw_data)

        applied = adapter.apply_price_update(event)

        # Подтверждаем offset строго после успешной транзакции в БД
        consumer.commit(message=msg, asynchronous=False)

        latency_ms = (
            datetime.now(timezone.utc) - event.occurred_at
        ).total_seconds() * 1000
        consumer_lag = calculate_consumer_lag(consumer, msg)

        monitor.record_success(
            latency_ms=latency_ms,
            consumer_lag=consumer_lag,
            applied=applied,
        )

        if applied:
            logger.info(
                "Цена обновлена: product=%s new_price=%.2f %s | latency=%.1f ms | lag=%d",
                event.payload.product_id,
                event.payload.new_price,
                event.payload.currency,
                latency_ms,
                consumer_lag,
            )
        else:
            logger.warning(
                "Дубликат события пропущен: event_id=%s | lag=%d",
                event.event_id,
                consumer_lag,
            )
        return True

    except Exception as exc:
        # Offset НЕ подтверждается — при перезапуске/ребалансировке сообщение придёт снова
        monitor.record_error()
        logger.exception("Ошибка обработки сообщения (offset не подтверждён): %s", exc)
        return False


def on_assign(_consumer, partitions) -> None:
    logger.info("Ребалансировка: назначены партиции %s", partitions)


def on_revoke(_consumer, partitions) -> None:
    logger.info("Ребалансировка: отозваны партиции %s", partitions)


def run_consumer(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "price-updates",
    group_id: str = "catalog-price-updaters",
    adapter: CatalogAdapter | None = None,
    monitor: PriceSyncMonitor | None = None,
) -> None:
    adapter = adapter or SQLiteCatalogAdapter()
    monitor = monitor or PriceSyncMonitor()

    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # Ручной commit offset (п. 5.2)
    }

    consumer = Consumer(conf)
    consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
    logger.info("Consumer запущен. Ожидание сообщений из топика '%s'...", topic)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue

            if msg.error():
                code = msg.error().code()
                if code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    logger.info("Топик '%s' ещё не создан, ждём первое сообщение...", topic)
                elif code != KafkaError._PARTITION_EOF:
                    logger.error("Ошибка Kafka: %s", msg.error())
                continue

            process_message(msg, adapter, consumer, monitor)

    except KeyboardInterrupt:
        logger.info("Остановка consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    run_consumer()