import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from .models import PriceUpdatedEvent


class CatalogAdapter(ABC):
    @abstractmethod
    def apply_price_update(self, event: PriceUpdatedEvent) -> bool:
        """
        Обновляет цену товара в каталоге.
        Возвращает True, если событие применено впервые,
        и False, если событие является дубликатом.
        """

    @abstractmethod
    def get_product(self, product_id: str) -> dict | None:
        """Возвращает текущие данные товара из каталога."""


class SQLiteCatalogAdapter(CatalogAdapter):
    def __init__(self, db_path: str = "catalog.db") -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    product_id TEXT PRIMARY KEY,
                    price REAL NOT NULL,
                    currency TEXT NOT NULL,
                    last_event_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id TEXT PRIMARY KEY,
                    processed_at TEXT NOT NULL
                )
            """)

    def apply_price_update(self, event: PriceUpdatedEvent) -> bool:
        now_iso = datetime.now(timezone.utc).isoformat()
        occurred_iso = event.occurred_at.isoformat()

        with self.conn:
            # 1. Дедупликация по event_id в одной транзакции с обновлением цены
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO processed_events (event_id, processed_at)
                VALUES (?, ?)
                """,
                (event.event_id, now_iso),
            )
            if cur.rowcount == 0:
                return False

            # 2. Идемпотентное обновление 
            self.conn.execute(
                """
                INSERT INTO products (product_id, price, currency, last_event_at, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    price = excluded.price,
                    currency = excluded.currency,
                    last_event_at = excluded.last_event_at,
                    updated_at = excluded.updated_at
                WHERE excluded.last_event_at >= products.last_event_at
                """,
                (
                    event.payload.product_id,
                    event.payload.new_price,
                    event.payload.currency,
                    occurred_iso,
                    now_iso,
                ),
            )
        return True

    def get_product(self, product_id: str) -> dict | None:
        row = self.conn.execute(
            "SELECT * FROM products WHERE product_id = ?",
            (product_id,),
        ).fetchone()
        return dict(row) if row else None