import os
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from decimal import Decimal
import psycopg
from psycopg.rows import dict_row
from .models import PriceChangedEvent


class CatalogAdapter(ABC):
    @abstractmethod
    def apply_price_update(self, event: PriceChangedEvent) -> bool:
        """
        Обновляет цену товара в каталоге.
        Возвращает True, если событие применено впервые,
        и False, если событие является дубликатом.
        """

    @abstractmethod
    def get_product(self, product_id: str) -> dict | None:
        """Возвращает текущие данные товара из каталога."""


class PostgresCatalogAdapter(CatalogAdapter):
    def __init__(self, dsn: str | None = None) -> None:
        self.dsn = dsn or os.getenv(
            "DATABASE_URL",
            "postgresql://catalog:catalog@localhost:5432/catalog_db",
        )
        # autocommit=True + self.conn.transaction() — безопасные явные транзакции в psycopg 3
        self.conn = psycopg.connect(self.dsn, row_factory=dict_row, autocommit=True)
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn.transaction():
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    product_id TEXT PRIMARY KEY,
                    price NUMERIC(12, 2) NOT NULL,
                    currency CHAR(3) NOT NULL,
                    price_version INTEGER NOT NULL,
                    last_event_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL
                )
            """)
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    event_id TEXT PRIMARY KEY,
                    processed_at TIMESTAMPTZ NOT NULL
                )
            """)

    def apply_price_update(self, event: PriceChangedEvent) -> bool:
        now_dt = datetime.now(timezone.utc)
        p = event.payload

        with self.conn.transaction():
            # 1. Дедупликация по eventId в одной транзакции с обновлением цены
            row = self.conn.execute(
                """
                INSERT INTO processed_events (event_id, processed_at)
                VALUES (%s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING event_id
                """,
                (event.eventId, now_dt),
            ).fetchone()

            if row is None:
                return False

            # 2. Идемпотентное обновление цены с защитой по priceVersion и occurredAt
            self.conn.execute(
                """
                INSERT INTO products (
                    product_id, price, currency, price_version, last_event_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_id) DO UPDATE SET
                    price = EXCLUDED.price,
                    currency = EXCLUDED.currency,
                    price_version = EXCLUDED.price_version,
                    last_event_at = EXCLUDED.last_event_at,
                    updated_at = EXCLUDED.updated_at
                WHERE EXCLUDED.price_version > products.price_version
                   OR (
                       EXCLUDED.price_version = products.price_version
                       AND EXCLUDED.last_event_at >= products.last_event_at
                   )
                """,
                (
                    p.productId,
                    p.newPrice,
                    p.currency,
                    p.priceVersion,
                    event.occurredAt,
                    now_dt,
                ),
            )
        return True

    def get_product(self, product_id: str) -> dict | None:
        return self.conn.execute(
            "SELECT * FROM products WHERE product_id = %s",
            (product_id,),
        ).fetchone()


class SQLiteCatalogAdapter(CatalogAdapter):
    """Облегчённый адаптер для локальных unit-тестов без запуска Postgres."""

    def __init__(self, db_path: str = "catalog.db") -> None:
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self._init_schema()

    def _init_schema(self) -> None:
        with self.conn:
            self.conn.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    product_id TEXT PRIMARY KEY,
                    price TEXT NOT NULL,
                    currency TEXT NOT NULL,
                    price_version INTEGER NOT NULL,
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

    def apply_price_update(self, event: PriceChangedEvent) -> bool:
        now_iso = datetime.now(timezone.utc).isoformat()
        occurred_iso = event.occurredAt.isoformat()
        p = event.payload

        with self.conn:
            cur = self.conn.execute(
                """
                INSERT OR IGNORE INTO processed_events (event_id, processed_at)
                VALUES (?, ?)
                """,
                (event.eventId, now_iso),
            )
            if cur.rowcount == 0:
                return False

            self.conn.execute(
                """
                INSERT INTO products (
                    product_id, price, currency, price_version, last_event_at, updated_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(product_id) DO UPDATE SET
                    price = excluded.price,
                    currency = excluded.currency,
                    price_version = excluded.price_version,
                    last_event_at = excluded.last_event_at,
                    updated_at = excluded.updated_at
                WHERE excluded.price_version > products.price_version
                   OR (
                       excluded.price_version = products.price_version
                       AND excluded.last_event_at >= products.last_event_at
                   )
                """,
                (
                    p.productId,
                    f"{p.newPrice:.2f}",
                    p.currency,
                    p.priceVersion,
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
        if not row:
            return None
        data = dict(row)
        data["price"] = Decimal(data["price"])
        return data