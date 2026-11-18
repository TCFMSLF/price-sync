from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from catalog.db import Base


class Product(Base):
    __tablename__ = "products"
    __table_args__ = (
        CheckConstraint("price >= 0", name="ck_catalog_products_price_non_negative"),
        CheckConstraint("currency ~ '^[A-Z]{3}$'", name="ck_catalog_products_currency_format"),
        CheckConstraint("price_version >= 1", name="ck_catalog_products_price_version_positive"),
    )

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    price_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )


class ProcessedEvent(Base):
    __tablename__ = "processed_events"
    __table_args__ = (
        CheckConstraint("price_version >= 1", name="ck_processed_events_price_version_positive"),
        CheckConstraint("latency_ms >= 0", name="ck_processed_events_latency_non_negative"),
        Index("ix_processed_events_processed_at", "processed_at"),
    )

    event_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), primary_key=True)
    product_id: Mapped[str] = mapped_column(Text, nullable=False)
    price_version: Mapped[int] = mapped_column(Integer, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    processed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    applied: Mapped[bool] = mapped_column(Boolean, nullable=False)


class FailedEvent(Base):
    __tablename__ = "failed_events"
    __table_args__ = (
        UniqueConstraint("topic", "partition_no", "kafka_offset", name="uq_failed_events_position"),
        CheckConstraint("attempts >= 1", name="ck_failed_events_attempts_positive"),
        Index("ix_failed_events_failed_at", "failed_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    event_id: Mapped[UUID | None] = mapped_column(PGUUID(as_uuid=True), nullable=True)
    product_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    raw_value: Mapped[str] = mapped_column(Text, nullable=False)
    error: Mapped[str] = mapped_column(Text, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    partition_no: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    failed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
