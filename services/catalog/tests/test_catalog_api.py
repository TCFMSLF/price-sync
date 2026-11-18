from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from catalog.models import FailedEvent, ProcessedEvent, Product
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration


def add_processed(
    session: Session,
    *,
    applied: bool = True,
    latency_ms: int = 10,
    processed_at: datetime | None = None,
) -> ProcessedEvent:
    moment = processed_at or datetime.now(UTC)
    event = ProcessedEvent(
        event_id=uuid4(),
        product_id="SKU-001",
        price_version=2,
        occurred_at=moment - timedelta(milliseconds=latency_ms),
        processed_at=moment,
        latency_ms=latency_ms,
        applied=applied,
    )
    session.add(event)
    return event


def add_failed(session: Session, offset: int = 1) -> None:
    session.add(
        FailedEvent(
            event_id=None,
            product_id=None,
            payload=None,
            raw_value="broken",
            error="invalid json",
            attempts=1,
            topic="price-updates",
            partition_no=0,
            kafka_offset=offset,
            failed_at=datetime.now(UTC),
        )
    )


def test_health(api_client: TestClient):
    response = api_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_list_products(api_client: TestClient, db_session: Session, sample_product: Product):
    db_session.add(
        Product(
            id="SKU-002",
            name="Чай черный, 500 г",
            price=Decimal("799.00"),
            currency="RUB",
        )
    )
    db_session.commit()

    response = api_client.get("/products")

    assert response.status_code == 200
    body = response.json()
    assert [item["id"] for item in body] == ["SKU-001", "SKU-002"]
    assert body[0]["name"] == "Кофе в зернах, 1 кг"
    assert body[0]["price"] == "1299.00"
    assert body[0]["currency"] == "RUB"
    assert body[0]["priceVersion"] == 1
    assert "updatedAt" in body[0]


def test_get_product(api_client: TestClient, sample_product: Product):
    response = api_client.get("/products/SKU-001")

    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "Кофе в зернах, 1 кг"
    assert body["price"] == "1299.00"


def test_get_product_returns_404(api_client: TestClient, sample_product: Product):
    response = api_client.get("/products/SKU-404")

    assert response.status_code == 404


def test_stats_empty(api_client: TestClient, sample_product: Product):
    response = api_client.get("/stats")

    assert response.status_code == 200
    assert response.json() == {
        "processed": 0,
        "applied": 0,
        "skipped": 0,
        "failed": 0,
        "latency": None,
    }


def test_stats_counts(api_client: TestClient, db_session: Session, sample_product: Product):
    add_processed(db_session, applied=True, latency_ms=10)
    add_processed(db_session, applied=True, latency_ms=20)
    add_processed(db_session, applied=False, latency_ms=30)
    add_failed(db_session)
    db_session.commit()

    body = api_client.get("/stats").json()

    assert body["processed"] == 3
    assert body["applied"] == 2
    assert body["skipped"] == 1
    assert body["failed"] == 1
    assert body["latency"]["sampleSize"] == 3
    assert body["latency"]["p95Ms"] == 30
    assert body["latency"]["maxMs"] == 30
    assert body["latency"]["avgMs"] == pytest.approx(20.0)


def test_stats_uses_last_100_events(
    api_client: TestClient, db_session: Session, sample_product: Product
):
    base = datetime.now(UTC) - timedelta(seconds=300)
    for index in range(120):
        latency = 9999 if index < 20 else index - 19
        add_processed(
            db_session,
            latency_ms=latency,
            processed_at=base + timedelta(seconds=index),
        )
    db_session.commit()

    body = api_client.get("/stats").json()

    assert body["processed"] == 120
    assert body["latency"]["sampleSize"] == 100
    assert body["latency"]["maxMs"] == 100
    assert body["latency"]["p95Ms"] == 95
    assert body["latency"]["avgMs"] == pytest.approx(50.5)
