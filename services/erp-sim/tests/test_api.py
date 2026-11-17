from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from erp_sim.models import OutboxEvent, Product
from fastapi.testclient import TestClient
from sqlalchemy import select
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration


def assert_product_unchanged(db_session: Session) -> None:
    db_session.expire_all()
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price == Decimal("1299.00")
    assert product.price_version == 1
    assert db_session.scalar(select(OutboxEvent)) is None


def test_update_price_returns_new_version(
    api_client: TestClient, db_session: Session, sample_product: Product
):
    correlation_id = uuid4()
    response = api_client.patch(
        "/products/SKU-001/price",
        json={"newPrice": "1199.00", "expectedPriceVersion": 1},
        headers={"X-Correlation-ID": str(correlation_id)},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["productId"] == "SKU-001"
    assert body["oldPrice"] == "1299.00"
    assert body["newPrice"] == "1199.00"
    assert body["currency"] == "RUB"
    assert body["priceVersion"] == 2
    assert UUID(body["eventId"])

    db_session.expire_all()
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price == Decimal("1199.00")
    assert product.price_version == 2

    outbox = db_session.scalar(select(OutboxEvent))
    assert outbox is not None
    assert outbox.payload["correlationId"] == str(correlation_id)


def test_update_price_rejects_unknown_product(api_client: TestClient):
    response = api_client.patch(
        "/products/SKU-404/price",
        json={"newPrice": "1199.00", "expectedPriceVersion": 1},
    )

    assert response.status_code == 404


def test_update_price_rejects_stale_version(api_client: TestClient, sample_product: Product):
    first = api_client.patch(
        "/products/SKU-001/price",
        json={"newPrice": "1199.00", "expectedPriceVersion": 1},
    )
    assert first.status_code == 200

    second = api_client.patch(
        "/products/SKU-001/price",
        json={"newPrice": "1099.00", "expectedPriceVersion": 1},
    )

    assert second.status_code == 409
    assert second.json()["detail"]["currentPriceVersion"] == 2


def test_update_price_rejects_same_price(api_client: TestClient, sample_product: Product):
    response = api_client.patch(
        "/products/SKU-001/price",
        json={"newPrice": "1299.00", "expectedPriceVersion": 1},
    )

    assert response.status_code == 409


@pytest.mark.parametrize(
    "payload",
    [
        {"newPrice": 1199.0, "expectedPriceVersion": 1},
        {"newPrice": "1199.0", "expectedPriceVersion": 1},
        {"newPrice": "1199.000", "expectedPriceVersion": 1},
        {"newPrice": "1199.00"},
        {"newPrice": "1199.00", "expectedPriceVersion": "1"},
    ],
)
def test_update_price_rejects_invalid_payload(
    api_client: TestClient, db_session: Session, sample_product: Product, payload: dict
):
    response = api_client.patch("/products/SKU-001/price", json=payload)

    assert response.status_code == 422
    assert_product_unchanged(db_session)


def test_update_price_rejects_invalid_correlation_header(
    api_client: TestClient, db_session: Session, sample_product: Product
):
    response = api_client.patch(
        "/products/SKU-001/price",
        json={"newPrice": "1199.00", "expectedPriceVersion": 1},
        headers={"X-Correlation-ID": "not-a-uuid"},
    )

    assert response.status_code == 422
    assert_product_unchanged(db_session)


def test_update_price_generates_correlation_id(
    api_client: TestClient, db_session: Session, sample_product: Product
):
    response = api_client.patch(
        "/products/SKU-001/price",
        json={"newPrice": "1199.00", "expectedPriceVersion": 1},
    )

    assert response.status_code == 200
    db_session.expire_all()
    outbox = db_session.scalar(select(OutboxEvent))
    assert outbox is not None
    assert UUID(outbox.payload["correlationId"])


def test_health(api_client: TestClient):
    response = api_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
