from decimal import Decimal
from pathlib import Path

import pytest
from erp_sim.models import Product
from erp_sim.seed import seed_products
from sqlalchemy import func, select
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[3]
SEED_PATH = ROOT / "data" / "products.json"


def test_seed_creates_three_products(db_session: Session):
    inserted = seed_products(db_session, SEED_PATH)
    assert inserted == 3

    db_session.expire_all()
    products = db_session.scalars(select(Product).order_by(Product.id)).all()
    assert [product.id for product in products] == ["SKU-001", "SKU-002", "SKU-003"]
    assert [product.price_version for product in products] == [1, 1, 1]


def test_seed_is_idempotent(db_session: Session):
    assert seed_products(db_session, SEED_PATH) == 3
    assert seed_products(db_session, SEED_PATH) == 0

    total = db_session.scalar(select(func.count()).select_from(Product))
    assert total == 3


def test_seed_does_not_reset_existing_product(db_session: Session):
    seed_products(db_session, SEED_PATH)

    product = db_session.get(Product, "SKU-001")
    assert product is not None
    product.price = Decimal("999.00")
    product.price_version = 5
    db_session.commit()

    assert seed_products(db_session, SEED_PATH) == 0

    db_session.expire_all()
    loaded = db_session.get(Product, "SKU-001")
    assert loaded is not None
    assert loaded.price == Decimal("999.00")
    assert loaded.price_version == 5
