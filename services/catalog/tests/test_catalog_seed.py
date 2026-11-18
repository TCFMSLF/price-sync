from pathlib import Path

import pytest
from catalog.models import Product
from catalog.seed import seed_products
from sqlalchemy import func, select
from sqlalchemy.orm import Session

pytestmark = pytest.mark.integration
ROOT = Path(__file__).resolve().parents[3]
SEED_PATH = ROOT / "data" / "products.json"


def test_seed_products_loads_file(db_session: Session):
    inserted = seed_products(db_session, SEED_PATH)

    assert inserted == 3
    product = db_session.get(Product, "SKU-001")
    assert product is not None
    assert product.price.as_tuple().exponent == -2
    assert str(product.price) == "1299.00"
    assert product.currency == "RUB"
    assert product.price_version == 1


def test_seed_products_is_idempotent(db_session: Session):
    seed_products(db_session, SEED_PATH)

    inserted = seed_products(db_session, SEED_PATH)

    assert inserted == 0
    count = db_session.scalar(select(func.count()).select_from(Product))
    assert count == 3
