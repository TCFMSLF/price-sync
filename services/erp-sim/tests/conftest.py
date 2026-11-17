import os
from collections.abc import Iterator
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

TEST_DATABASE_URL = "postgresql+psycopg://price_sync:price_sync@localhost:5433/erp_test"
ADMIN_DATABASE_URL = "postgresql+psycopg://price_sync:price_sync@localhost:5433/postgres"

os.environ.setdefault("ERP_DATABASE_URL", TEST_DATABASE_URL)


def _ensure_test_database() -> None:
    admin_engine = create_engine(ADMIN_DATABASE_URL, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as connection:
        exists = connection.execute(
            text("SELECT 1 FROM pg_database WHERE datname = 'erp_test'")
        ).scalar()
        if not exists:
            connection.execute(text("CREATE DATABASE erp_test"))
    admin_engine.dispose()


@pytest.fixture(scope="session")
def db_engine() -> Iterator[Engine]:
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("integration tests disabled; set RUN_INTEGRATION=1")

    _ensure_test_database()
    engine = create_engine(TEST_DATABASE_URL)

    import erp_sim.models  # noqa: F401
    from erp_sim.db import Base

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture()
def db_session(db_engine: Engine) -> Iterator[Session]:
    session_factory = sessionmaker(bind=db_engine, expire_on_commit=False)
    with session_factory() as session:
        yield session
        session.rollback()
    with db_engine.begin() as connection:
        connection.execute(text("TRUNCATE TABLE outbox, products"))


@pytest.fixture()
def sample_product(db_session: Session):
    from erp_sim.models import Product

    product = Product(
        id="SKU-001",
        name="Кофе в зернах, 1 кг",
        price=Decimal("1299.00"),
        currency="RUB",
    )
    db_session.add(product)
    db_session.commit()
    return product
