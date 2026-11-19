import os
from collections.abc import Iterator
from decimal import Decimal

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

TEST_DATABASE_URL = "postgresql+psycopg://price_sync:price_sync@localhost:5433/catalog_test"
ADMIN_DATABASE_URL = "postgresql+psycopg://price_sync:price_sync@localhost:5433/postgres"
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

os.environ.setdefault("CATALOG_DATABASE_URL", TEST_DATABASE_URL)


def _ensure_test_database() -> None:
    admin_engine = create_engine(ADMIN_DATABASE_URL, isolation_level="AUTOCOMMIT")
    with admin_engine.connect() as connection:
        exists = connection.execute(
            text("SELECT 1 FROM pg_database WHERE datname = 'catalog_test'")
        ).scalar()
        if not exists:
            connection.execute(text("CREATE DATABASE catalog_test"))
    admin_engine.dispose()


@pytest.fixture(scope="session")
def db_engine() -> Iterator[Engine]:
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("integration tests disabled; set RUN_INTEGRATION=1")

    _ensure_test_database()
    engine = create_engine(TEST_DATABASE_URL)

    import catalog.models  # noqa: F401
    from catalog.db import Base

    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture()
def session_factory(db_engine: Engine) -> sessionmaker:
    return sessionmaker(bind=db_engine, expire_on_commit=False)


@pytest.fixture()
def db_session(session_factory: sessionmaker, db_engine: Engine) -> Iterator[Session]:
    with session_factory() as session:
        yield session
        session.rollback()
    with db_engine.begin() as connection:
        connection.execute(
            text("TRUNCATE TABLE failed_events, processed_events, products RESTART IDENTITY")
        )


@pytest.fixture()
def sample_product(db_session: Session):
    from catalog.models import Product

    product = Product(
        id="SKU-001",
        name="Кофе в зернах, 1 кг",
        price=Decimal("1299.00"),
        currency="RUB",
    )
    db_session.add(product)
    db_session.commit()
    return product


@pytest.fixture()
def api_client(session_factory: sessionmaker) -> Iterator[TestClient]:
    from catalog.api import app, get_session

    def override_get_session() -> Iterator[Session]:
        with session_factory() as session:
            yield session

    app.dependency_overrides[get_session] = override_get_session
    client = TestClient(app)
    try:
        yield client
    finally:
        app.dependency_overrides.clear()


@pytest.fixture()
def kafka_topic() -> Iterator[str]:
    if os.getenv("RUN_INTEGRATION") != "1":
        pytest.skip("integration tests disabled; set RUN_INTEGRATION=1")

    from uuid import uuid4

    from confluent_kafka.admin import AdminClient, NewTopic

    topic = f"catalog-price-updates-test-{uuid4()}"
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    futures = admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
    futures[topic].result(timeout=10)
    try:
        yield topic
    finally:
        futures = admin.delete_topics([topic])
        futures[topic].result(timeout=10)
