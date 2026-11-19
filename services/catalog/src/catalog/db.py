from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from catalog.config import settings


class Base(DeclarativeBase):
    pass


engine = create_engine(settings.catalog_database_url, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def init_db() -> None:
    import catalog.models  # noqa: F401

    Base.metadata.create_all(bind=engine)
