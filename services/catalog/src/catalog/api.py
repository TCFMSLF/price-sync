import math
from collections.abc import Iterator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from catalog.api_schemas import LatencyStats, ProductOut, StatsResponse
from catalog.config import settings
from catalog.db import SessionLocal, init_db
from catalog.models import FailedEvent, ProcessedEvent, Product


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="Catalog", version="0.1.0", lifespan=lifespan)


def get_session() -> Iterator[Session]:
    with SessionLocal() as session:
        yield session


SessionDep = Annotated[Session, Depends(get_session)]


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/products", response_model=list[ProductOut])
def list_products(session: SessionDep) -> list[ProductOut]:
    products = session.scalars(select(Product).order_by(Product.id)).all()
    return [ProductOut.from_model(product) for product in products]


@app.get("/products/{product_id}", response_model=ProductOut)
def get_product(product_id: str, session: SessionDep) -> ProductOut:
    product = session.get(Product, product_id)
    if product is None:
        raise HTTPException(status_code=404, detail="product not found")
    return ProductOut.from_model(product)


@app.get("/stats", response_model=StatsResponse)
def get_stats(session: SessionDep) -> StatsResponse:
    processed = session.scalar(select(func.count()).select_from(ProcessedEvent)) or 0
    applied = (
        session.scalar(
            select(func.count()).select_from(ProcessedEvent).where(ProcessedEvent.applied.is_(True))
        )
        or 0
    )
    failed = session.scalar(select(func.count()).select_from(FailedEvent)) or 0

    latencies = session.scalars(
        select(ProcessedEvent.latency_ms)
        .order_by(ProcessedEvent.processed_at.desc())
        .limit(settings.latency_window_size)
    ).all()

    latency = None
    if latencies:
        values = sorted(latencies)
        p95 = values[max(0, math.ceil(0.95 * len(values)) - 1)]
        latency = LatencyStats(
            p95Ms=p95,
            avgMs=sum(values) / len(values),
            maxMs=values[-1],
            sampleSize=len(values),
        )

    return StatsResponse(
        processed=processed,
        applied=applied,
        skipped=processed - applied,
        failed=failed,
        latency=latency,
    )
