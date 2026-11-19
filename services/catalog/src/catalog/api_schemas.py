from datetime import datetime

from pydantic import BaseModel

from catalog.models import Product


class ProductOut(BaseModel):
    id: str
    name: str
    price: str
    currency: str
    priceVersion: int
    updatedAt: datetime

    @classmethod
    def from_model(cls, product: Product) -> "ProductOut":
        return cls(
            id=product.id,
            name=product.name,
            price=f"{product.price:.2f}",
            currency=product.currency,
            priceVersion=product.price_version,
            updatedAt=product.updated_at,
        )


class LatencyStats(BaseModel):
    p95Ms: int
    avgMs: float
    maxMs: int
    sampleSize: int


class StatsResponse(BaseModel):
    processed: int
    applied: int
    skipped: int
    failed: int
    latency: LatencyStats | None
