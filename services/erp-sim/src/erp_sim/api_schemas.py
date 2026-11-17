from uuid import UUID

from pydantic import BaseModel, ConfigDict

from erp_sim.schemas import Price, PriceVersion


class PriceChangeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    newPrice: Price
    expectedPriceVersion: PriceVersion


class PriceChangeResponse(BaseModel):
    productId: str
    oldPrice: str
    newPrice: str
    currency: str
    priceVersion: int
    eventId: UUID
