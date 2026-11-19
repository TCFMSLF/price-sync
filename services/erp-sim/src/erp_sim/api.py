from collections.abc import Iterator
from contextlib import asynccontextmanager
from typing import Annotated
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, status
from sqlalchemy.orm import Session

from erp_sim.api_schemas import PriceChangeRequest, PriceChangeResponse
from erp_sim.db import SessionLocal, init_db
from erp_sim.service import (
    PriceUnchangedError,
    PriceVersionConflictError,
    ProductNotFoundError,
    change_price,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="ERP Sim", version="0.1.0", lifespan=lifespan)


def get_session() -> Iterator[Session]:
    with SessionLocal() as session:
        yield session


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.patch("/products/{product_id}/price", response_model=PriceChangeResponse)
def update_price(
    product_id: str,
    request: PriceChangeRequest,
    session: Annotated[Session, Depends(get_session)],
    correlation_id: Annotated[UUID | None, Header(alias="X-Correlation-ID")] = None,
) -> PriceChangeResponse:
    try:
        result = change_price(
            session=session,
            product_id=product_id,
            new_price=request.newPrice,
            expected_version=request.expectedPriceVersion,
            correlation_id=correlation_id or uuid4(),
        )
    except ProductNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Товар не найден",
        ) from None
    except PriceVersionConflictError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "message": "Версия цены не совпадает",
                "currentPriceVersion": error.current_version,
            },
        ) from error
    except PriceUnchangedError:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Новая цена совпадает с текущей",
        ) from None

    return PriceChangeResponse(
        productId=result.product_id,
        oldPrice=format(result.old_price, ".2f"),
        newPrice=format(result.new_price, ".2f"),
        currency=result.currency,
        priceVersion=result.price_version,
        eventId=result.event_id,
    )
