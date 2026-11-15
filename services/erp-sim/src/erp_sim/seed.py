import json
from decimal import Decimal
from pathlib import Path

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from erp_sim.config import settings
from erp_sim.db import SessionLocal, init_db
from erp_sim.models import Product


def seed_products(session: Session, path: Path) -> int:
    items = json.loads(path.read_text(encoding="utf-8"))
    rows = [
        {
            "id": item["id"],
            "name": item["name"],
            "price": Decimal(item["price"]),
            "currency": item["currency"],
            "price_version": item["priceVersion"],
        }
        for item in items
    ]
    statement = (
        insert(Product)
        .values(rows)
        .on_conflict_do_nothing(index_elements=["id"])
        .returning(Product.id)
    )
    inserted_ids = session.execute(statement).scalars().all()
    session.commit()
    return len(inserted_ids)


def main() -> None:
    init_db()
    path = Path(settings.seed_file)
    with SessionLocal() as session:
        inserted = seed_products(session, path)
    print(f"Добавлено товаров: {inserted}")


if __name__ == "__main__":
    main()
