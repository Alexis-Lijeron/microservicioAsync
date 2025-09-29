from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.gestion import Gestion
from app.schemas.gestion import GestionCreate, GestionUpdate


class CRUDGestion(CRUDBase[Gestion, GestionCreate, GestionUpdate]):
    def __init__(self):
        super().__init__(Gestion, "codigo_gestion")

    async def get_with_relations(self, db: AsyncSession, codigo_gestion: str) -> Optional[Gestion]:
        result = await db.execute(
            select(Gestion)
            .options(
                selectinload(Gestion.grupos),
                selectinload(Gestion.inscripciones)
            )
            .where(Gestion.codigo_gestion == codigo_gestion)
        )
        return result.scalar_one_or_none()

    async def get_by_año(
        self, db: AsyncSession, año: int, skip: int = 0, limit: int = 100
    ):
        result = await db.execute(
            select(Gestion)
            .where(Gestion.año == año)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()


gestion = CRUDGestion()
