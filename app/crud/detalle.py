from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.detalle import Detalle
from app.schemas.detalle import DetalleCreate, DetalleUpdate


class CRUDDetalle(CRUDBase[Detalle, DetalleCreate, DetalleUpdate]):
    def __init__(self):
        super().__init__(Detalle, "codigo_detalle")

    async def get_with_relations(self, db: AsyncSession, codigo_detalle: str) -> Optional[Detalle]:
        result = await db.execute(
            select(Detalle)
            .options(selectinload(Detalle.grupo))
            .where(Detalle.codigo_detalle == codigo_detalle)
        )
        return result.scalar_one_or_none()

    async def get_by_grupo(
        self, db: AsyncSession, grupo_codigo: str, skip: int = 0, limit: int = 100
    ):
        result = await db.execute(
            select(Detalle)
            .where(Detalle.grupo_codigo == grupo_codigo)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()


detalle = CRUDDetalle()
