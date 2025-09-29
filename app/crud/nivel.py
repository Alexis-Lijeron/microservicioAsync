from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.nivel import Nivel
from app.schemas.nivel import NivelCreate, NivelUpdate


class CRUDNivel(CRUDBase[Nivel, NivelCreate, NivelUpdate]):
    def __init__(self):
        super().__init__(Nivel, "codigo_nivel")

    async def get_with_relations(self, db: AsyncSession, codigo_nivel: str) -> Optional[Nivel]:
        result = await db.execute(
            select(Nivel)
            .options(selectinload(Nivel.materias))
            .where(Nivel.codigo_nivel == codigo_nivel)
        )
        return result.scalar_one_or_none()

    async def get_by_nivel_number(self, db: AsyncSession, nivel: int) -> Optional[Nivel]:
        result = await db.execute(
            select(Nivel).where(Nivel.nivel == nivel)
        )
        return result.scalar_one_or_none()


nivel = CRUDNivel()
