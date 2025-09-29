from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.prerrequisito import Prerrequisito
from app.schemas.prerrequisito import PrerequisitoCreate, PrerequisitoUpdate


class CRUDPrerrequisito(CRUDBase[Prerrequisito, PrerequisitoCreate, PrerequisitoUpdate]):
    def __init__(self):
        super().__init__(Prerrequisito, "codigo_prerrequisito")

    async def get_with_relations(self, db: AsyncSession, codigo_prerrequisito: str) -> Optional[Prerrequisito]:
        result = await db.execute(
            select(Prerrequisito)
            .options(selectinload(Prerrequisito.materia))
            .where(Prerrequisito.codigo_prerrequisito == codigo_prerrequisito)
        )
        return result.scalar_one_or_none()

    async def get_by_materia(
        self, db: AsyncSession, materia_sigla: str, skip: int = 0, limit: int = 100
    ):
        result = await db.execute(
            select(Prerrequisito)
            .where(Prerrequisito.materia_sigla == materia_sigla)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()


prerrequisito = CRUDPrerrequisito()
