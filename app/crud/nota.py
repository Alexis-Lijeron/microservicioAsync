from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.nota import Nota
from app.schemas.nota import NotaCreate, NotaUpdate


class CRUDNota(CRUDBase[Nota, NotaCreate, NotaUpdate]):
    def __init__(self):
        super().__init__(Nota, "codigo_nota")

    async def get_with_relations(self, db: AsyncSession, codigo_nota: str) -> Optional[Nota]:
        result = await db.execute(
            select(Nota)
            .options(selectinload(Nota.estudiante))
            .where(Nota.codigo_nota == codigo_nota)
        )
        return result.scalar_one_or_none()

    async def get_by_estudiante(
        self, db: AsyncSession, estudiante_registro: str, skip: int = 0, limit: int = 100
    ):
        result = await db.execute(
            select(Nota)
            .where(Nota.estudiante_registro == estudiante_registro)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()


nota = CRUDNota()
