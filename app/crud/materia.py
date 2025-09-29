from typing import Optional, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.materia import Materia
from app.schemas.materia import MateriaCreate, MateriaUpdate


class CRUDMateria(CRUDBase[Materia, MateriaCreate, MateriaUpdate]):
    def __init__(self):
        super().__init__(Materia, "sigla")

    async def get_by_sigla(self, db: AsyncSession, *, sigla: str) -> Optional[Materia]:
        result = await db.execute(select(Materia).where(Materia.sigla == sigla))
        return result.scalar_one_or_none()

    async def get_with_relations(self, db: AsyncSession, sigla: str) -> Optional[Materia]:
        result = await db.execute(
            select(Materia)
            .options(
                selectinload(Materia.nivel),
                selectinload(Materia.plan_estudio),
                selectinload(Materia.grupos),
                selectinload(Materia.prerrequisitos_como_materia),
            )
            .where(Materia.sigla == sigla)
        )
        return result.scalar_one_or_none()

    async def get_electivas(
        self, db: AsyncSession, skip: int = 0, limit: int = 100
    ) -> List[Materia]:
        result = await db.execute(
            select(Materia).where(Materia.es_electiva == True).offset(skip).limit(limit)
        )
        return result.scalars().all()

    async def get_by_nivel(
        self, db: AsyncSession, nivel_codigo: str, skip: int = 0, limit: int = 100
    ) -> List[Materia]:
        result = await db.execute(
            select(Materia)
            .where(Materia.nivel_codigo == nivel_codigo)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    async def get_by_plan_estudio(
        self, db: AsyncSession, plan_estudio_codigo: str, skip: int = 0, limit: int = 100
    ) -> List[Materia]:
        result = await db.execute(
            select(Materia)
            .where(Materia.plan_estudio_codigo == plan_estudio_codigo)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()


materia = CRUDMateria()
