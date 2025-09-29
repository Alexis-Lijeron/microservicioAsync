from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.horario import Horario
from app.schemas.horario import HorarioCreate, HorarioUpdate


class CRUDHorario(CRUDBase[Horario, HorarioCreate, HorarioUpdate]):
    def __init__(self):
        super().__init__(Horario, "codigo_horario")

    async def get_with_relations(self, db: AsyncSession, codigo_horario: str) -> Optional[Horario]:
        result = await db.execute(
            select(Horario)
            .options(
                selectinload(Horario.aula),
                selectinload(Horario.grupos)
            )
            .where(Horario.codigo_horario == codigo_horario)
        )
        return result.scalar_one_or_none()

    async def get_by_aula(
        self, db: AsyncSession, aula_codigo: str, skip: int = 0, limit: int = 100
    ):
        result = await db.execute(
            select(Horario)
            .where(Horario.aula_codigo == aula_codigo)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    async def get_by_dia(
        self, db: AsyncSession, dia: str, skip: int = 0, limit: int = 100
    ):
        result = await db.execute(
            select(Horario)
            .where(Horario.dia == dia)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()


horario = CRUDHorario()
