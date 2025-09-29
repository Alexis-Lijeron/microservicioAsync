from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.plan_estudio import PlanEstudio
from app.schemas.plan_estudio import PlanEstudioCreate, PlanEstudioUpdate


class CRUDPlanEstudio(CRUDBase[PlanEstudio, PlanEstudioCreate, PlanEstudioUpdate]):
    def __init__(self):
        super().__init__(PlanEstudio, "codigo")

    async def get_with_relations(self, db: AsyncSession, codigo: str) -> Optional[PlanEstudio]:
        result = await db.execute(
            select(PlanEstudio)
            .options(
                selectinload(PlanEstudio.carrera),
                selectinload(PlanEstudio.materias)
            )
            .where(PlanEstudio.codigo == codigo)
        )
        return result.scalar_one_or_none()

    async def get_by_carrera(
        self, db: AsyncSession, carrera_codigo: str, skip: int = 0, limit: int = 100
    ):
        result = await db.execute(
            select(PlanEstudio)
            .where(PlanEstudio.carrera_codigo == carrera_codigo)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()


plan_estudio = CRUDPlanEstudio()
