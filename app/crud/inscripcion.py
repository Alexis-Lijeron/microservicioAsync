from typing import Optional, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.inscripcion import Inscripcion
from app.schemas.inscripcion import InscripcionCreate, InscripcionUpdate


class CRUDInscripcion(CRUDBase[Inscripcion, InscripcionCreate, InscripcionUpdate]):
    def __init__(self):
        super().__init__(Inscripcion, "codigo_inscripcion")

    async def get_by_estudiante(
        self, db: AsyncSession, estudiante_registro: str, skip: int = 0, limit: int = 100
    ) -> List[Inscripcion]:
        result = await db.execute(
            select(Inscripcion)
            .where(Inscripcion.estudiante_registro == estudiante_registro)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    async def get_by_estudiante_with_relations(
        self, db: AsyncSession, estudiante_registro: str, skip: int = 0, limit: int = 100
    ) -> List[Inscripcion]:
        result = await db.execute(
            select(Inscripcion)
            .options(
                selectinload(Inscripcion.gestion),
                selectinload(Inscripcion.grupo),
                selectinload(Inscripcion.estudiante),
            )
            .where(Inscripcion.estudiante_registro == estudiante_registro)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    async def get_by_grupo(
        self, db: AsyncSession, grupo_codigo: str, skip: int = 0, limit: int = 100
    ) -> List[Inscripcion]:
        result = await db.execute(
            select(Inscripcion)
            .where(Inscripcion.grupo_codigo == grupo_codigo)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    async def get_by_estudiante_grupo(
        self, db: AsyncSession, estudiante_registro: str, grupo_codigo: str
    ) -> Optional[Inscripcion]:
        result = await db.execute(
            select(Inscripcion).where(
                (Inscripcion.estudiante_registro == estudiante_registro)
                & (Inscripcion.grupo_codigo == grupo_codigo)
            )
        )
        return result.scalar_one_or_none()

    async def get_by_gestion(
        self, db: AsyncSession, gestion_codigo: str, skip: int = 0, limit: int = 100
    ) -> List[Inscripcion]:
        result = await db.execute(
            select(Inscripcion)
            .where(Inscripcion.gestion_codigo == gestion_codigo)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()


inscripcion = CRUDInscripcion()
