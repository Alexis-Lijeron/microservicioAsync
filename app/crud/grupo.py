from typing import Optional, List
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.grupo import Grupo
from app.schemas.grupo import GrupoCreate, GrupoUpdate


class CRUDGrupo(CRUDBase[Grupo, GrupoCreate, GrupoUpdate]):
    def __init__(self):
        super().__init__(Grupo, "codigo_grupo")

    async def get_with_relations(self, db: AsyncSession, codigo_grupo: str) -> Optional[Grupo]:
        result = await db.execute(
            select(Grupo)
            .options(
                selectinload(Grupo.docente),
                selectinload(Grupo.gestion),
                selectinload(Grupo.materia),
                selectinload(Grupo.horario),
                selectinload(Grupo.inscripciones),
                selectinload(Grupo.detalles),
            )
            .where(Grupo.codigo_grupo == codigo_grupo)
        )
        return result.scalar_one_or_none()

    async def get_by_materia(
        self, db: AsyncSession, materia_sigla: str, skip: int = 0, limit: int = 100
    ) -> List[Grupo]:
        result = await db.execute(
            select(Grupo)
            .where(Grupo.materia_sigla == materia_sigla)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    async def get_by_docente(
        self, db: AsyncSession, docente_codigo: str, skip: int = 0, limit: int = 100
    ) -> List[Grupo]:
        result = await db.execute(
            select(Grupo)
            .where(Grupo.docente_codigo == docente_codigo)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()

    async def get_by_gestion(
        self, db: AsyncSession, gestion_codigo: str, skip: int = 0, limit: int = 100
    ) -> List[Grupo]:
        result = await db.execute(
            select(Grupo)
            .where(Grupo.gestion_codigo == gestion_codigo)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()


grupo = CRUDGrupo()
