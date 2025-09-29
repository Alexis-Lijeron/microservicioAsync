from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.estudiante import Estudiante
from app.schemas.estudiante import EstudianteCreate, EstudianteUpdate
from app.core.security import get_password_hash


class CRUDEstudiante(CRUDBase[Estudiante, EstudianteCreate, EstudianteUpdate]):
    def __init__(self):
        super().__init__(Estudiante, "registro")

    async def get_by_registro(
        self, db: AsyncSession, *, registro: str
    ) -> Optional[Estudiante]:
        result = await db.execute(
            select(Estudiante).where(Estudiante.registro == registro)
        )
        return result.scalar_one_or_none()

    async def get_by_ci(self, db: AsyncSession, *, ci: str) -> Optional[Estudiante]:
        result = await db.execute(select(Estudiante).where(Estudiante.ci == ci))
        return result.scalar_one_or_none()

    async def create(self, db: AsyncSession, *, obj_in: EstudianteCreate) -> Estudiante:
        hashed_password = get_password_hash(obj_in.contraseña)
        db_obj = Estudiante(
            registro=obj_in.registro,
            nombre=obj_in.nombre,
            apellido=obj_in.apellido,
            ci=obj_in.ci,
            contraseña=hashed_password,
            carrera_codigo=obj_in.carrera_codigo,
        )
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    async def get_with_relations(
        self, db: AsyncSession, registro: str
    ) -> Optional[Estudiante]:
        result = await db.execute(
            select(Estudiante)
            .options(
                selectinload(Estudiante.carrera),
                selectinload(Estudiante.inscripciones),
                selectinload(Estudiante.notas),
            )
            .where(Estudiante.registro == registro)
        )
        return result.scalar_one_or_none()

    async def get_estudiantes_by_carrera(
        self, db: AsyncSession, carrera_codigo: str, skip: int = 0, limit: int = 100
    ):
        result = await db.execute(
            select(Estudiante)
            .where(Estudiante.carrera_codigo == carrera_codigo)
            .offset(skip)
            .limit(limit)
        )
        return result.scalars().all()


estudiante = CRUDEstudiante()
