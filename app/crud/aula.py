from typing import Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.crud.base import CRUDBase
from app.models.aula import Aula
from app.schemas.aula import AulaCreate, AulaUpdate


class CRUDAula(CRUDBase[Aula, AulaCreate, AulaUpdate]):
    def __init__(self):
        super().__init__(Aula, "codigo_aula")

    async def get_with_relations(self, db: AsyncSession, codigo_aula: str) -> Optional[Aula]:
        result = await db.execute(
            select(Aula)
            .options(selectinload(Aula.horarios))
            .where(Aula.codigo_aula == codigo_aula)
        )
        return result.scalar_one_or_none()


aula = CRUDAula()
