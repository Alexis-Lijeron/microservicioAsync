from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.config.database import Base

ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)


class CRUDBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):
    def __init__(self, model: Type[ModelType], primary_key_field: str = None):
        """
        Objeto CRUD con métodos por defecto para Create, Read, Update, Delete (CRUD).
        """
        self.model = model
        self.primary_key_field = primary_key_field or self._get_primary_key_field()

    def _get_primary_key_field(self):
        """
        Obtiene el nombre del campo de clave primaria del modelo.
        """
        for column in self.model.__table__.columns:
            if column.primary_key:
                return column.name
        return "id"  # fallback por defecto

    async def get(self, db: AsyncSession, code: Any) -> Optional[ModelType]:
        primary_key_column = getattr(self.model, self.primary_key_field)
        result = await db.execute(select(self.model).where(primary_key_column == code))
        return result.scalar_one_or_none()

    async def get_multi(
        self, db: AsyncSession, *, skip: int = 0, limit: int = 100
    ) -> List[ModelType]:
        result = await db.execute(select(self.model).offset(skip).limit(limit))
        return result.scalars().all()

    async def create(self, db: AsyncSession, *, obj_in: CreateSchemaType) -> ModelType:
        # Use dict() instead of jsonable_encoder to preserve Python objects like time
        if hasattr(obj_in, 'dict'):
            obj_in_data = obj_in.dict()
        else:
            obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(**obj_in_data)
        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    async def update(
        self,
        db: AsyncSession,
        *,
        db_obj: ModelType,
        obj_in: Union[UpdateSchemaType, Dict[str, Any]]
    ) -> ModelType:
        obj_data = jsonable_encoder(db_obj)
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.dict(exclude_unset=True)

        for field in obj_data:
            if field in update_data:
                setattr(db_obj, field, update_data[field])

        db.add(db_obj)
        await db.commit()
        await db.refresh(db_obj)
        return db_obj

    async def remove(self, db: AsyncSession, *, code: Any) -> ModelType:
        primary_key_column = getattr(self.model, self.primary_key_field)
        result = await db.execute(select(self.model).where(primary_key_column == code))
        obj = result.scalar_one_or_none()
        if obj:
            await db.delete(obj)
            await db.commit()
        return obj

    async def count(self, db: AsyncSession) -> int:
        primary_key_column = getattr(self.model, self.primary_key_field)
        result = await db.execute(select(func.count(primary_key_column)))
        return result.scalar()
