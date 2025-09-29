from pydantic import BaseModel, ConfigDict
from typing import Optional, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    from .estudiante import Estudiante


class NotaBase(BaseModel):
    codigo_nota: str
    nota: float
    estudiante_registro: str


class NotaCreate(NotaBase):
    pass


class NotaUpdate(BaseModel):
    nota: Optional[float] = None
    estudiante_registro: Optional[str] = None


class NotaInDB(NotaBase):
    model_config = ConfigDict(from_attributes=True)
    created_at: datetime
    updated_at: datetime


class Nota(NotaInDB):
    pass


class NotaWithRelations(Nota):
    estudiante: Optional["Estudiante"] = None
