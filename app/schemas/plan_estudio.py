from pydantic import BaseModel, ConfigDict
from typing import Optional, List, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    from .carrera import Carrera
    from .materia import Materia


class PlanEstudioBase(BaseModel):
    codigo: str
    cant_semestre: int
    plan: str
    carrera_codigo: str


class PlanEstudioCreate(PlanEstudioBase):
    pass


class PlanEstudioUpdate(BaseModel):
    cant_semestre: Optional[int] = None
    plan: Optional[str] = None
    carrera_codigo: Optional[str] = None


class PlanEstudioInDB(PlanEstudioBase):
    model_config = ConfigDict(from_attributes=True)
    created_at: datetime
    updated_at: datetime


class PlanEstudio(PlanEstudioInDB):
    pass


class PlanEstudioWithRelations(PlanEstudio):
    carrera: Optional["Carrera"] = None
    materias: Optional[List["Materia"]] = []
