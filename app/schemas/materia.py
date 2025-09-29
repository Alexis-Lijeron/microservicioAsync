from pydantic import BaseModel, ConfigDict
from typing import Optional, List, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    from .nivel import Nivel
    from .plan_estudio import PlanEstudio
    from .grupo import Grupo
    from .prerrequisito import Prerrequisito


class MateriaBase(BaseModel):
    sigla: str
    nombre: str
    creditos: int
    es_electiva: bool = False
    nivel_codigo: str
    plan_estudio_codigo: str


class MateriaCreate(MateriaBase):
    pass


class MateriaUpdate(BaseModel):
    nombre: Optional[str] = None
    creditos: Optional[int] = None
    es_electiva: Optional[bool] = None
    nivel_codigo: Optional[str] = None
    plan_estudio_codigo: Optional[str] = None


class MateriaInDB(MateriaBase):
    model_config = ConfigDict(from_attributes=True)

    created_at: datetime
    updated_at: datetime


class Materia(MateriaInDB):
    pass


class MateriaWithRelations(Materia):
    nivel: Optional["Nivel"] = None
    plan_estudio: Optional["PlanEstudio"] = None
    grupos: Optional[List["Grupo"]] = []
    prerrequisitos_como_materia: Optional[List["Prerrequisito"]] = []
