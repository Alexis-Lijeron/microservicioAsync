from pydantic import BaseModel, ConfigDict
from typing import Optional, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    from .materia import Materia


class PrerrequisiteBase(BaseModel):
    codigo_prerrequisito: str
    materia_sigla: str
    sigla_prerrequisito: str


class PrerequisitoCreate(PrerrequisiteBase):
    pass


class PrerequisitoUpdate(BaseModel):
    materia_sigla: Optional[str] = None
    sigla_prerrequisito: Optional[str] = None


class PrerrequisiteInDB(PrerrequisiteBase):
    model_config = ConfigDict(from_attributes=True)
    created_at: datetime
    updated_at: datetime


class Prerrequisito(PrerrequisiteInDB):
    pass


class PrerrequisiteWithRelations(Prerrequisito):
    materia: Optional["Materia"] = None
