from pydantic import BaseModel, ConfigDict
from typing import Optional, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    from .gestion import Gestion
    from .estudiante import Estudiante
    from .grupo import Grupo


class InscripcionBase(BaseModel):
    codigo_inscripcion: str
    semestre: int
    gestion_codigo: str
    estudiante_registro: str
    grupo_codigo: str


class InscripcionCreate(InscripcionBase):
    pass


class InscripcionUpdate(BaseModel):
    semestre: Optional[int] = None
    gestion_codigo: Optional[str] = None
    estudiante_registro: Optional[str] = None
    grupo_codigo: Optional[str] = None


class InscripcionInDB(InscripcionBase):
    model_config = ConfigDict(from_attributes=True)

    created_at: datetime
    updated_at: datetime


class Inscripcion(InscripcionInDB):
    pass


class InscripcionWithRelations(Inscripcion):
    gestion: Optional["Gestion"] = None
    estudiante: Optional["Estudiante"] = None
    grupo: Optional["Grupo"] = None
