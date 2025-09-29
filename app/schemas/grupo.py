from pydantic import BaseModel, ConfigDict
from typing import Optional, List, TYPE_CHECKING
from datetime import datetime

if TYPE_CHECKING:
    from .docente import Docente
    from .gestion import Gestion
    from .materia import Materia
    from .horario import Horario
    from .inscripcion import Inscripcion
    from .detalle import Detalle


class GrupoBase(BaseModel):
    codigo_grupo: str
    descripcion: str
    docente_codigo: str
    gestion_codigo: str
    materia_sigla: str
    horario_codigo: str


class GrupoCreate(GrupoBase):
    pass


class GrupoUpdate(BaseModel):
    descripcion: Optional[str] = None
    docente_codigo: Optional[str] = None
    gestion_codigo: Optional[str] = None
    materia_sigla: Optional[str] = None
    horario_codigo: Optional[str] = None


class GrupoInDB(GrupoBase):
    model_config = ConfigDict(from_attributes=True)

    created_at: datetime
    updated_at: datetime


class Grupo(GrupoInDB):
    pass


class GrupoWithRelations(Grupo):
    docente: Optional["Docente"] = None
    gestion: Optional["Gestion"] = None
    materia: Optional["Materia"] = None
    horario: Optional["Horario"] = None
    inscripciones: Optional[List["Inscripcion"]] = []
    detalles: Optional[List["Detalle"]] = []
