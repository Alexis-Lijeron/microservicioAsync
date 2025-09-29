from pydantic import BaseModel, ConfigDict, field_validator
from typing import Optional, List, TYPE_CHECKING, Union
from datetime import datetime, time

if TYPE_CHECKING:
    from .aula import Aula
    from .grupo import Grupo

class HorarioBase(BaseModel):
    codigo_horario: str
    dia: str
    hora_inicio: time
    hora_final: time
    aula_codigo: str

class HorarioCreate(HorarioBase):
    @field_validator('hora_inicio', 'hora_final', mode='before')
    @classmethod
    def parse_time(cls, v):
        if isinstance(v, str):
            # Handle both HH:MM:SS and HH:MM:SS.sss formats
            if '.' in v:
                v = v.split('.')[0]  # Remove milliseconds
            time_parts = v.split(':')
            return time(
                int(time_parts[0]), 
                int(time_parts[1]), 
                int(time_parts[2]) if len(time_parts) > 2 else 0
            )
        return v

class HorarioUpdate(BaseModel):
    dia: Optional[str] = None
    hora_inicio: Optional[time] = None
    hora_final: Optional[time] = None
    aula_codigo: Optional[str] = None
    
    @field_validator('hora_inicio', 'hora_final', mode='before')
    @classmethod
    def parse_time(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            # Handle both HH:MM:SS and HH:MM:SS.sss formats
            if '.' in v:
                v = v.split('.')[0]  # Remove milliseconds
            time_parts = v.split(':')
            return time(
                int(time_parts[0]), 
                int(time_parts[1]), 
                int(time_parts[2]) if len(time_parts) > 2 else 0
            )
        return v

class HorarioInDB(HorarioBase):
    model_config = ConfigDict(from_attributes=True)
    created_at: datetime
    updated_at: datetime

class Horario(HorarioInDB):
    pass

class HorarioWithRelations(Horario):
    aula: Optional["Aula"] = None
    grupos: Optional[List["Grupo"]] = []