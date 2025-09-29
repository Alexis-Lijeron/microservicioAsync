from pydantic import BaseModel, ConfigDict, field_validator
from typing import Optional, TYPE_CHECKING
from datetime import datetime, date, time

if TYPE_CHECKING:
    from .grupo import Grupo


class DetalleBase(BaseModel):
    codigo_detalle: str
    fecha: date
    hora: time
    grupo_codigo: str


class DetalleCreate(DetalleBase):
    @field_validator('fecha', mode='before')
    @classmethod
    def parse_date(cls, v):
        if isinstance(v, str):
            # Handle ISO date format
            try:
                return datetime.fromisoformat(v).date()
            except ValueError:
                # Try other common date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']:
                    try:
                        return datetime.strptime(v, fmt).date()
                    except ValueError:
                        continue
                raise ValueError(f"Invalid date format: {v}")
        return v
    
    @field_validator('hora', mode='before')
    @classmethod
    def parse_time(cls, v):
        if isinstance(v, str):
            # Handle both HH:MM:SS and HH:MM:SS.sss formats and ISO time formats
            if 'T' in v:  # ISO format like "19:11:49.377Z"
                v = v.split('T')[-1].rstrip('Z')
            if '.' in v:
                v = v.split('.')[0]  # Remove milliseconds
            time_parts = v.split(':')
            return time(
                int(time_parts[0]), 
                int(time_parts[1]), 
                int(time_parts[2]) if len(time_parts) > 2 else 0
            )
        return v


class DetalleUpdate(BaseModel):
    fecha: Optional[date] = None
    hora: Optional[time] = None
    grupo_codigo: Optional[str] = None
    
    @field_validator('fecha', mode='before')
    @classmethod
    def parse_date(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            # Handle ISO date format
            try:
                return datetime.fromisoformat(v).date()
            except ValueError:
                # Try other common date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y']:
                    try:
                        return datetime.strptime(v, fmt).date()
                    except ValueError:
                        continue
                raise ValueError(f"Invalid date format: {v}")
        return v
    
    @field_validator('hora', mode='before')
    @classmethod
    def parse_time(cls, v):
        if v is None:
            return v
        if isinstance(v, str):
            # Handle both HH:MM:SS and HH:MM:SS.sss formats and ISO time formats
            if 'T' in v:  # ISO format like "19:11:49.377Z"
                v = v.split('T')[-1].rstrip('Z')
            if '.' in v:
                v = v.split('.')[0]  # Remove milliseconds
            time_parts = v.split(':')
            return time(
                int(time_parts[0]), 
                int(time_parts[1]), 
                int(time_parts[2]) if len(time_parts) > 2 else 0
            )
        return v


class DetalleInDB(DetalleBase):
    model_config = ConfigDict(from_attributes=True)
    created_at: datetime
    updated_at: datetime


class Detalle(DetalleInDB):
    pass


class DetalleWithRelations(Detalle):
    grupo: Optional["Grupo"] = None
