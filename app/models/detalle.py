from sqlalchemy import Column, Date, Time, Integer, ForeignKey, String
from sqlalchemy.orm import relationship
from .base import BaseModel


class Detalle(BaseModel):
    __tablename__ = "detalles"

    codigo_detalle = Column(String(20), primary_key=True, index=True)
    fecha = Column(Date, nullable=False)
    hora = Column(Time, nullable=False)
    grupo_codigo = Column(String(20), ForeignKey("grupos.codigo_grupo"), nullable=False)

    # Relationships
    grupo = relationship("Grupo", back_populates="detalles")
