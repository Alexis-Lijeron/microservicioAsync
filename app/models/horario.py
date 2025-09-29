from sqlalchemy import Column, String, Integer, ForeignKey, Time
from sqlalchemy.orm import relationship
from .base import BaseModel


class Horario(BaseModel):
    __tablename__ = "horarios"

    codigo_horario = Column(String(20), primary_key=True, index=True)
    dia = Column(String(20), nullable=False)
    hora_inicio = Column(Time, nullable=False)
    hora_final = Column(Time, nullable=False)
    aula_codigo = Column(String(20), ForeignKey("aulas.codigo_aula"), nullable=False)

    # Relationships
    aula = relationship("Aula", back_populates="horarios")
    grupos = relationship("Grupo", back_populates="horario")
