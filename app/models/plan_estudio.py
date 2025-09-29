from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from .base import BaseModel


class PlanEstudio(BaseModel):
    __tablename__ = "planes_estudio"

    codigo = Column(String(20), primary_key=True, index=True)
    cant_semestre = Column(Integer, nullable=False)
    plan = Column(String(100), nullable=False)
    carrera_codigo = Column(String(10), ForeignKey("carreras.codigo"), nullable=False)

    # Relationships
    carrera = relationship("Carrera", back_populates="planes_estudio")
    materias = relationship("Materia", back_populates="plan_estudio")
