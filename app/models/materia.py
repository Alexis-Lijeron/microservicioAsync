from sqlalchemy import Column, String, Integer, ForeignKey, Boolean
from sqlalchemy.orm import relationship
from .base import BaseModel


class Materia(BaseModel):
    __tablename__ = "materias"

    sigla = Column(String(20), primary_key=True, index=True)
    nombre = Column(String(200), nullable=False)
    creditos = Column(Integer, nullable=False)
    es_electiva = Column(Boolean, default=False)
    nivel_codigo = Column(String(20), ForeignKey("niveles.codigo_nivel"), nullable=False)
    plan_estudio_codigo = Column(String(20), ForeignKey("planes_estudio.codigo"), nullable=False)

    # Relationships
    nivel = relationship("Nivel", back_populates="materias")
    plan_estudio = relationship("PlanEstudio", back_populates="materias")
    grupos = relationship("Grupo", back_populates="materia")

    # Solo mantenemos la relación simple con prerrequisitos
    prerrequisitos_como_materia = relationship(
        "Prerrequisito",
        foreign_keys="Prerrequisito.materia_sigla",
        back_populates="materia",
    )
