from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from .base import BaseModel


class Inscripcion(BaseModel):
    __tablename__ = "inscripciones"

    codigo_inscripcion = Column(String(20), primary_key=True, index=True)
    semestre = Column(Integer, nullable=False)
    gestion_codigo = Column(String(20), ForeignKey("gestiones.codigo_gestion"), nullable=False)
    estudiante_registro = Column(String(20), ForeignKey("estudiantes.registro"), nullable=False)
    grupo_codigo = Column(String(20), ForeignKey("grupos.codigo_grupo"), nullable=False)

    # Relationships
    gestion = relationship("Gestion", back_populates="inscripciones")
    estudiante = relationship("Estudiante", back_populates="inscripciones")
    grupo = relationship("Grupo", back_populates="inscripciones")
