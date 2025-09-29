from sqlalchemy import Column, String, Integer, ForeignKey
from sqlalchemy.orm import relationship
from .base import BaseModel


class Grupo(BaseModel):
    __tablename__ = "grupos"

    codigo_grupo = Column(String(20), primary_key=True, index=True)
    descripcion = Column(String(100), nullable=False)
    docente_codigo = Column(String(20), ForeignKey("docentes.codigo_docente"), nullable=False)
    gestion_codigo = Column(String(20), ForeignKey("gestiones.codigo_gestion"), nullable=False)
    materia_sigla = Column(String(20), ForeignKey("materias.sigla"), nullable=False)
    horario_codigo = Column(String(20), ForeignKey("horarios.codigo_horario"), nullable=False)

    # Relationships
    docente = relationship("Docente", back_populates="grupos")
    gestion = relationship("Gestion", back_populates="grupos")
    materia = relationship("Materia", back_populates="grupos")
    horario = relationship("Horario", back_populates="grupos")
    inscripciones = relationship("Inscripcion", back_populates="grupo")
    detalles = relationship("Detalle", back_populates="grupo")
