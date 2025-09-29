from sqlalchemy import Column, Float, Integer, ForeignKey, String
from sqlalchemy.orm import relationship
from .base import BaseModel


class Nota(BaseModel):
    __tablename__ = "notas"

    codigo_nota = Column(String(20), primary_key=True, index=True)
    nota = Column(Float, nullable=False)
    estudiante_registro = Column(String(20), ForeignKey("estudiantes.registro"), nullable=False)

    # Relationships
    estudiante = relationship("Estudiante", back_populates="notas")
