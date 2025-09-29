from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from .base import BaseModel


class Nivel(BaseModel):
    __tablename__ = "niveles"

    codigo_nivel = Column(String(20), primary_key=True, index=True)
    nivel = Column(Integer, nullable=False, unique=True)

    # Relationships
    materias = relationship("Materia", back_populates="nivel")
