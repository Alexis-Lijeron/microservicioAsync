from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from .base import BaseModel


class Prerrequisito(BaseModel):
    __tablename__ = "prerrequisitos"

    codigo_prerrequisito = Column(String(20), primary_key=True, index=True)
    materia_sigla = Column(String(20), ForeignKey("materias.sigla"), nullable=False)
    sigla_prerrequisito = Column(String(20), nullable=False)

    # Solo una relación simple con la materia principal
    materia = relationship(
        "Materia",
        foreign_keys=[materia_sigla],
        back_populates="prerrequisitos_como_materia",
    )
