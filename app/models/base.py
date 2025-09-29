from sqlalchemy import Column, Integer, DateTime, func, String
from sqlalchemy.ext.declarative import declared_attr
from app.config.database import Base


class TimestampMixin:
    """Mixin para agregar campos de timestamp a los modelos"""

    @declared_attr
    def created_at(cls):
        return Column(DateTime(timezone=True), server_default=func.now())

    @declared_attr
    def updated_at(cls):
        return Column(
            DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
        )


class BaseModel(Base, TimestampMixin):
    """Modelo base con códigos únicos y timestamps"""

    __abstract__ = True
