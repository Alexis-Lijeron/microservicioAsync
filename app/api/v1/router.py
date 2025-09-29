from fastapi import APIRouter

from app.api.v1 import (
    estudiantes, docentes, carreras, materias, grupos, inscripciones, 
    notas, aulas, horarios, gestiones, niveles, planes_estudio, 
    prerrequisitos, detalles, redis, queue_management
)

api_router = APIRouter()

# Endpoints principales del sistema académico
api_router.include_router(
    estudiantes.router, prefix="/estudiantes", tags=["estudiantes"]
)
api_router.include_router(docentes.router, prefix="/docentes", tags=["docentes"])
api_router.include_router(carreras.router, prefix="/carreras", tags=["carreras"])
api_router.include_router(materias.router, prefix="/materias", tags=["materias"])
api_router.include_router(grupos.router, prefix="/grupos", tags=["grupos"])
api_router.include_router(
    inscripciones.router, prefix="/inscripciones", tags=["inscripciones"]
)
api_router.include_router(notas.router, prefix="/notas", tags=["notas"])
api_router.include_router(aulas.router, prefix="/aulas", tags=["aulas"])
api_router.include_router(horarios.router, prefix="/horarios", tags=["horarios"])
api_router.include_router(gestiones.router, prefix="/gestiones", tags=["gestiones"])
api_router.include_router(niveles.router, prefix="/niveles", tags=["niveles"])
api_router.include_router(
    planes_estudio.router, prefix="/planes-estudio", tags=["planes-estudio"]
)
api_router.include_router(
    prerrequisitos.router, prefix="/prerrequisitos", tags=["prerrequisitos"]
)
api_router.include_router(detalles.router, prefix="/detalles", tags=["detalles"])

# Rutas para sistema de colas y dashboard
api_router.include_router(redis.router, prefix="/redis", tags=["redis"])
api_router.include_router(queue_management.router, prefix="/queue", tags=["queue"])
