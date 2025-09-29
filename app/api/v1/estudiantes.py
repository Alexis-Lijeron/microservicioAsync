from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.estudiante import EstudianteCreate, EstudianteUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_estudiantes_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todos los estudiantes - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.estudiante import estudiante
        estudiantes_list = await estudiante.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        estudiantes_data = [
            {
                "registro": e.registro,
                "nombre": e.nombre,
                "apellido": e.apellido,
                "ci": e.ci,
                "carrera_codigo": e.carrera_codigo,
            }
            for e in estudiantes_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_estudiantes_list", task_data)
        
        return {
            "success": True,
            "data": estudiantes_data,
            "total": len(estudiantes_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{registro}", response_model=dict)
async def read_estudiante_async(
    registro: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener estudiante específico - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.estudiante import estudiante
        estudiante_obj = await estudiante.get(db, code=registro)
        
        if not estudiante_obj:
            raise HTTPException(status_code=404, detail="Estudiante no encontrado")
        
        # Preparar datos de respuesta
        estudiante_data = {
            "registro": estudiante_obj.registro,
            "nombre": estudiante_obj.nombre,
            "apellido": estudiante_obj.apellido,
            "ci": estudiante_obj.ci,
            "carrera_codigo": estudiante_obj.carrera_codigo,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"registro": registro}
        task_id = await optimized_thread_queue_manager.add_task("get_estudiante", task_data)
        
        return {
            "success": True,
            "data": estudiante_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_estudiante_async(
    estudiante: EstudianteCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear estudiante de forma asíncrona"""
    try:
        task_data = estudiante.dict()
        task_id = await optimized_thread_queue_manager.add_task("create_estudiante", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{registro}", response_model=dict)
async def update_estudiante_async(
    registro: str,
    estudiante: EstudianteUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar estudiante de forma asíncrona"""
    try:
        task_data = {"registro": registro, "update_data": estudiante.dict(exclude_unset=True)}
        task_id = await optimized_thread_queue_manager.add_task("update_estudiante", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{registro}", response_model=dict)
async def delete_estudiante_async(
    registro: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar estudiante de forma asíncrona"""
    try:
        task_data = {"registro": registro}
        task_id = await optimized_thread_queue_manager.add_task("delete_estudiante", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))