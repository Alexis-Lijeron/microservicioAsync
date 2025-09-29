from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.inscripcion import InscripcionCreate, InscripcionUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_inscripciones_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todos los inscripciones - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.inscripcion import inscripcion
        inscripciones_list = await inscripcion.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        inscripciones_data = [
            {
                "codigo_inscripcion": i.codigo_inscripcion,
                "semestre": i.semestre,
                "gestion_codigo": i.gestion_codigo,
                "estudiante_registro": i.estudiante_registro,
                "grupo_codigo": i.grupo_codigo,
            }
            for i in inscripciones_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_inscripciones_list", task_data)
        
        return {
            "success": True,
            "data": inscripciones_data,
            "total": len(inscripciones_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo_inscripcion}", response_model=dict)
async def read_inscripcion_async(
    codigo_inscripcion: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener inscripcion específico - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.inscripcion import inscripcion
        inscripcion_obj = await inscripcion.get(db, code=codigo_inscripcion)
        
        if not inscripcion_obj:
            raise HTTPException(status_code=404, detail="Inscripción no encontrada")
        
        # Preparar datos de respuesta
        inscripcion_data = {
            "codigo_inscripcion": inscripcion_obj.codigo_inscripcion,
            "semestre": inscripcion_obj.semestre,
            "gestion_codigo": inscripcion_obj.gestion_codigo,
            "estudiante_registro": inscripcion_obj.estudiante_registro,
            "grupo_codigo": inscripcion_obj.grupo_codigo,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo_inscripcion": codigo_inscripcion}
        task_id = await optimized_thread_queue_manager.add_task("get_inscripcion", task_data)
        
        return {
            "success": True,
            "data": inscripcion_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_inscripcion_async(
    inscripcion: InscripcionCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear inscripcion de forma asíncrona"""
    try:
        task_data = inscripcion.dict()
        task_id = await optimized_thread_queue_manager.add_task("create_inscripcion", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo_inscripcion}", response_model=dict)
async def update_inscripcion_async(
    codigo_inscripcion: str,
    inscripcion: InscripcionUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar inscripcion de forma asíncrona"""
    try:
        task_data = {"codigo_inscripcion": codigo_inscripcion, "update_data": inscripcion.dict(exclude_unset=True)}
        task_id = await optimized_thread_queue_manager.add_task("update_inscripcion", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo_inscripcion}", response_model=dict)
async def delete_inscripcion_async(
    codigo_inscripcion: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar inscripcion de forma asíncrona"""
    try:
        task_data = {"codigo_inscripcion": codigo_inscripcion}
        task_id = await optimized_thread_queue_manager.add_task("delete_inscripcion", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))