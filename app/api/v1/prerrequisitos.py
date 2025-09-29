from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.prerrequisito import PrerequisitoCreate, PrerequisitoUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_prerrequisitos_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todos los prerrequisitos - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.prerrequisito import prerrequisito
        prerrequisitos_list = await prerrequisito.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        prerrequisitos_data = [
            {
                "codigo_prerrequisito": p.codigo_prerrequisito,
                "materia_sigla": p.materia_sigla,
                "sigla_prerrequisito": p.sigla_prerrequisito,
            }
            for p in prerrequisitos_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_prerrequisitos_list", task_data)
        
        return {
            "success": True,
            "data": prerrequisitos_data,
            "total": len(prerrequisitos_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo_prerrequisito}", response_model=dict)
async def read_prerrequisito_async(
    codigo_prerrequisito: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener prerrequisito específico - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.prerrequisito import prerrequisito
        prerrequisito_obj = await prerrequisito.get(db, code=codigo_prerrequisito)
        
        if not prerrequisito_obj:
            raise HTTPException(status_code=404, detail="Prerrequisito no encontrado")
        
        # Preparar datos de respuesta
        prerrequisito_data = {
            "codigo_prerrequisito": prerrequisito_obj.codigo_prerrequisito,
            "materia_sigla": prerrequisito_obj.materia_sigla,
            "sigla_prerrequisito": prerrequisito_obj.sigla_prerrequisito,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo_prerrequisito": codigo_prerrequisito}
        task_id = await optimized_thread_queue_manager.add_task("get_prerrequisito", task_data)
        
        return {
            "success": True,
            "data": prerrequisito_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_prerrequisito_async(
    prerrequisito: PrerequisitoCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear prerrequisito de forma asíncrona"""
    try:
        task_data = prerrequisito.dict()
        task_id = await optimized_thread_queue_manager.add_task("create_prerrequisito", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo_prerrequisito}", response_model=dict)
async def update_prerrequisito_async(
    codigo_prerrequisito: str,
    prerrequisito: PrerequisitoUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar prerrequisito de forma asíncrona"""
    try:
        task_data = {"codigo_prerrequisito": codigo_prerrequisito, **prerrequisito.dict(exclude_unset=True)}
        task_id = await optimized_thread_queue_manager.add_task("update_prerrequisito", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo_prerrequisito}", response_model=dict)
async def delete_prerrequisito_async(
    codigo_prerrequisito: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar prerrequisito de forma asíncrona"""
    try:
        task_data = {"codigo_prerrequisito": codigo_prerrequisito}
        task_id = await optimized_thread_queue_manager.add_task("delete_prerrequisito", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))