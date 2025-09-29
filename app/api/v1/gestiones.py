from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.gestion import GestionCreate, GestionUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_gestiones_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todos los gestiones - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.gestion import gestion
        gestiones_list = await gestion.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        gestiones_data = [
            {
                "codigo_gestion": g.codigo_gestion,
                "semestre": g.semestre,
                "año": g.año,
            }
            for g in gestiones_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_gestiones_list", task_data)
        
        return {
            "success": True,
            "data": gestiones_data,
            "total": len(gestiones_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo_gestion}", response_model=dict)
async def read_gestion_async(
    codigo_gestion: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener gestion específico - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.gestion import gestion
        gestion_obj = await gestion.get(db, code=codigo_gestion)
        
        if not gestion_obj:
            raise HTTPException(status_code=404, detail="Gestión no encontrada")
        
        # Preparar datos de respuesta
        gestion_data = {
            "codigo_gestion": gestion_obj.codigo_gestion,
            "semestre": gestion_obj.semestre,
            "año": gestion_obj.año,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo_gestion": codigo_gestion}
        task_id = await optimized_thread_queue_manager.add_task("get_gestion", task_data)
        
        return {
            "success": True,
            "data": gestion_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_gestion_async(
    gestion: GestionCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear gestion de forma asíncrona"""
    try:
        task_data = gestion.dict()
        task_id = await optimized_thread_queue_manager.add_task("create_gestion", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo_gestion}", response_model=dict)
async def update_gestion_async(
    codigo_gestion: str,
    gestion: GestionUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar gestion de forma asíncrona"""
    try:
        task_data = {"codigo_gestion": codigo_gestion, "update_data": gestion.dict(exclude_unset=True)}
        task_id = await optimized_thread_queue_manager.add_task("update_gestion", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo_gestion}", response_model=dict)
async def delete_gestion_async(
    codigo_gestion: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar gestion de forma asíncrona"""
    try:
        task_data = {"codigo_gestion": codigo_gestion}
        task_id = await optimized_thread_queue_manager.add_task("delete_gestion", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))