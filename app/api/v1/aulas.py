from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.aula import AulaCreate, AulaUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_aulas_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todas las aulas - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.aula import aula
        aulas_list = await aula.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        aulas_data = [
            {
                "codigo_aula": a.codigo_aula,
                "modulo": a.modulo,
                "aula": a.aula,
            }
            for a in aulas_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_aulas_list", task_data)
        
        return {
            "success": True,
            "data": aulas_data,
            "total": len(aulas_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo_aula}", response_model=dict)
async def read_aula_async(
    codigo_aula: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener aula específica - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.aula import aula
        aula_obj = await aula.get(db, code=codigo_aula)
        
        if not aula_obj:
            raise HTTPException(status_code=404, detail="Aula no encontrada")
        
        # Preparar datos de respuesta
        aula_data = {
            "codigo_aula": aula_obj.codigo_aula,
            "modulo": aula_obj.modulo,
            "aula": aula_obj.aula,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo_aula": codigo_aula}
        task_id = await optimized_thread_queue_manager.add_task("get_aula", task_data)
        
        return {
            "success": True,
            "data": aula_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_aula_async(
    aula: AulaCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear aula de forma asíncrona"""
    try:
        task_data = aula.dict()
        task_id = await optimized_thread_queue_manager.add_task("create_aula", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo_aula}", response_model=dict)
async def update_aula_async(
    codigo_aula: str,
    aula: AulaUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar aula de forma asíncrona"""
    try:
        task_data = {"codigo_aula": codigo_aula, "update_data": aula.dict(exclude_unset=True)}
        task_id = await optimized_thread_queue_manager.add_task("update_aula", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo_aula}", response_model=dict)
async def delete_aula_async(
    codigo_aula: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar aula de forma asíncrona"""
    try:
        task_data = {"codigo_aula": codigo_aula}
        task_id = await optimized_thread_queue_manager.add_task("delete_aula", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))