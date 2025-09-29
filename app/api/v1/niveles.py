from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.nivel import NivelCreate, NivelUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_niveles_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todos los niveles - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.nivel import nivel
        niveles_list = await nivel.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        niveles_data = [
            {
                "codigo_nivel": n.codigo_nivel,
                "nivel": n.nivel,
            }
            for n in niveles_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_niveles_list", task_data)
        
        return {
            "success": True,
            "data": niveles_data,
            "total": len(niveles_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo}", response_model=dict)
async def read_nivel_async(
    codigo: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener nivel específico - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.nivel import nivel
        nivel_obj = await nivel.get(db, code=codigo)
        
        if not nivel_obj:
            raise HTTPException(status_code=404, detail="Nivel no encontrado")
        
        # Preparar datos de respuesta
        nivel_data = {
            "codigo_nivel": nivel_obj.codigo_nivel,
            "nivel": nivel_obj.nivel,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo_nivel": codigo}
        task_id = await optimized_thread_queue_manager.add_task("get_nivel", task_data)
        
        return {
            "success": True,
            "data": nivel_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_nivel_async(
    nivel: NivelCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear nivel de forma asíncrona"""
    try:
        task_data = nivel.model_dump()
        task_id = await optimized_thread_queue_manager.add_task("create_nivel", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo}", response_model=dict)
async def update_nivel_async(
    codigo: str,
    nivel: NivelUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar nivel de forma asíncrona"""
    try:
        task_data = nivel.model_dump()
        task_data["codigo_nivel"] = codigo
        task_id = await optimized_thread_queue_manager.add_task("update_nivel", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo}", response_model=dict)
async def delete_nivel_async(
    codigo: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar nivel de forma asíncrona"""
    try:
        task_data = {"codigo_nivel": codigo}
        task_id = await optimized_thread_queue_manager.add_task("delete_nivel", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))