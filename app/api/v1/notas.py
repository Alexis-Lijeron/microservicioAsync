from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.nota import NotaCreate, NotaUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_notas_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todos los notas - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.nota import nota
        notas_list = await nota.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        notas_data = [
            {
                "codigo_nota": n.codigo_nota,
                "nota": float(n.nota) if n.nota else None,
                "estudiante_registro": n.estudiante_registro,
            }
            for n in notas_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_notas_list", task_data)
        
        return {
            "success": True,
            "data": notas_data,
            "total": len(notas_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo_nota}", response_model=dict)
async def read_nota_async(
    codigo_nota: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener nota específico - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.nota import nota
        nota_obj = await nota.get(db, code=codigo_nota)
        
        if not nota_obj:
            raise HTTPException(status_code=404, detail="Nota no encontrada")
        
        # Preparar datos de respuesta
        nota_data = {
            "codigo_nota": nota_obj.codigo_nota,
            "nota": float(nota_obj.nota) if nota_obj.nota else None,
            "estudiante_registro": nota_obj.estudiante_registro,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo_nota": codigo_nota}
        task_id = await optimized_thread_queue_manager.add_task("get_nota", task_data)
        
        return {
            "success": True,
            "data": nota_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_nota_async(
    nota: NotaCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear nota de forma asíncrona"""
    try:
        task_data = nota.dict()
        task_id = await optimized_thread_queue_manager.add_task("create_nota", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo_nota}", response_model=dict)
async def update_nota_async(
    codigo_nota: str,
    nota: NotaUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar nota de forma asíncrona"""
    try:
        task_data = {"codigo_nota": codigo_nota, "update_data": nota.dict(exclude_unset=True)}
        task_id = await optimized_thread_queue_manager.add_task("update_nota", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo_nota}", response_model=dict)
async def delete_nota_async(
    codigo_nota: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar nota de forma asíncrona"""
    try:
        task_data = {"codigo_nota": codigo_nota}
        task_id = await optimized_thread_queue_manager.add_task("delete_nota", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))