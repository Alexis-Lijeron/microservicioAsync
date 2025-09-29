from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.detalle import DetalleCreate, DetalleUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_detalles_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todos los detalles - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.detalle import detalle
        detalles_list = await detalle.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        detalles_data = [
            {
                "codigo_detalle": d.codigo_detalle,
                "fecha": d.fecha.isoformat() if d.fecha else None,
                "hora": d.hora.isoformat() if d.hora else None,
                "grupo_codigo": d.grupo_codigo,
            }
            for d in detalles_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_detalles_list", task_data)
        
        return {
            "success": True,
            "data": detalles_data,
            "total": len(detalles_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo_detalle}", response_model=dict)
async def read_detalle_async(
    codigo_detalle: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener detalle específico - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.detalle import detalle
        detalle_obj = await detalle.get(db, code=codigo_detalle)
        
        if not detalle_obj:
            raise HTTPException(status_code=404, detail="Detalle no encontrado")
        
        # Preparar datos de respuesta
        detalle_data = {
            "codigo_detalle": detalle_obj.codigo_detalle,
            "fecha": detalle_obj.fecha.isoformat() if detalle_obj.fecha else None,
            "hora": detalle_obj.hora.isoformat() if detalle_obj.hora else None,
            "grupo_codigo": detalle_obj.grupo_codigo,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo_detalle": codigo_detalle}
        task_id = await optimized_thread_queue_manager.add_task("get_detalle", task_data)
        
        return {
            "success": True,
            "data": detalle_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_detalle_async(
    detalle: DetalleCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear detalle de forma asíncrona"""
    try:
        # Convert to dict and serialize date/time objects to strings
        task_data = detalle.dict()
        
        # Handle date serialization
        if 'fecha' in task_data and task_data['fecha']:
            if hasattr(task_data['fecha'], 'isoformat'):
                task_data['fecha'] = task_data['fecha'].isoformat()
                
        # Handle time serialization  
        if 'hora' in task_data and task_data['hora']:
            if hasattr(task_data['hora'], 'isoformat'):
                task_data['hora'] = task_data['hora'].isoformat()
            elif hasattr(task_data['hora'], 'strftime'):
                task_data['hora'] = task_data['hora'].strftime("%H:%M:%S")
                
        task_id = await optimized_thread_queue_manager.add_task("create_detalle", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo_detalle}", response_model=dict)
async def update_detalle_async(
    codigo_detalle: str,
    detalle: DetalleUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar detalle de forma asíncrona"""
    try:
        update_data = detalle.dict(exclude_unset=True)
        
        # Handle date serialization
        if 'fecha' in update_data and update_data['fecha']:
            if hasattr(update_data['fecha'], 'isoformat'):
                update_data['fecha'] = update_data['fecha'].isoformat()
                
        # Handle time serialization  
        if 'hora' in update_data and update_data['hora']:
            if hasattr(update_data['hora'], 'isoformat'):
                update_data['hora'] = update_data['hora'].isoformat()
            elif hasattr(update_data['hora'], 'strftime'):
                update_data['hora'] = update_data['hora'].strftime("%H:%M:%S")
        
        task_data = {"codigo_detalle": codigo_detalle, "update_data": update_data}
        task_id = await optimized_thread_queue_manager.add_task("update_detalle", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo_detalle}", response_model=dict)
async def delete_detalle_async(
    codigo_detalle: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar detalle de forma asíncrona"""
    try:
        task_data = {"codigo_detalle": codigo_detalle}
        task_id = await optimized_thread_queue_manager.add_task("delete_detalle", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))