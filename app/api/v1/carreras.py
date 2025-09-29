from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.carrera import CarreraCreate, CarreraUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_carreras_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todas las carreras - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.carrera import carrera
        carreras_list = await carrera.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        carreras_data = [
            {
                "codigo": c.codigo,
                "nombre": c.nombre,
            }
            for c in carreras_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_carreras_list", task_data)
        
        return {
            "success": True,
            "data": carreras_data,
            "total": len(carreras_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo}", response_model=dict)
async def read_carrera_async(
    codigo: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener carrera específica - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.carrera import carrera
        carrera_obj = await carrera.get(db, code=codigo)
        
        if not carrera_obj:
            raise HTTPException(status_code=404, detail="Carrera no encontrada")
        
        # Preparar datos de respuesta
        carrera_data = {
            "codigo": carrera_obj.codigo,
            "nombre": carrera_obj.nombre,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo": codigo}
        task_id = await optimized_thread_queue_manager.add_task("get_carrera", task_data)
        
        return {
            "success": True,
            "data": carrera_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_carrera_async(
    carrera: CarreraCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear carrera de forma asincrona"""
    try:
        task_data = carrera.dict()
        task_id = await optimized_thread_queue_manager.add_task("create_carrera", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo}", response_model=dict)
async def update_carrera_async(
    codigo: str,
    carrera: CarreraUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar carrera de forma asincrona"""
    try:
        task_data = {"codigo": codigo, "update_data": carrera.dict(exclude_unset=True)}
        task_id = await optimized_thread_queue_manager.add_task("update_carrera", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo}", response_model=dict)
async def delete_carrera_async(
    codigo: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar carrera de forma asincrona"""
    try:
        task_data = {"codigo": codigo}
        task_id = await optimized_thread_queue_manager.add_task("delete_carrera", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
