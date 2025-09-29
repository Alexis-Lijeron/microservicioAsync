from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.plan_estudio import PlanEstudioCreate, PlanEstudioUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_planes_estudio_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todos los planes_estudio - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.plan_estudio import plan_estudio
        planes_estudio_list = await plan_estudio.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        planes_estudio_data = [
            {
                "codigo": p.codigo,
                "cant_semestre": p.cant_semestre,
                "plan": p.plan,
                "carrera_codigo": p.carrera_codigo,
            }
            for p in planes_estudio_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_planes_estudio_list", task_data)
        
        return {
            "success": True,
            "data": planes_estudio_data,
            "total": len(planes_estudio_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo}", response_model=dict)
async def read_plan_estudio_async(
    codigo: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener plan_estudio específico - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.plan_estudio import plan_estudio
        plan_estudio_obj = await plan_estudio.get(db, code=codigo)
        
        if not plan_estudio_obj:
            raise HTTPException(status_code=404, detail="Plan de estudio no encontrado")
        
        # Preparar datos de respuesta
        plan_estudio_data = {
            "codigo": plan_estudio_obj.codigo,
            "cant_semestre": plan_estudio_obj.cant_semestre,
            "plan": plan_estudio_obj.plan,
            "carrera_codigo": plan_estudio_obj.carrera_codigo,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo": codigo}
        task_id = await optimized_thread_queue_manager.add_task("get_plan_estudio", task_data)
        
        return {
            "success": True,
            "data": plan_estudio_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_plan_estudio_async(
    plan_estudio: PlanEstudioCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear plan_estudio de forma asíncrona"""
    try:
        task_data = plan_estudio.dict()
        task_id = await optimized_thread_queue_manager.add_task("create_plan_estudio", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo}", response_model=dict)
async def update_plan_estudio_async(
    codigo: str,
    plan_estudio: PlanEstudioUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar plan_estudio de forma asíncrona"""
    try:
        task_data = {"codigo": codigo, "update_data": plan_estudio.dict(exclude_unset=True)}
        task_id = await optimized_thread_queue_manager.add_task("update_plan_estudio", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo}", response_model=dict)
async def delete_plan_estudio_async(
    codigo: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar plan_estudio de forma asíncrona"""
    try:
        task_data = {"codigo": codigo}
        task_id = await optimized_thread_queue_manager.add_task("delete_plan_estudio", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))