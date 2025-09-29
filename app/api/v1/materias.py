from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.materia import MateriaCreate, MateriaUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_materias_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todas las materias - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.materia import materia
        materias_list = await materia.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        materias_data = [
            {
                "sigla": m.sigla,
                "nombre": m.nombre,
                "creditos": m.creditos,
                "es_electiva": m.es_electiva,
                "nivel_codigo": m.nivel_codigo,
                "plan_estudio_codigo": m.plan_estudio_codigo,
            }
            for m in materias_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_materias_list", task_data)
        
        return {
            "success": True,
            "data": materias_data,
            "total": len(materias_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{sigla}", response_model=dict)
async def read_materia_async(
    sigla: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener materia específica - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.materia import materia
        materia_obj = await materia.get(db, code=sigla)
        
        if not materia_obj:
            raise HTTPException(status_code=404, detail="Materia no encontrada")
        
        # Preparar datos de respuesta
        materia_data = {
            "sigla": materia_obj.sigla,
            "nombre": materia_obj.nombre,
            "creditos": materia_obj.creditos,
            "es_electiva": materia_obj.es_electiva,
            "nivel_codigo": materia_obj.nivel_codigo,
            "plan_estudio_codigo": materia_obj.plan_estudio_codigo,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"sigla": sigla}
        task_id = await optimized_thread_queue_manager.add_task("get_materia", task_data)
        
        return {
            "success": True,
            "data": materia_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_materia_async(
    materia: MateriaCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear materia de forma asíncrona"""
    try:
        task_data = materia.dict()
        task_id = await optimized_thread_queue_manager.add_task("create_materia", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{sigla}", response_model=dict)
async def update_materia_async(
    sigla: str,
    materia: MateriaUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar materia de forma asíncrona"""
    try:
        task_data = {"sigla": sigla, "update_data": materia.dict(exclude_unset=True)}
        task_id = await optimized_thread_queue_manager.add_task("update_materia", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{sigla}", response_model=dict)
async def delete_materia_async(
    sigla: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar materia de forma asíncrona"""
    try:
        task_data = {"sigla": sigla}
        task_id = await optimized_thread_queue_manager.add_task("delete_materia", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))