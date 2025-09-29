from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.docente import DocenteCreate, DocenteUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_docentes_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todos los docentes - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.docente import docente
        docentes_list = await docente.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        docentes_data = [
            {
                "codigo_docente": d.codigo_docente,
                "nombre": d.nombre,
                "apellido": d.apellido,
            }
            for d in docentes_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_docentes_list", task_data)
        
        return {
            "success": True,
            "data": docentes_data,
            "total": len(docentes_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo}", response_model=dict)
async def read_docente_async(
    codigo: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener docente específico - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.docente import docente
        docente_obj = await docente.get(db, code=codigo)
        
        if not docente_obj:
            raise HTTPException(status_code=404, detail="Docente no encontrado")
        
        # Preparar datos de respuesta
        docente_data = {
            "codigo_docente": docente_obj.codigo_docente,
            "nombre": docente_obj.nombre,
            "apellido": docente_obj.apellido,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo_docente": codigo}
        task_id = await optimized_thread_queue_manager.add_task("get_docente", task_data)
        
        return {
            "success": True,
            "data": docente_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_docente_async(
    docente: DocenteCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear docente de forma asíncrona"""
    try:
        task_data = docente.dict()
        task_id = await optimized_thread_queue_manager.add_task("create_docente", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo}", response_model=dict)
async def update_docente_async(
    codigo: str,
    docente: DocenteUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar docente de forma asíncrona"""
    try:
        task_data = {"codigo": codigo, "update_data": docente.dict(exclude_unset=True)}
        task_id = await optimized_thread_queue_manager.add_task("update_docente", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo}", response_model=dict)
async def delete_docente_async(
    codigo: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar docente de forma asíncrona"""
    try:
        task_data = {"codigo": codigo}
        task_id = await optimized_thread_queue_manager.add_task("delete_docente", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))