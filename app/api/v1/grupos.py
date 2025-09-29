from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.grupo import GrupoCreate, GrupoUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_grupos_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todos los grupos - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.grupo import grupo
        grupos_list = await grupo.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        grupos_data = [
            {
                "codigo_grupo": g.codigo_grupo,
                "descripcion": g.descripcion,
                "materia_sigla": g.materia_sigla,
                "docente_codigo": g.docente_codigo,
                "gestion_codigo": g.gestion_codigo,
                "horario_codigo": g.horario_codigo,
            }
            for g in grupos_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_grupos_list", task_data)
        
        return {
            "success": True,
            "data": grupos_data,
            "total": len(grupos_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo}", response_model=dict)
async def read_grupo_async(
    codigo: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener grupo específico - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.grupo import grupo
        grupo_obj = await grupo.get(db, code=codigo)
        
        if not grupo_obj:
            raise HTTPException(status_code=404, detail="Grupo no encontrado")
        
        # Preparar datos de respuesta
        grupo_data = {
            "codigo_grupo": grupo_obj.codigo_grupo,
            "descripcion": grupo_obj.descripcion,
            "materia_sigla": grupo_obj.materia_sigla,
            "docente_codigo": grupo_obj.docente_codigo,
            "gestion_codigo": grupo_obj.gestion_codigo,
            "horario_codigo": grupo_obj.horario_codigo,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo_grupo": codigo}
        task_id = await optimized_thread_queue_manager.add_task("get_grupo", task_data)
        
        return {
            "success": True,
            "data": grupo_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_grupo_async(
    grupo: GrupoCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear grupo de forma asíncrona"""
    try:
        task_data = grupo.dict()
        task_id = await optimized_thread_queue_manager.add_task("create_grupo", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo}", response_model=dict)
async def update_grupo_async(
    codigo: str,
    grupo: GrupoUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar grupo de forma asíncrona"""
    try:
        task_data = {"codigo": codigo, "update_data": grupo.dict(exclude_unset=True)}
        task_id = await optimized_thread_queue_manager.add_task("update_grupo", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo}", response_model=dict)
async def delete_grupo_async(
    codigo: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar grupo de forma asíncrona"""
    try:
        task_data = {"codigo": codigo}
        task_id = await optimized_thread_queue_manager.add_task("delete_grupo", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))