from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_active_user
from app.config.database import get_db
from app.schemas.horario import HorarioCreate, HorarioUpdate
from app.core.thread_queue import optimized_thread_queue_manager

router = APIRouter()


@router.get("/", response_model=dict)
async def read_horarios_async(
    skip: int = Query(default=0, ge=0),
    limit: int = Query(default=100, ge=1, le=1000),
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener todos los horarios - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.horario import horario
        horarios_list = await horario.get_multi(db, skip=skip, limit=limit)
        
        # Preparar datos de respuesta
        horarios_data = [
            {
                "codigo_horario": h.codigo_horario,
                "dia": h.dia,
                "hora_inicio": h.hora_inicio.isoformat() if h.hora_inicio else None,
                "hora_final": h.hora_final.isoformat() if h.hora_final else None,
                "aula_codigo": h.aula_codigo,
            }
            for h in horarios_list
        ]
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"skip": skip, "limit": limit}
        task_id = await optimized_thread_queue_manager.add_task("get_horarios_list", task_data)
        
        return {
            "success": True,
            "data": horarios_data,
            "total": len(horarios_data),
            "skip": skip,
            "limit": limit,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{codigo_horario}", response_model=dict)
async def read_horario_async(
    codigo_horario: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Obtener horario específico - devuelve datos directamente y registra en cola"""
    try:
        # Ejecutar consulta directamente
        from app.crud.horario import horario
        horario_obj = await horario.get(db, code=codigo_horario)
        
        if not horario_obj:
            raise HTTPException(status_code=404, detail="Horario no encontrado")
        
        # Preparar datos de respuesta
        horario_data = {
            "codigo_horario": horario_obj.codigo_horario,
            "dia": horario_obj.dia,
            "hora_inicio": horario_obj.hora_inicio.isoformat() if horario_obj.hora_inicio else None,
            "hora_final": horario_obj.hora_final.isoformat() if horario_obj.hora_final else None,
        }
        
        # Registrar en cola para auditoría (sin esperar)
        task_data = {"codigo_horario": codigo_horario}
        task_id = await optimized_thread_queue_manager.add_task("get_horario", task_data)
        
        return {
            "success": True,
            "data": horario_data,
            "_audit": {"task_id": task_id, "status": "logged"}
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=dict)
async def create_horario_async(
    horario: HorarioCreate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Crear horario de forma asíncrona"""
    try:
        task_data = horario.dict()
        # Convert time objects to strings for JSON serialization
        if 'hora_inicio' in task_data and task_data['hora_inicio']:
            task_data['hora_inicio'] = task_data['hora_inicio'].strftime("%H:%M:%S")
        if 'hora_final' in task_data and task_data['hora_final']:
            task_data['hora_final'] = task_data['hora_final'].strftime("%H:%M:%S")
        
        task_id = await optimized_thread_queue_manager.add_task("create_horario", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/{codigo_horario}", response_model=dict)
async def update_horario_async(
    codigo_horario: str,
    horario: HorarioUpdate,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Actualizar horario de forma asíncrona"""
    try:
        update_data = horario.dict(exclude_unset=True)
        # Convert time objects to strings for JSON serialization
        if 'hora_inicio' in update_data and update_data['hora_inicio']:
            update_data['hora_inicio'] = update_data['hora_inicio'].strftime("%H:%M:%S")
        if 'hora_final' in update_data and update_data['hora_final']:
            update_data['hora_final'] = update_data['hora_final'].strftime("%H:%M:%S")
        
        task_data = {"codigo_horario": codigo_horario, "update_data": update_data}
        task_id = await optimized_thread_queue_manager.add_task("update_horario", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{codigo_horario}", response_model=dict)
async def delete_horario_async(
    codigo_horario: str,
    db: AsyncSession = Depends(get_db),
    current_user=Depends(get_current_active_user),
):
    """Eliminar horario de forma asíncrona"""
    try:
        task_data = {"codigo_horario": codigo_horario}
        task_id = await optimized_thread_queue_manager.add_task("delete_horario", task_data)
        return {"task_id": task_id, "status": "processing"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))