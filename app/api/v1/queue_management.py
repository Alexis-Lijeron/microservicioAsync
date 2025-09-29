from typing import List, Optional
from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, Query, Path
from pydantic import BaseModel

from app.api.deps import get_current_active_user
from app.core.thread_queue import optimized_thread_queue_manager
from app.core.pagination_system import corrected_smart_paginator
from app.core.task_processors import TASK_PROCESSORS

router = APIRouter()


# Modelos Pydantic
class TaskCreate(BaseModel):
    task_type: str
    data: dict
    priority: Optional[int] = 5
    max_retries: Optional[int] = 3
    rollback_data: Optional[dict] = None


class TaskResponse(BaseModel):
    task_id: str
    message: str
    status: str = "pending"


class BulkTaskCreate(BaseModel):
    tasks: List[TaskCreate]


# Control de cola
@router.post("/start", tags=["Cola - Control"])
async def start_queue(
    max_workers: int = Query(4, ge=1, le=10),
    current_user=Depends(get_current_active_user),
):
    """Iniciar el sistema de colas"""
    try:
        await optimized_thread_queue_manager.start(max_workers=max_workers)
        return {
            "success": True,
            "message": f"Sistema de colas iniciado con {max_workers} workers",
            "status": "running",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/stop", tags=["Cola - Control"])
async def stop_queue(current_user=Depends(get_current_active_user)):
    """Detener el sistema de colas"""
    try:
        optimized_thread_queue_manager.stop()
        return {
            "success": True,
            "message": "Sistema de colas detenido",
            "status": "stopped",
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/status", tags=["Cola - Información"])
async def get_queue_status(current_user=Depends(get_current_active_user)):
    """Obtener estado y estadísticas de la cola"""
    stats = await optimized_thread_queue_manager.get_queue_stats()
    return stats


# Gestión de tareas
@router.get("/tasks", tags=["Cola - Tareas"])
async def get_tasks(
    status: Optional[str] = Query(None),
    task_type: Optional[str] = Query(None),
    session_id: Optional[str] = Query(None),
    page_size: int = Query(20, ge=1, le=100),
    current_user=Depends(get_current_active_user),
):
    """Obtener lista de tareas con paginación inteligente"""

    async def query_tasks(db, offset: int, limit: int, **kwargs):
        return await optimized_thread_queue_manager.get_tasks(
            status=status, task_type=task_type, skip=offset, limit=limit
        )

    if not session_id:
        import uuid

        session_id = str(uuid.uuid4())[:8]

    results, metadata = await corrected_smart_paginator.get_next_page(
        session_id=session_id,
        endpoint="queue_tasks",
        query_function=query_tasks,
        query_params={"status": status, "task_type": task_type},
        page_size=page_size,
    )

    return {"data": results, "pagination": metadata}


@router.post("/tasks", response_model=TaskResponse, tags=["Cola - Tareas"])
async def create_task(
    task_data: TaskCreate, current_user=Depends(get_current_active_user)
):
    """Crear nueva tarea en la cola"""
    try:
        if task_data.task_type not in TASK_PROCESSORS:
            raise HTTPException(
                status_code=400,
                detail=f"Tipo de tarea no soportado: {task_data.task_type}",
            )

        task_id = await optimized_thread_queue_manager.add_task(
            task_type=task_data.task_type,
            data=task_data.data,
            priority=task_data.priority,
            max_retries=task_data.max_retries,
            rollback_data=task_data.rollback_data,
        )

        return TaskResponse(
            task_id=task_id,
            message=f"Tarea {task_data.task_type} agregada a la cola",
            status="pending",
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks/{task_id}", tags=["Cola - Tareas"])
async def get_task_status(
    task_id: str = Path(...), current_user=Depends(get_current_active_user)
):
    """Obtener estado de una tarea específica"""
    task_status = await optimized_thread_queue_manager.get_task_status(task_id)

    if not task_status:
        raise HTTPException(status_code=404, detail="Tarea no encontrada")

    return task_status


@router.post("/tasks/{task_id}/cancel", tags=["Cola - Tareas"])
async def cancel_task(
    task_id: str = Path(...), current_user=Depends(get_current_active_user)
):
    """Cancelar una tarea"""
    success = await optimized_thread_queue_manager.cancel_task(task_id)

    if not success:
        raise HTTPException(
            status_code=404, detail="Tarea no encontrada o no se puede cancelar"
        )

    return {
        "success": True,
        "message": f"Tarea {task_id} cancelada",
        "task_id": task_id,
    }


@router.post("/tasks/{task_id}/retry", tags=["Cola - Tareas"])
async def retry_task(
    task_id: str = Path(...), current_user=Depends(get_current_active_user)
):
    """Reintentar una tarea fallida"""
    success = await optimized_thread_queue_manager.retry_task(task_id)

    if not success:
        raise HTTPException(status_code=400, detail="Tarea no se puede reintentar")

    return {
        "success": True,
        "message": f"Tarea {task_id} reintentada",
        "task_id": task_id,
    }


# Mantenimiento
@router.delete("/tasks/cleanup", tags=["Cola - Mantenimiento"])
async def cleanup_old_tasks(
    days_old: int = Query(7, ge=0, le=365),
    current_user=Depends(get_current_active_user),
):
    """Limpiar tareas antiguas completadas"""
    try:
        deleted_count = await optimized_thread_queue_manager.cleanup_old_tasks(days_old)
        return {
            "success": True,
            "message": f"{deleted_count} tareas antiguas eliminadas",
            "deleted_count": deleted_count,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/processors", tags=["Cola - Información"])
async def get_available_processors(current_user=Depends(get_current_active_user)):
    """Obtener lista de procesadores disponibles"""
    return {
        "available_processors": list(TASK_PROCESSORS.keys()),
        "total_processors": len(TASK_PROCESSORS),
    }


@router.delete("/tasks/{task_id}", tags=["Cola - Tareas"])
async def delete_task(
    task_id: str = Path(...), current_user=Depends(get_current_active_user)
):
    """Eliminar una tarea específica por su ID"""
    success = await optimized_thread_queue_manager.delete_task(task_id)

    if not success:
        raise HTTPException(
            status_code=404, detail="Tarea no encontrada o no se puede eliminar"
        )

    return {
        "success": True,
        "message": f"Tarea {task_id} eliminada",
        "task_id": task_id,
    }


# Endpoints de paginación
@router.get("/pagination/sessions", tags=["Paginación"])
async def get_pagination_sessions(
    session_id: str = Query(...), current_user=Depends(get_current_active_user)
):
    """Obtener información de sesiones de paginación"""
    sessions = await corrected_smart_paginator.get_session_info(session_id)
    return {
        "session_id": session_id,
        "active_sessions": sessions,
        "total_sessions": len(sessions),
    }


@router.delete("/pagination/sessions/{session_id}", tags=["Paginación"])
async def reset_pagination_session(
    session_id: str = Path(...),
    endpoint: Optional[str] = Query(None),
    current_user=Depends(get_current_active_user),
):
    """Reiniciar sesión de paginación"""
    if endpoint:
        success = await corrected_smart_paginator.reset_session(session_id, endpoint)
    else:
        sessions = await corrected_smart_paginator.get_session_info(session_id)
        success_count = 0
        for session in sessions:
            if await corrected_smart_paginator.reset_session(
                session_id, session["endpoint"]
            ):
                success_count += 1
        success = success_count > 0

    if not success:
        raise HTTPException(status_code=404, detail="Sesión no encontrada")

    return {
        "success": True,
        "message": f"Sesión {session_id} reiniciada",
        "session_id": session_id,
    }


@router.delete("/pagination/cleanup", tags=["Paginación"])
async def cleanup_expired_pagination_sessions(
    current_user=Depends(get_current_active_user),
):
    """Limpiar sesiones de paginación expiradas"""
    try:
        deleted_count = await corrected_smart_paginator.cleanup_expired_sessions()
        return {
            "success": True,
            "message": f"{deleted_count} sesiones expiradas eliminadas",
            "deleted_count": deleted_count,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/tasks", tags=["Cola - Tareas"])
async def get_all_tasks(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    status: Optional[str] = Query(None, description="Filtrar por estado: pending, processing, completed, failed, cancelled"),
    current_user=Depends(get_current_active_user)
):
    """Obtener todas las tareas con paginación - para el dashboard"""
    try:
        # Obtener tareas desde el manager
        all_tasks = await optimized_thread_queue_manager.get_all_tasks()
        
        # Filtrar por estado si se especifica
        if status:
            all_tasks = [task for task in all_tasks if task.get('status') == status]
        
        # Calcular paginación
        total = len(all_tasks)
        start_idx = (page - 1) * page_size
        end_idx = start_idx + page_size
        
        tasks_page = all_tasks[start_idx:end_idx]
        
        # Preparar respuesta en formato esperado por el dashboard
        return {
            "data": tasks_page,
            "pagination": {
                "page": page,
                "page_size": page_size,
                "total": total,
                "pages": (total + page_size - 1) // page_size,
                "has_next": end_idx < total,
                "has_prev": page > 1
            },
            "events": [
                {
                    "event_type": "queue_stats",
                    "timestamp": datetime.utcnow().isoformat(),
                    "data": {
                        "total_tasks": total,
                        "filtered_by": status,
                        "page_info": f"Página {page} de {(total + page_size - 1) // page_size}"
                    }
                }
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error obteniendo tareas: {str(e)}")
