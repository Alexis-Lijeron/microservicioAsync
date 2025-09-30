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
    max_retries: Optional[int] = 0
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


@router.delete("/cleanup/all-tasks", tags=["Cola - Mantenimiento"])
async def clear_all_tasks(current_user=Depends(get_current_active_user)):
    """Limpiar TODAS las tareas de TODAS las colas sin importar su estado"""
    try:
        total_cleared = await optimized_thread_queue_manager.clear_all_tasks()
        return {
            "success": True,
            "message": f"Todas las tareas han sido eliminadas. Total: {total_cleared}",
            "cleared_tasks": total_cleared,
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


# NUEVOS ENDPOINTS PARA GESTIÓN DINÁMICA DE COLAS Y WORKERS

@router.post("/workers/scale", tags=["Cola - Escalado Dinámico"])
async def scale_queue_workers(
    queue_type: int = Query(..., description="Tipo de cola (1=Critical, 2=High, 3=Normal, 4=Bulk)"),
    target_workers: int = Query(..., ge=0, le=10, description="Número objetivo de workers"),
    current_user=Depends(get_current_active_user)
):
    """Escalar workers de una cola específica dinámicamente"""
    try:
        from app.core.thread_queue import QueueType
        
        # Mapear entero a QueueType
        queue_type_map = {
            1: QueueType.CRITICAL,
            2: QueueType.HIGH, 
            3: QueueType.NORMAL,
            4: QueueType.BULK
        }
        
        if queue_type not in queue_type_map:
            raise HTTPException(status_code=400, detail="Tipo de cola inválido")
            
        queue_enum = queue_type_map[queue_type]
        
        if not optimized_thread_queue_manager.is_running():
            raise HTTPException(status_code=400, detail="El sistema de colas no está ejecutándose")
        
        result = await optimized_thread_queue_manager.scale_workers(queue_enum, target_workers)
        
        return {
            "success": True,
            "message": f"Workers escalados en cola {result['queue']}",
            "scaling_result": result,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/queues/add", tags=["Cola - Gestión Dinámica"])
async def add_new_queue(
    queue_type: int = Query(..., description="Tipo de cola único (usar números altos como 10, 11, etc.)"),
    name: str = Query(..., description="Nombre de la nueva cola"),
    min_workers: int = Query(1, ge=0, le=5),
    max_workers: int = Query(3, ge=1, le=10),
    priority_min: int = Query(6, ge=1, le=10),
    priority_max: int = Query(8, ge=1, le=10),
    check_interval: float = Query(5.0, ge=1.0, le=30.0),
    current_user=Depends(get_current_active_user)
):
    """Agregar una nueva cola dinámicamente"""
    try:
        from app.core.thread_queue import QueueType
        
        if not optimized_thread_queue_manager.is_running():
            raise HTTPException(status_code=400, detail="El sistema de colas no está ejecutándose")
        
        # Crear QueueType dinámico (esto es una limitación de enum, en producción se usaría un sistema más flexible)
        if queue_type < 100:  # Reservar números bajos para tipos predefinidos
            raise HTTPException(status_code=400, detail="Use números >= 100 para colas personalizadas")
        
        config = {
            "name": name,
            "min_workers": min_workers,
            "max_workers": max_workers,
            "priority_range": [priority_min, priority_max],
            "check_interval": check_interval
        }
        
        # Para esta implementación, agregaremos a los configs existentes
        # En una implementación más robusta, esto sería completamente dinámico
        queue_enum = QueueType(queue_type) if queue_type in [e.value for e in QueueType] else None
        
        if queue_enum is None:
            return {
                "success": False,
                "message": f"Por ahora solo se admiten tipos de cola predefinidos (1-4)",
                "available_types": {
                    "1": "CRITICAL",
                    "2": "HIGH", 
                    "3": "NORMAL",
                    "4": "BULK"
                }
            }
        
        success = await optimized_thread_queue_manager.add_queue(queue_enum, config)
        
        return {
            "success": success,
            "message": f"Cola '{name}' {'agregada' if success else 'ya existe'}",
            "config": config,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/queues/{queue_type}", tags=["Cola - Gestión Dinámica"])
async def remove_queue(
    queue_type: int = Path(..., description="Tipo de cola a eliminar"),
    current_user=Depends(get_current_active_user)
):
    """Eliminar una cola dinámicamente (TODAS las colas son eliminables ahora)"""
    try:
        from app.core.thread_queue import QueueType
        
        queue_type_map = {
            1: QueueType.CRITICAL,
            2: QueueType.HIGH,
            3: QueueType.NORMAL, 
            4: QueueType.BULK
        }
        
        if queue_type not in queue_type_map:
            raise HTTPException(status_code=400, detail="Tipo de cola inválido")
            
        queue_enum = queue_type_map[queue_type]
        
        if not optimized_thread_queue_manager.is_running():
            raise HTTPException(status_code=400, detail="El sistema de colas no está ejecutándose")
        
        # TODAS las colas son eliminables ahora, incluida la crítica
        success = await optimized_thread_queue_manager.remove_queue(queue_enum)
        
        return {
            "success": success,
            "message": f"Cola {'eliminada' if success else 'no encontrada'}",
            "queue_type": queue_type,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/queues/info", tags=["Cola - Información Detallada"])
async def get_queues_info(current_user=Depends(get_current_active_user)):
    """Obtener información detallada de todas las colas"""
    try:
        from app.core.thread_queue import QueueType
        
        if not optimized_thread_queue_manager.is_running():
            raise HTTPException(status_code=400, detail="El sistema de colas no está ejecutándose")
        
        queues_info = {}
        queue_types = [QueueType.CRITICAL, QueueType.HIGH, QueueType.NORMAL, QueueType.BULK]
        
        for queue_type in queue_types:
            info = await optimized_thread_queue_manager.get_queue_info(queue_type)
            if info:
                queues_info[queue_type.value] = info
        
        return {
            "success": True,
            "queues": queues_info,
            "total_queues": len(queues_info),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/priorities/map", tags=["Cola - Configuración"])
async def get_task_priorities(current_user=Depends(get_current_active_user)):
    """Obtener mapeo de tipos de tarea a prioridades"""
    try:
        # Acceder al mapeo de prioridades
        priority_map = optimized_thread_queue_manager._task_priority_map
        
        return {
            "success": True,
            "task_priorities": dict(priority_map),
            "priority_descriptions": {
                1: "CRÍTICA - Sistema, rollbacks, autenticación",
                2: "ALTA - Inscripciones, notas, horarios",
                3: "MEDIA-ALTA - Estudiantes, docentes, grupos", 
                4: "MEDIA - Materias, carreras, gestiones",
                5: "NORMAL - Aulas, niveles, planes, prerequisitos"
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/priorities/{task_type}", tags=["Cola - Configuración"])
async def update_task_priority(
    task_type: str = Path(..., description="Tipo de tarea"),
    priority: int = Query(..., ge=1, le=10, description="Nueva prioridad (1=más alta, 10=más baja)"),
    current_user=Depends(get_current_active_user)
):
    """Actualizar la prioridad de un tipo de tarea"""
    try:
        optimized_thread_queue_manager.set_task_priority(task_type, priority)
        
        return {
            "success": True,
            "message": f"Prioridad actualizada para {task_type}",
            "task_type": task_type,
            "new_priority": priority,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# NUEVOS ENDPOINTS PARA COLAS COMPLETAMENTE DINÁMICAS

@router.post("/queues/create", tags=["Cola - Gestión Dinámica Avanzada"])
async def create_new_dynamic_queue(
    name: str = Query(..., description="Nombre único de la nueva cola"),
    priority_min: int = Query(..., ge=1, le=20, description="Prioridad mínima que maneja"),
    priority_max: int = Query(..., ge=1, le=20, description="Prioridad máxima que maneja"),
    min_workers: int = Query(0, ge=0, le=5, description="Mínimo workers"),
    max_workers: int = Query(5, ge=1, le=1000, description="Máximo workers"),
    check_interval: float = Query(5.0, ge=1.0, le=60.0, description="Intervalo de chequeo en segundos"),
    auto_scale: bool = Query(True, description="Habilitar auto-escalado"),
    current_user=Depends(get_current_active_user)
):
    """Crear una cola completamente nueva y dinámica"""
    try:
        if not optimized_thread_queue_manager.is_running():
            raise HTTPException(status_code=400, detail="El sistema de colas no está ejecutándose")
        
        if priority_min > priority_max:
            raise HTTPException(status_code=400, detail="priority_min debe ser <= priority_max")
        
        queue_id = await optimized_thread_queue_manager.create_dynamic_queue(
            name=name,
            priority_min=priority_min,
            priority_max=priority_max,
            min_workers=min_workers,
            max_workers=max_workers,
            check_interval=check_interval,
            auto_scale=auto_scale
        )
        
        return {
            "success": True,
            "message": f"Cola dinámica '{name}' creada exitosamente",
            "queue_id": queue_id,
            "queue_name": name,
            "priority_range": [priority_min, priority_max],
            "worker_range": [min_workers, max_workers],
            "auto_scale": auto_scale,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/queues/all", tags=["Cola - Información Completa"])
async def get_all_queues_info(current_user=Depends(get_current_active_user)):
    """Obtener información de TODAS las colas (predefinidas + dinámicas)"""
    try:
        if not optimized_thread_queue_manager.is_running():
            raise HTTPException(status_code=400, detail="El sistema de colas no está ejecutándose")
        
        all_queues = optimized_thread_queue_manager.get_all_queues()
        
        # Organizar por tipo
        predefined_queues = {}
        dynamic_queues = {}
        
        for queue_key, queue_info in all_queues.items():
            if queue_info["is_predefined"]:
                predefined_queues[queue_key] = queue_info
            else:
                dynamic_queues[queue_key] = queue_info
        
        return {
            "success": True,
            "total_queues": len(all_queues),
            "predefined_count": len(predefined_queues),
            "dynamic_count": len(dynamic_queues),
            "predefined_queues": predefined_queues,
            "dynamic_queues": dynamic_queues,
            "system_running": optimized_thread_queue_manager.is_running(),
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/workers/scale-by-name", tags=["Cola - Escalado Dinámico"])
async def scale_workers_by_queue_name(
    queue_name: str = Query(..., description="Nombre de la cola"),
    target_workers: int = Query(..., ge=0, le=1000, description="Número objetivo de workers"),
    current_user=Depends(get_current_active_user)
):
    """Escalar workers de cualquier cola por su nombre"""
    try:
        if not optimized_thread_queue_manager.is_running():
            raise HTTPException(status_code=400, detail="El sistema de colas no está ejecutándose")
        
        result = await optimized_thread_queue_manager.scale_workers_by_name(queue_name, target_workers)
        
        return {
            "success": True,
            "message": f"Workers escalados en cola '{queue_name}'",
            "scaling_result": result,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/queues/dynamic/{queue_id}", tags=["Cola - Gestión Dinámica Avanzada"])
async def remove_dynamic_queue(
    queue_id: str = Path(..., description="ID de la cola dinámica a eliminar"),
    current_user=Depends(get_current_active_user)
):
    """Eliminar una cola dinámica específica"""
    try:
        if not optimized_thread_queue_manager.is_running():
            raise HTTPException(status_code=400, detail="El sistema de colas no está ejecutándose")
        
        success = await optimized_thread_queue_manager.remove_dynamic_queue(queue_id)
        
        if not success:
            raise HTTPException(status_code=404, detail=f"Cola dinámica no encontrada: {queue_id}")
        
        return {
            "success": True,
            "message": f"Cola dinámica eliminada: {queue_id}",
            "queue_id": queue_id,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/auto-distribute", tags=["Cola - Distribución Automática"])
async def auto_distribute_priorities(
    task_types: List[str] = Query(..., description="Lista de tipos de tareas para distribuir"),
    start_priority: int = Query(6, ge=1, le=20, description="Prioridad inicial"),
    end_priority: int = Query(10, ge=1, le=20, description="Prioridad final"),
    current_user=Depends(get_current_active_user)
):
    """Distribuir automáticamente prioridades para nuevos tipos de tarea"""
    try:
        if start_priority > end_priority:
            raise HTTPException(status_code=400, detail="start_priority debe ser <= end_priority")
        
        # Distribuir prioridades uniformemente
        num_tasks = len(task_types)
        if num_tasks == 0:
            raise HTTPException(status_code=400, detail="Lista de task_types no puede estar vacía")
        
        priority_range = end_priority - start_priority
        step = priority_range / max(1, num_tasks - 1) if num_tasks > 1 else 0
        
        assignments = []
        for i, task_type in enumerate(task_types):
            assigned_priority = int(start_priority + (step * i))
            optimized_thread_queue_manager.set_task_priority(task_type, assigned_priority)
            assignments.append({
                "task_type": task_type,
                "assigned_priority": assigned_priority
            })
        
        return {
            "success": True,
            "message": f"Distribuidas prioridades para {num_tasks} tipos de tarea",
            "assignments": assignments,
            "priority_range": [start_priority, end_priority],
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/queues/bulk-create", tags=["Cola - Gestión Masiva"])
async def bulk_create_queues(
    count: int = Query(..., ge=1, le=50, description="Número de colas a crear"),
    workers_per_queue: int = Query(..., ge=1, le=1000, description="Workers por cola"),
    prefix: str = Query("queue_", description="Prefijo para los nombres"),
    current_user=Depends(get_current_active_user)
):
    """Crear múltiples colas de forma masiva"""
    try:
        if not optimized_thread_queue_manager.is_running():
            raise HTTPException(status_code=400, detail="El sistema de colas no está ejecutándose")

        created_queues = []
        failed_queues = []

        # Rangos de prioridad especializados para asignar automáticamente
        specialized_ranges = [
            (1, 2),   # Críticas: inscripciones, notas
            (3, 4),   # Importantes: docentes, estudiantes  
            (5, 6),   # Medias: grupos, materias, carreras
            (7, 8),   # Normales: aulas, niveles, planes
            (9, 10),  # Bajas: prerrequisitos, etc.
        ]

        for i in range(1, count + 1):
            try:
                queue_name = f"{prefix}{str(i).zfill(2)}"
                
                # Asignar rango especializado (cíclico si hay más colas que rangos)
                range_index = (i - 1) % len(specialized_ranges)
                priority_min, priority_max = specialized_ranges[range_index]
                
                queue_id = await optimized_thread_queue_manager.create_dynamic_queue(
                    name=queue_name,
                    priority_min=priority_min,
                    priority_max=priority_max,
                    min_workers=0,
                    max_workers=workers_per_queue,
                    check_interval=5.0,
                    auto_scale=True
                )
                
                created_queues.append({
                    "queue_id": queue_id,
                    "name": queue_name,
                    "max_workers": workers_per_queue,
                    "priority_range": f"[{priority_min}, {priority_max}]"
                })
                
            except Exception as e:
                failed_queues.append({
                    "name": f"{prefix}{str(i).zfill(2)}",
                    "error": str(e)
                })

        return {
            "success": len(created_queues) > 0,
            "message": f"Creación masiva completada: {len(created_queues)} exitosas, {len(failed_queues)} fallidas",
            "created_count": len(created_queues),
            "failed_count": len(failed_queues),
            "created_queues": created_queues,
            "failed_queues": failed_queues,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/queues/optimize-specialization", tags=["Cola - Optimización"])
async def optimize_queue_specialization(
    current_user=Depends(get_current_active_user)
):
    """Optimizar especialización de colas existentes por rangos de prioridad"""
    try:
        if not optimized_thread_queue_manager.is_running():
            raise HTTPException(status_code=400, detail="El sistema de colas no está ejecutándose")

        success = await optimized_thread_queue_manager.optimize_queue_specialization()
        
        if not success:
            raise HTTPException(status_code=400, detail="No se pudo completar la optimización")

        return {
            "success": True,
            "message": "Especialización de colas optimizada exitosamente",
            "description": "Las colas han sido especializadas con rangos de prioridad específicos para mejorar la distribución de tareas",
            "priority_ranges": {
                "Críticas (1-2)": "Inscripciones, Notas",
                "Importantes (3-4)": "Docentes, Estudiantes", 
                "Medias (5-6)": "Grupos, Materias, Carreras",
                "Normales (7-8)": "Aulas, Niveles, Planes",
                "Bajas (9-10)": "Prerrequisitos, otros"
            },
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
