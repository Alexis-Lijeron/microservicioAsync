from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse
from sse_starlette.sse import EventSourceResponse
import asyncio
import json
from datetime import datetime

from app.api.deps import get_current_active_user
from app.core.redis_manager import redis_manager

router = APIRouter()


@router.get("/health")
async def redis_health_check(current_user=Depends(get_current_active_user)):
    """Verificación de salud de Redis"""
    health = await redis_manager.health_check()
    
    if health["status"] == "healthy":
        return health
    else:
        raise HTTPException(status_code=503, detail=health)


@router.get("/stats")
async def get_redis_stats(current_user=Depends(get_current_active_user)):
    """Obtener estadísticas de Redis"""
    if not redis_manager.is_connected:
        raise HTTPException(status_code=503, detail="Redis no conectado")
    
    stats = await redis_manager.get_stats()
    return stats


@router.get("/workers")
async def get_redis_workers(current_user=Depends(get_current_active_user)):
    """Obtener estado de workers Redis"""
    if not redis_manager.is_connected:
        raise HTTPException(status_code=503, detail="Redis no conectado")
    
    workers = await redis_manager.get_workers_status()
    return {
        "workers": workers,
        "total_workers": len(workers),
        "active_workers": sum(1 for w in workers if w.get('status') == 'active'),
        "timestamp": datetime.utcnow().isoformat()
    }


@router.get("/events/recent")
async def get_recent_events(
    limit: int = Query(50, ge=1, le=200),
    current_user=Depends(get_current_active_user)
):
    """Obtener eventos recientes del historial"""
    if not redis_manager.is_connected:
        raise HTTPException(status_code=503, detail="Redis no conectado")
    
    events = await redis_manager.get_recent_events(limit)
    return {
        "events": events,
        "total": len(events),
        "timestamp": datetime.utcnow().isoformat()
    }


@router.post("/events/publish")
async def publish_test_event(
    event_data: dict,
    current_user=Depends(get_current_active_user)
):
    """Publicar evento de prueba"""
    if not redis_manager.is_connected:
        raise HTTPException(status_code=503, detail="Redis no conectado")
    
    # Agregar metadatos del evento
    event_data.update({
        "event_type": event_data.get("event_type", "test_event"),
        "timestamp": datetime.utcnow().isoformat(),
        "user_id": getattr(current_user, 'id', 'unknown')
    })
    
    success = await redis_manager.publish_event(event_data)
    
    if success:
        return {"success": True, "message": "Evento publicado", "event": event_data}
    else:
        raise HTTPException(status_code=500, detail="Error publicando evento")


@router.get("/stream/all")
async def stream_all_events(
    token: Optional[str] = Query(None, description="JWT token para autenticación")
):
    """Stream de eventos en tiempo real usando Server-Sent Events"""
    
    # Verificar autenticación usando token de query param
    if not token:
        raise HTTPException(status_code=401, detail="Token requerido")
    
    try:
        # Verificar token JWT
        from app.core.security import verify_token
        from app.config.database import async_session_factory
        from app.models.estudiante import Estudiante
        from sqlalchemy import select
        
        registro = verify_token(token)
        if not registro:
            raise HTTPException(status_code=401, detail="Token inválido")
        
        # Verificar usuario en BD (opcional, para mejor seguridad)
        async with async_session_factory() as db:
            result = await db.execute(select(Estudiante).where(Estudiante.registro == registro))
            user = result.scalar_one_or_none()
            if not user:
                raise HTTPException(status_code=401, detail="Usuario no encontrado")
        
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Error de autenticación: {str(e)}")
    
    if not redis_manager.is_connected:
        raise HTTPException(status_code=503, detail="Redis no conectado")
    
    async def event_generator():
        """Generador de eventos para SSE"""
        try:
            # Enviar eventos recientes primero
            recent_events = await redis_manager.get_recent_events(20)
            for event in reversed(recent_events):  # Más recientes primero
                yield {
                    "event": "message",
                    "data": json.dumps(event)
                }
            
            # Suscribirse a eventos en tiempo real
            async for event in redis_manager.subscribe_events():
                yield {
                    "event": "message", 
                    "data": json.dumps(event)
                }
                
        except asyncio.CancelledError:
            print("🛑 Stream de eventos cancelado")
        except Exception as e:
            print(f"❌ Error en stream de eventos: {e}")
            yield {
                "event": "error",
                "data": json.dumps({"error": str(e), "timestamp": datetime.utcnow().isoformat()})
            }
    
    return EventSourceResponse(event_generator())


@router.get("/stream/tasks")
async def stream_task_events(
    token: Optional[str] = Query(None),
    task_types: Optional[str] = Query(None, description="Filtrar por tipos de tarea (separados por coma)")
):
    """Stream de eventos específicos de tareas"""
    
    # Autenticación similar al endpoint anterior
    if not token:
        raise HTTPException(status_code=401, detail="Token requerido")
    
    try:
        from app.core.security import verify_token
        registro = verify_token(token)
        if not registro:
            raise HTTPException(status_code=401, detail="Token inválido")
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Error de autenticación: {str(e)}")
    
    if not redis_manager.is_connected:
        raise HTTPException(status_code=503, detail="Redis no conectado")
    
    # Filtros de tipo de tarea
    allowed_task_types = set()
    if task_types:
        allowed_task_types = set(task_types.split(','))
    
    async def task_event_generator():
        """Generador filtrado para eventos de tareas"""
        try:
            async for event in redis_manager.subscribe_events():
                # Filtrar solo eventos relacionados con tareas
                if event.get('event_type') in ['task_created', 'task_started', 'task_completed', 'task_failed']:
                    
                    # Aplicar filtro de tipo de tarea si está especificado
                    if allowed_task_types and event.get('task_type') not in allowed_task_types:
                        continue
                    
                    yield {
                        "event": "task_event",
                        "data": json.dumps(event)
                    }
                
        except asyncio.CancelledError:
            print("🛑 Stream de tareas cancelado")
        except Exception as e:
            print(f"❌ Error en stream de tareas: {e}")
            yield {
                "event": "error",
                "data": json.dumps({"error": str(e)})
            }
    
    return EventSourceResponse(task_event_generator())


@router.get("/stream/workers")
async def stream_worker_events(
    token: Optional[str] = Query(None)
):
    """Stream de eventos específicos de workers"""
    
    # Autenticación
    if not token:
        raise HTTPException(status_code=401, detail="Token requerido")
    
    try:
        from app.core.security import verify_token
        registro = verify_token(token)
        if not registro:
            raise HTTPException(status_code=401, detail="Token inválido")
    except Exception as e:
        raise HTTPException(status_code=401, detail=f"Error de autenticación: {str(e)}")
    
    if not redis_manager.is_connected:
        raise HTTPException(status_code=503, detail="Redis no conectado")
    
    async def worker_event_generator():
        """Generador para eventos de workers"""
        try:
            # Enviar estado actual de workers
            workers = await redis_manager.get_workers_status()
            yield {
                "event": "workers_status",
                "data": json.dumps({
                    "event_type": "workers_initial",
                    "workers": workers,
                    "timestamp": datetime.utcnow().isoformat()
                })
            }
            
            # Stream de eventos de workers
            async for event in redis_manager.subscribe_events():
                if event.get('event_type') in ['worker_registered', 'worker_unregistered', 'task_started', 'task_completed']:
                    yield {
                        "event": "worker_event",
                        "data": json.dumps(event)
                    }
                
        except asyncio.CancelledError:
            print("🛑 Stream de workers cancelado")
        except Exception as e:
            print(f"❌ Error en stream de workers: {e}")
            yield {
                "event": "error",
                "data": json.dumps({"error": str(e)})
            }
    
    return EventSourceResponse(worker_event_generator())
