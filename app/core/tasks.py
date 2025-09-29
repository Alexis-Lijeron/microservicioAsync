import asyncio
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, Optional
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.redis_queue_integration import RedisIntegratedQueueManager
from app.models.task import Task, TaskStatus

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def create_async_task(
    db: AsyncSession,
    task_type: str,
    task_data: Dict[str, Any],
    user_id: Optional[str] = None,
    priority: int = 5
) -> Task:
    """
    Crear una nueva tarea asíncrona en la base de datos
    """
    try:
        # Generar ID único para la tarea
        task_id = f"{task_type}_{uuid.uuid4().hex[:8]}"
        
        # Crear la tarea
        task = Task(
            task_id=task_id,
            task_type=task_type,
            status=TaskStatus.PENDING,
            priority=priority,
            progress=0.0,
            scheduled_at=datetime.utcnow()
        )
        
        # Establecer los datos
        task.set_data(task_data)
        
        # Agregar contexto de usuario si está disponible
        if user_id:
            task_data["user_id"] = user_id
            task.set_data(task_data)
        
        # Guardar en la base de datos
        db.add(task)
        await db.commit()
        await db.refresh(task)
        
        logger.info(f"📝 Tarea creada: {task_id} ({task_type})")
        
        # TODO: Aquí se podría agregar la tarea a la cola de Redis
        # queue_manager = RedisIntegratedQueueManager()
        # await queue_manager.add_task(task_id, task_type, task_data, priority)
        
        return task
        
    except Exception as e:
        logger.error(f"❌ Error al crear tarea: {e}")
        await db.rollback()
        raise


async def task_worker():
    """
    Worker principal para procesar tareas desde Redis
    Este worker se ejecuta en un contenedor separado
    """
    logger.info("🚀 Iniciando Task Worker...")
    
    # Crear instancia del manager integrado
    queue_manager = RedisIntegratedQueueManager()
    
    try:
        # Iniciar el sistema de colas con Redis
        redis_url = "redis://redis:6379"  # URL para Docker
        success = await queue_manager.start(max_workers=2, redis_url=redis_url)
        
        if not success:
            logger.error("❌ No se pudo iniciar el worker con Redis")
            return
        
        logger.info("✅ Worker iniciado correctamente, procesando tareas...")
        
        # Mantener el worker corriendo
        while True:
            await asyncio.sleep(10)
            logger.info("💓 Worker activo...")
            
    except KeyboardInterrupt:
        logger.info("🛑 Worker detenido por usuario")
    except Exception as e:
        logger.error(f"❌ Error en worker: {e}")
    finally:
        # Cleanup
        queue_manager.stop()
        logger.info("🔚 Worker finalizado")

if __name__ == "__main__":
    asyncio.run(task_worker())