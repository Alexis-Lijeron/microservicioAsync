import asyncio
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

from app.config.database import async_session_factory
from app.models.task import Task
from app.core.redis_manager import redis_manager
from app.core.task_processors import get_task_processor


class RedisIntegratedQueueManager:
    """
    Gestor de cola integrado que combina PostgreSQL con Redis
    - PostgreSQL: Persistencia y recuperación ante fallos
    - Redis: Cola rápida y eventos en tiempo real
    """

    def __init__(self):
        self._running = False
        self._workers = []
        self._worker_tasks = []
        self._max_workers = 4
        self._stats = {
            "tasks_processed": 0,
            "tasks_failed": 0,
            "tasks_completed": 0,
            "workers_active": 0,
            "uptime_start": None,
            "last_sync": None,
        }

    async def start(
        self, max_workers: int = 4, redis_url: str = "redis://redis:6379"
    ):
        """Iniciar sistema de cola con Redis"""
        if self._running:
            print("⚠️ Sistema de cola ya está en ejecución")
            return False

        try:
            # Conectar a Redis
            redis_connected = await redis_manager.connect(redis_url)
            if not redis_connected:
                print("❌ No se pudo conectar a Redis, continuando sin Redis")
                return False

            self._running = True
            self._max_workers = max_workers
            self._stats["uptime_start"] = datetime.utcnow()

            print(
                f"🚀 RedisIntegratedQueueManager iniciando con {max_workers} workers..."
            )

            # Sincronizar tareas existentes en PostgreSQL con Redis
            await self._sync_existing_tasks()

            # Iniciar workers
            for i in range(max_workers):
                worker_id = f"redis_worker_{i+1}"
                await redis_manager.register_worker(worker_id)

                task = asyncio.create_task(self._run_redis_worker(worker_id))
                self._worker_tasks.append(task)
                self._workers.append(worker_id)

            # Iniciar monitor de sincronización
            sync_task = asyncio.create_task(self._sync_monitor())
            self._worker_tasks.append(sync_task)

            self._stats["workers_active"] = len(self._workers)

            print(f"✅ {len(self._workers)} workers Redis iniciados")
            return True

        except Exception as e:
            print(f"❌ Error iniciando sistema Redis: {e}")
            return False

    def stop(self):
        """Detener sistema de cola"""
        if not self._running:
            return

        print("🛑 Deteniendo RedisIntegratedQueueManager...")
        self._running = False

        # Cancelar workers
        for task in self._worker_tasks:
            task.cancel()

        # Desregistrar workers de Redis
        async def cleanup_workers():
            for worker_id in self._workers:
                await redis_manager.unregister_worker(worker_id)
            await redis_manager.disconnect()

        try:
            asyncio.create_task(cleanup_workers())
        except Exception as e:
            print(f"⚠️ Error en cleanup: {e}")

        self._worker_tasks.clear()
        self._workers.clear()
        self._stats["workers_active"] = 0

        print("✅ RedisIntegratedQueueManager detenido")

    async def add_task(
        self,
        task_type: str,
        data: Dict[str, Any],
        priority: int = 5,
        max_retries: int = 3,
        rollback_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Agregar tarea al sistema híbrido"""
        task_id = str(uuid.uuid4())

        try:
            # 1. Guardar en PostgreSQL (persistencia)
            async with async_session_factory() as db:
                task = Task(
                    task_id=task_id,
                    task_type=task_type,
                    status="pending",
                    priority=priority,
                    max_retries=max_retries,
                    scheduled_at=datetime.utcnow(),
                )

                task.set_data(data)
                if rollback_data:
                    task.set_rollback_data(rollback_data)

                db.add(task)
                await db.commit()

            # 2. Agregar a Redis (cola rápida)
            redis_task_data = {
                "task_type": task_type,
                "data": data,
                "priority": priority,
                "max_retries": max_retries,
                "created_at": datetime.utcnow().isoformat(),
            }

            await redis_manager.add_task(task_id, redis_task_data)

            print(f"📝 Tarea híbrida agregada: {task_id} ({task_type})")
            return task_id

        except Exception as e:
            print(f"❌ Error agregando tarea híbrida: {e}")
            return task_id  # Devolver ID aunque Redis falle

    async def _run_redis_worker(self, worker_id: str):
        """Worker que procesa tareas de Redis con sincronización a PostgreSQL"""
        print(f"🔧 Worker Redis {worker_id} iniciado")

        while self._running:
            try:
                await redis_manager.update_worker_status(worker_id, "idle")

                # Obtener tarea de Redis
                redis_task = await redis_manager.get_next_task(worker_id)

                if not redis_task:
                    await asyncio.sleep(2)
                    continue

                task_id = redis_task["task_id"]
                print(f"📋 Worker Redis {worker_id} procesando: {task_id}")

                await redis_manager.update_worker_status(worker_id, "active", task_id)

                # Sincronizar estado con PostgreSQL
                await self._sync_task_to_pg(task_id, "processing", worker_id)

                try:
                    # Procesar tarea
                    result = await self._execute_redis_task(redis_task, worker_id)

                    if result.get("success", False):
                        # Éxito
                        await redis_manager.complete_task(task_id, result)
                        await self._sync_task_to_pg(
                            task_id, "completed", worker_id, result
                        )

                        self._stats["tasks_completed"] += 1
                        self._stats["tasks_processed"] += 1

                    else:
                        # Error controlado
                        error_msg = result.get("error", "Error desconocido")
                        await self._handle_redis_task_failure(
                            redis_task, error_msg, worker_id
                        )

                except Exception as e:
                    # Error no controlado
                    await self._handle_redis_task_failure(redis_task, str(e), worker_id)

            except asyncio.CancelledError:
                print(f"🛑 Worker Redis {worker_id} cancelado")
                break
            except Exception as e:
                print(f"❌ Error en worker Redis {worker_id}: {e}")
                await asyncio.sleep(1)

        await redis_manager.unregister_worker(worker_id)
        print(f"🛑 Worker Redis {worker_id} detenido")

    async def _execute_redis_task(
        self, redis_task: Dict[str, Any], worker_id: str
    ) -> Dict[str, Any]:
        """Ejecutar tarea usando el procesador correspondiente"""
        task_type = redis_task.get("task_type")
        processor = get_task_processor(task_type)

        if not processor:
            return {"success": False, "error": f"No hay procesador para: {task_type}"}

        # Crear objeto Task temporal para el procesador
        from app.models.task import Task

        temp_task = Task()
        temp_task.task_id = redis_task["task_id"]
        temp_task.task_type = task_type
        temp_task.set_data(redis_task.get("data", {}))

        # Ejecutar procesador
        return await processor(redis_task.get("data", {}), temp_task)

    async def _handle_redis_task_failure(
        self, redis_task: Dict[str, Any], error: str, worker_id: str
    ):
        """Manejar fallo de tarea Redis"""
        task_id = redis_task["task_id"]
        retry_count = int(redis_task.get("retry_count", 0))
        max_retries = int(redis_task.get("max_retries", 3))

        can_retry = retry_count < max_retries

        await redis_manager.fail_task(task_id, error, can_retry)

        # Sincronizar con PostgreSQL
        if can_retry:
            await self._sync_task_to_pg(
                task_id, "pending", "", None, error, retry_count + 1
            )
        else:
            await self._sync_task_to_pg(task_id, "failed", worker_id, None, error)
            self._stats["tasks_failed"] += 1

        self._stats["tasks_processed"] += 1

    async def _sync_task_to_pg(
        self,
        task_id: str,
        status: str,
        worker_id: str = "",
        result: Dict[str, Any] = None,
        error: str = None,
        retry_count: int = None,
    ):
        """Sincronizar estado de tarea con PostgreSQL"""
        try:
            async with async_session_factory() as db:
                from sqlalchemy import select, update

                updates = {"status": status}

                if status == "processing":
                    updates.update(
                        {
                            "started_at": datetime.utcnow(),
                            "locked_by": worker_id,
                            "locked_at": datetime.utcnow(),
                        }
                    )
                elif status == "completed":
                    updates.update(
                        {
                            "completed_at": datetime.utcnow(),
                            "locked_by": None,
                            "locked_at": None,
                        }
                    )
                    if result:
                        # Usar función del modelo para serializar resultado
                        result_task = Task()
                        result_task.set_result(result)
                        updates["result"] = result_task.result

                elif status == "failed":
                    updates.update(
                        {
                            "completed_at": datetime.utcnow(),
                            "error_message": error,
                            "locked_by": None,
                            "locked_at": None,
                            "needs_rollback": True,
                        }
                    )
                elif status == "pending" and retry_count is not None:
                    updates.update(
                        {
                            "retry_count": retry_count,
                            "error_message": error,
                            "started_at": None,
                            "locked_by": None,
                            "locked_at": None,
                        }
                    )

                await db.execute(
                    update(Task).where(Task.task_id == task_id).values(**updates)
                )
                await db.commit()

        except Exception as e:
            print(f"❌ Error sincronizando tarea a PostgreSQL: {e}")

    async def _sync_existing_tasks(self):
        """Sincronizar tareas existentes de PostgreSQL a Redis"""
        try:
            async with async_session_factory() as db:
                from sqlalchemy import select

                # Obtener tareas pendientes de PostgreSQL
                result = await db.execute(
                    select(Task).where(Task.status == "pending").limit(1000)
                )
                pending_tasks = result.scalars().all()

                sync_count = 0
                for task in pending_tasks:
                    redis_task_data = {
                        "task_type": task.task_type,
                        "data": task.get_data(),
                        "priority": task.priority,
                        "max_retries": task.max_retries,
                        "retry_count": task.retry_count,
                        "created_at": (
                            task.scheduled_at.isoformat()
                            if task.scheduled_at
                            else datetime.utcnow().isoformat()
                        ),
                    }

                    success = await redis_manager.add_task(
                        task.task_id, redis_task_data
                    )
                    if success:
                        sync_count += 1

                if sync_count > 0:
                    print(f"🔄 Sincronizadas {sync_count} tareas pendientes a Redis")

        except Exception as e:
            print(f"❌ Error sincronizando tareas existentes: {e}")

    async def _sync_monitor(self):
        """Monitor que sincroniza periódicamente PostgreSQL con Redis"""
        print("📊 Monitor de sincronización iniciado")

        while self._running:
            try:
                await asyncio.sleep(30)  # Sincronizar cada 30 segundos

                if not self._running:
                    break

                await self._sync_existing_tasks()
                self._stats["last_sync"] = datetime.utcnow()

            except asyncio.CancelledError:
                print("🛑 Monitor de sincronización cancelado")
                break
            except Exception as e:
                print(f"❌ Error en monitor de sincronización: {e}")
                await asyncio.sleep(5)

        print("🛑 Monitor de sincronización detenido")

    # === MÉTODOS DE CONSULTA ===

    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Obtener estado de tarea (prioriza Redis, fallback a PostgreSQL)"""
        try:
            # Intentar Redis primero
            if redis_manager.is_connected:
                redis_data = await redis_manager.redis.hgetall(
                    redis_manager.KEYS["task_data"].format(task_id)
                )
                if redis_data:
                    # Parsear datos de Redis
                    return {
                        "task_id": redis_data.get("task_id"),
                        "task_type": redis_data.get("task_type"),
                        "status": redis_data.get("status"),
                        "priority": int(redis_data.get("priority", 5)),
                        "retry_count": int(redis_data.get("retry_count", 0)),
                        "max_retries": int(redis_data.get("max_retries", 3)),
                        "created_at": redis_data.get("created_at"),
                        "started_at": redis_data.get("started_at"),
                        "completed_at": redis_data.get("completed_at"),
                        "worker_id": redis_data.get("worker_id"),
                        "error_message": redis_data.get("last_error")
                        or redis_data.get("final_error"),
                        "data_source": "redis",
                    }

            # Fallback a PostgreSQL
            async with async_session_factory() as db:
                from sqlalchemy import select

                result = await db.execute(select(Task).where(Task.task_id == task_id))
                task = result.scalar_one_or_none()

                if task:
                    return {
                        "task_id": task.task_id,
                        "task_type": task.task_type,
                        "status": task.status,
                        "priority": task.priority,
                        "progress": task.progress,
                        "retry_count": task.retry_count,
                        "max_retries": task.max_retries,
                        "scheduled_at": (
                            task.scheduled_at.isoformat() if task.scheduled_at else None
                        ),
                        "started_at": (
                            task.started_at.isoformat() if task.started_at else None
                        ),
                        "completed_at": (
                            task.completed_at.isoformat() if task.completed_at else None
                        ),
                        "error_message": task.error_message,
                        "data": task.get_data(),
                        "result": task.get_result() if task.result else None,
                        "locked_by": task.locked_by,
                        "needs_rollback": task.needs_rollback,
                        "data_source": "postgresql",
                    }

            return None

        except Exception as e:
            print(f"❌ Error obteniendo estado de tarea: {e}")
            return None

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas de la cola (híbrido Redis + PostgreSQL)"""
        try:
            # Stats de Redis si está disponible
            redis_stats = {}
            if redis_manager.is_connected:
                redis_stats = await redis_manager.get_stats()

            # Complementar con PostgreSQL si es necesario
            async with async_session_factory() as db:
                from sqlalchemy import select, func

                pg_stats = {}
                statuses = ["pending", "processing", "completed", "failed", "cancelled"]

                for status in statuses:
                    result = await db.execute(
                        select(func.count(Task.task_id)).where(Task.status == status)
                    )
                    pg_stats[status] = result.scalar() or 0

            # Combinar estadísticas
            uptime = None
            if self._stats["uptime_start"]:
                uptime = datetime.utcnow() - self._stats["uptime_start"]

            combined_stats = {
                "queue_status": "running" if self._running else "stopped",
                "total_workers": self._max_workers,
                "active_workers": self._stats["workers_active"],
                "uptime_seconds": uptime.total_seconds() if uptime else 0,
                "last_sync": (
                    self._stats.get("last_sync", {}).isoformat()
                    if self._stats.get("last_sync")
                    else None
                ),
                "redis_connected": redis_manager.is_connected,
                "data_sources": {
                    "redis": redis_stats,
                    "postgresql": {"task_counts": pg_stats},
                },
            }

            # Usar Redis stats como primarios si están disponibles
            if redis_stats:
                combined_stats.update(
                    {
                        "task_counts": redis_stats.get("task_counts", pg_stats),
                        "workers": redis_stats.get("workers", []),
                    }
                )
            else:
                combined_stats["task_counts"] = pg_stats
                combined_stats["workers"] = []

            combined_stats["total_tasks"] = sum(combined_stats["task_counts"].values())
            combined_stats["stats"] = self._stats

            return combined_stats

        except Exception as e:
            print(f"❌ Error obteniendo estadísticas: {e}")
            return {
                "queue_status": "error",
                "error": str(e),
                "redis_connected": redis_manager.is_connected,
            }

    async def get_tasks(
        self,
        status: Optional[str] = None,
        task_type: Optional[str] = None,
        skip: int = 0,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Obtener lista de tareas (PostgreSQL como fuente principal)"""
        try:
            async with async_session_factory() as db:
                from sqlalchemy import select, and_

                query = select(Task)

                conditions = []
                if status:
                    conditions.append(Task.status == status)
                if task_type:
                    conditions.append(Task.task_type == task_type)

                if conditions:
                    query = query.where(and_(*conditions))

                query = query.order_by(Task.priority.asc(), Task.scheduled_at.desc())
                query = query.offset(skip).limit(limit)

                result = await db.execute(query)
                tasks = result.scalars().all()

                return [
                    {
                        "task_id": task.task_id,
                        "task_type": task.task_type,
                        "status": task.status,
                        "priority": task.priority,
                        "progress": task.progress,
                        "scheduled_at": (
                            task.scheduled_at.isoformat() if task.scheduled_at else None
                        ),
                        "started_at": (
                            task.started_at.isoformat() if task.started_at else None
                        ),
                        "completed_at": (
                            task.completed_at.isoformat() if task.completed_at else None
                        ),
                        "retry_count": task.retry_count,
                        "error_message": task.error_message,
                        "data_source": "postgresql",
                    }
                    for task in tasks
                ]

        except Exception as e:
            print(f"❌ Error obteniendo tareas: {e}")
            return []

    def is_running(self) -> bool:
        return self._running

    async def cleanup_old_tasks(self, days_old: int = 7) -> int:
        """Limpiar tareas antiguas de ambos sistemas"""
        total_cleaned = 0

        try:
            # Limpiar PostgreSQL
            cutoff_date = datetime.utcnow() - timedelta(days=days_old)

            async with async_session_factory() as db:
                from sqlalchemy import select, and_

                result = await db.execute(
                    select(Task).where(
                        and_(
                            Task.status.in_(["completed", "cancelled"]),
                            Task.completed_at < cutoff_date,
                        )
                    )
                )
                old_tasks = result.scalars().all()

                for task in old_tasks:
                    await db.delete(task)
                    total_cleaned += 1

                await db.commit()

            # Limpiar Redis (las tareas tienen TTL automático, pero limpiamos manualmente)
            if redis_manager.is_connected:
                # Aquí podrías implementar limpieza específica de Redis si es necesario
                pass

            if total_cleaned > 0:
                print(
                    f"🧹 {total_cleaned} tareas antiguas eliminadas del sistema híbrido"
                )

            return total_cleaned

        except Exception as e:
            print(f"❌ Error limpiando tareas antiguas: {e}")
            return 0


# Instancia global del gestor híbrido
redis_integrated_queue_manager = RedisIntegratedQueueManager()
