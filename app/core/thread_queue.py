import asyncio
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, update
from contextlib import asynccontextmanager

from app.config.database import async_session_factory
from app.models.task import Task


class OptimizedThreadQueueManager:
    """
    Sistema de colas asíncrono optimizado con menos consultas a BD
    """

    def __init__(self):
        self._running = False
        self._workers = []
        self._worker_tasks = []
        self._max_workers = 4
        self._check_interval = 5.0  # Aumentado de 1s a 5s
        self._lock_timeout = timedelta(minutes=5)
        self._task_notification = asyncio.Event()  # Para despertar workers

        self._stats = {
            "tasks_processed": 0,
            "tasks_failed": 0,
            "tasks_completed": 0,
            "workers_active": 0,
            "uptime_start": None,
            "last_db_check": None,
        }

    async def start(self, max_workers: int = 4):
        """Iniciar el sistema de colas con workers asíncronos"""
        if self._running:
            print("⚠️ Sistema de colas ya está en ejecución")
            return

        self._running = True
        self._max_workers = max_workers
        self._stats["uptime_start"] = datetime.utcnow()
        self._task_notification.clear()

        print(f"🚀 OptimizedThreadQueueManager iniciando con {max_workers} workers...")

        # Recuperar tareas huérfanas
        await self._recover_orphaned_tasks()

        # Iniciar workers como tareas asíncronas
        for i in range(max_workers):
            worker_id = f"worker_{i+1}"
            task = asyncio.create_task(self._run_optimized_worker(worker_id))
            self._worker_tasks.append(task)
            self._workers.append(worker_id)
            print(f"🔨 Worker {worker_id} iniciado")

        # Iniciar monitor de tareas
        monitor_task = asyncio.create_task(self._task_monitor())
        self._worker_tasks.append(monitor_task)

        print(f"✅ {len(self._workers)} workers iniciados con monitoreo optimizado")
        self._stats["workers_active"] = len(self._workers)

    def stop(self):
        """Detener el sistema de colas"""
        if not self._running:
            return

        print("🛑 Deteniendo OptimizedThreadQueueManager...")
        self._running = False

        # Despertar workers para que puedan terminar
        self._task_notification.set()

        # Cancelar todas las tareas de workers
        for task in self._worker_tasks:
            task.cancel()

        # Limpiar listas
        self._worker_tasks.clear()
        self._workers.clear()
        self._stats["workers_active"] = 0

        print("✅ OptimizedThreadQueueManager detenido")

    async def _task_monitor(self):
        """Monitor que revisa periódicamente por nuevas tareas"""
        print("📊 Monitor de tareas iniciado")

        while self._running:
            try:
                # Revisar si hay tareas pendientes cada 10 segundos
                async with async_session_factory() as db:
                    result = await db.execute(
                        select(func.count(Task.task_id)).where(Task.status == "pending")
                    )
                    pending_count = result.scalar() or 0

                    if pending_count > 0:
                        print(f"📋 Monitor detectó {pending_count} tareas pendientes")
                        # Despertar workers si hay tareas
                        self._task_notification.set()

                    self._stats["last_db_check"] = datetime.utcnow()

                # Esperar antes del próximo check
                await asyncio.sleep(10.0)  # Check cada 10 segundos

            except asyncio.CancelledError:
                print("🛑 Monitor de tareas cancelado")
                break
            except Exception as e:
                print(f"❌ Error en monitor: {e}")
                await asyncio.sleep(5.0)

        print("🛑 Monitor de tareas detenido")

    async def _run_optimized_worker(self, worker_id: str):
        """Worker optimizado que espera notificaciones en lugar de consultar constantemente"""
        print(f"🔧 Worker optimizado {worker_id} iniciado")

        while self._running:
            try:
                # Esperar notificación de nuevas tareas
                try:
                    await asyncio.wait_for(
                        self._task_notification.wait(), timeout=self._check_interval
                    )
                except asyncio.TimeoutError:
                    # Timeout normal, continuar
                    pass

                if not self._running:
                    break

                # Procesar tareas disponibles
                tasks_processed = 0
                while self._running:
                    task_processed = await self._process_next_task(worker_id)

                    if not task_processed:
                        break  # No hay más tareas

                    tasks_processed += 1

                    # Evitar monopolizar el worker
                    if tasks_processed >= 10:
                        break

                if tasks_processed > 0:
                    print(f"🔄 Worker {worker_id} procesó {tasks_processed} tareas")

                # Reset del evento después de procesar
                if tasks_processed > 0:
                    # Despertar otros workers si procesamos tareas
                    await asyncio.sleep(0.1)  # Pequeña pausa
                    self._task_notification.set()
                else:
                    # No hay tareas, limpiar evento
                    self._task_notification.clear()

            except asyncio.CancelledError:
                print(f"🛑 Worker {worker_id} cancelado")
                break
            except Exception as e:
                print(f"❌ Error en worker {worker_id}: {e}")
                await asyncio.sleep(1.0)

        print(f"🛑 Worker optimizado {worker_id} detenido")

    async def add_task(
        self,
        task_type: str,
        data: Dict[str, Any],
        priority: int = 5,
        max_retries: int = 3,
        rollback_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Agregar una tarea a la cola y notificar workers"""
        task_id = str(uuid.uuid4())

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

            print(f"📝 Tarea agregada: {task_id} ({task_type}) [Prioridad: {priority}]")

            # Despertar workers para procesar nueva tarea
            self._task_notification.set()

            return task_id

    async def _process_next_task(self, worker_id: str) -> bool:
        """Obtener y procesar la siguiente tarea disponible"""
        try:
            async with async_session_factory() as db:
                # Obtener siguiente tarea con bloqueo atómico
                task = await self._get_and_lock_task(db, worker_id)

                if not task:
                    return False

                print(f"📋 Worker {worker_id} procesando: {task.task_id}")

                try:
                    # Procesar la tarea
                    await self._execute_task(task, worker_id, db)
                    self._stats["tasks_processed"] += 1
                    return True

                except Exception as e:
                    print(f"❌ Error procesando tarea {task.task_id}: {e}")
                    await self._handle_task_failure(task, str(e), db)
                    self._stats["tasks_failed"] += 1
                    return True
                finally:
                    await db.commit()
        except Exception as e:
            print(f"❌ Error crítico en _process_next_task: {e}")
            return False

    async def _get_and_lock_task(
        self, db: AsyncSession, worker_id: str
    ) -> Optional[Task]:
        """Obtener y bloquear atómicamente la siguiente tarea disponible"""
        try:
            # Buscar tareas pendientes o con lock expirado
            lock_expiry = datetime.utcnow() - self._lock_timeout

            result = await db.execute(
                select(Task)
                .where(
                    and_(
                        Task.status == "pending",
                        or_(Task.locked_by.is_(None), Task.locked_at < lock_expiry),
                    )
                )
                .order_by(Task.priority.asc(), Task.scheduled_at.asc())
                .limit(1)
                .with_for_update(skip_locked=True)
            )

            task = result.scalar_one_or_none()

            if task:
                # Bloquear la tarea
                task.status = "processing"
                task.locked_by = worker_id
                task.locked_at = datetime.utcnow()
                task.started_at = datetime.utcnow()
                await db.flush()

            return task

        except Exception as e:
            print(f"Error obteniendo tarea: {e}")
            await db.rollback()
            return None

    async def _execute_task(self, task: Task, worker_id: str, db: AsyncSession):
        """Ejecutar una tarea específica"""
        from app.core.task_processors import get_task_processor

        processor = get_task_processor(task.task_type)
        if not processor:
            raise ValueError(f"No hay procesador para el tipo: {task.task_type}")

        # Actualizar progreso
        task.progress = 10.0
        await db.flush()

        # Ejecutar procesador
        result = await processor(task.get_data(), task)

        # Actualizar progreso
        task.progress = 90.0
        await db.flush()

        if result.get("success", False):
            # Tarea completada exitosamente
            task.status = "completed"
            task.set_result(result)
            task.progress = 100.0
            task.completed_at = datetime.utcnow()
            task.unlock()

            self._stats["tasks_completed"] += 1
            print(f"✅ Tarea completada: {task.task_id}")
        else:
            # Tarea falló
            raise Exception(result.get("error", "Error desconocido"))

    async def _handle_task_failure(
        self, task: Task, error_message: str, db: AsyncSession
    ):
        """Manejar fallo de tarea con reintentos"""
        print(f"❌ Tarea falló: {task.task_id} - {error_message}")

        task.error_message = error_message
        task.unlock()

        if task.can_retry():
            # Programar reintento
            task.status = "pending"
            task.retry_count += 1
            task.started_at = None
            # Retraso exponencial para reintentos
            delay = min(30 * (2**task.retry_count), 300)
            task.scheduled_at = datetime.utcnow() + timedelta(seconds=delay)
            print(
                f"🔄 Programando reintento {task.retry_count} para: {task.task_id} (en {delay}s)"
            )
        else:
            # Fallo definitivo
            task.status = "failed"
            task.completed_at = datetime.utcnow()
            task.needs_rollback = True
            print(f"💀 Tarea falló definitivamente: {task.task_id}")

            # Programar rollback si hay datos
            if task.rollback_data:
                await self._schedule_rollback(task)

    async def _schedule_rollback(self, failed_task: Task):
        """Programar operación de rollback"""
        rollback_data = failed_task.get_rollback_data()
        rollback_data["original_task_id"] = failed_task.task_id

        await self.add_task(
            task_type="rollback_operation",
            data=rollback_data,
            priority=1,  # Alta prioridad para rollbacks
        )

        print(f"⪪ Rollback programado para: {failed_task.task_id}")

    async def _recover_orphaned_tasks(self):
        """Recuperar tareas huérfanas después de un reinicio"""
        async with async_session_factory() as db:
            lock_expiry = datetime.utcnow() - self._lock_timeout

            # Buscar tareas en processing con lock expirado
            result = await db.execute(
                select(Task).where(
                    and_(Task.status == "processing", Task.locked_at < lock_expiry)
                )
            )

            orphaned_tasks = result.scalars().all()

            for task in orphaned_tasks:
                print(f"🔧 Recuperando tarea huérfana: {task.task_id}")
                task.status = "pending"
                task.unlock()
                task.started_at = None

            if orphaned_tasks:
                await db.commit()
                print(f"✅ {len(orphaned_tasks)} tareas huérfanas recuperadas")

    # Métodos de consulta mantenidos igual
    async def get_task_status(self, task_id: str) -> Optional[Dict[str, Any]]:
        """Obtener estado de una tarea"""
        async with async_session_factory() as db:
            result = await db.execute(select(Task).where(Task.task_id == task_id))
            task = result.scalar_one_or_none()

            if not task:
                return None

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
                "started_at": task.started_at.isoformat() if task.started_at else None,
                "completed_at": (
                    task.completed_at.isoformat() if task.completed_at else None
                ),
                "error_message": task.error_message,
                "data": task.get_data(),
                "result": task.get_result() if task.result else None,
                "locked_by": task.locked_by,
                "needs_rollback": task.needs_rollback,
            }

    async def get_queue_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas de la cola"""
        async with async_session_factory() as db:
            status_counts = {}
            statuses = ["pending", "processing", "completed", "failed", "cancelled"]

            for status in statuses:
                result = await db.execute(
                    select(func.count(Task.task_id)).where(Task.status == status)
                )
                status_counts[status] = result.scalar() or 0

            uptime = None
            if self._stats["uptime_start"]:
                uptime = datetime.utcnow() - self._stats["uptime_start"]

            return {
                "queue_status": "running" if self._running else "stopped",
                "total_workers": self._max_workers,
                "active_workers": self._stats["workers_active"],
                "task_counts": status_counts,
                "total_tasks": sum(status_counts.values()),
                "uptime_seconds": uptime.total_seconds() if uptime else 0,
                "last_db_check": (
                    self._stats["last_db_check"].isoformat()
                    if self._stats["last_db_check"]
                    else None
                ),
                "stats": self._stats,
            }

    async def delete_task(self, task_id: str) -> bool:
        """Eliminar una tarea específica por su ID"""
        async with async_session_factory() as db:
            try:
                # Buscar la tarea por task_id
                result = await db.execute(select(Task).where(Task.task_id == task_id))
                task = result.scalar_one_or_none()

                if not task:
                    print(f"❌ Tarea no encontrada: {task_id}")
                    return False  # La tarea no existe

                # Eliminar la tarea
                await db.delete(task)
                await db.commit()

                print(f"✅ Tarea eliminada: {task_id}")
                return True  # Tarea eliminada con éxito

            except Exception as e:
                print(f"❌ Error eliminando tarea {task_id}: {e}")
                await db.rollback()
                return False  # En caso de error, revertimos la transacción

    async def cleanup_old_tasks(self, days_old: int = 7) -> int:
        """Limpiar tareas antiguas completadas - CORREGIDO"""
        cutoff_date = datetime.utcnow() - timedelta(days=days_old)

        async with async_session_factory() as db:
            try:
                # Obtener tareas a eliminar
                result = await db.execute(
                    select(Task).where(
                        and_(
                            Task.status.in_(["completed", "cancelled"]),
                            Task.completed_at < cutoff_date,
                        )
                    )
                )
                old_tasks = result.scalars().all()

                count = len(old_tasks)

                # Eliminar una por una para evitar problemas
                for task in old_tasks:
                    await db.delete(task)

                await db.commit()
                print(f"🧹 {count} tareas antiguas eliminadas")
                return count

            except Exception as e:
                await db.rollback()
                print(f"❌ Error limpiando tareas: {e}")
                raise e

    async def get_tasks(
        self,
        status: Optional[str] = None,
        task_type: Optional[str] = None,
        skip: int = 0,
        limit: int = 20,
    ) -> List[Dict[str, Any]]:
        """Obtener lista de tareas con filtros"""
        async with async_session_factory() as db:
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
                }
                for task in tasks
            ]

    def is_running(self) -> bool:
        return self._running

    # Los demás métodos (cancel_task, retry_task) se mantienen igual...
    async def cancel_task(self, task_id: str) -> bool:
        """Cancelar una tarea"""
        async with async_session_factory() as db:
            result = await db.execute(
                select(Task)
                .where(
                    and_(
                        Task.task_id == task_id,
                        Task.status.in_(["pending", "processing"]),
                    )
                )
                .with_for_update()
            )
            task = result.scalar_one_or_none()

            if not task:
                return False

            task.status = "cancelled"
            task.completed_at = datetime.utcnow()
            task.unlock()

            await db.commit()
            print(f"⌛ Tarea cancelada: {task_id}")
            return True

    async def retry_task(self, task_id: str) -> bool:
        """Reintentar una tarea fallida"""
        async with async_session_factory() as db:
            result = await db.execute(
                select(Task).where(Task.task_id == task_id).with_for_update()
            )
            task = result.scalar_one_or_none()

            if not task or task.status != "failed":
                return False

            task.status = "pending"
            task.error_message = None
            task.started_at = None
            task.completed_at = None
            task.unlock()

            await db.commit()

            # Despertar workers para procesar la tarea
            self._task_notification.set()

            print(f"🔄 Tarea reintentada manualmente: {task_id}")
            return True

    async def get_all_tasks(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Obtener todas las tareas para el dashboard"""
        async with async_session_factory() as db:
            # Obtener tareas ordenadas por fecha de creación (más recientes primero)
            result = await db.execute(
                select(Task)
                .order_by(Task.created_at.desc())
                .limit(limit)
            )
            tasks = result.scalars().all()

            # Convertir a formato dict para el dashboard
            tasks_data = []
            for task in tasks:
                task_dict = {
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "status": task.status,
                    "priority": task.priority,
                    "created_at": task.created_at.isoformat() if task.created_at else None,
                    "started_at": task.started_at.isoformat() if task.started_at else None,
                    "completed_at": task.completed_at.isoformat() if task.completed_at else None,
                    "worker_id": task.worker_id,
                    "retry_count": task.retry_count,
                    "max_retries": task.max_retries,
                    "error_message": task.error_message,
                    "data": task.data
                }
                tasks_data.append(task_dict)

            return tasks_data


# Instancia global optimizada
optimized_thread_queue_manager = OptimizedThreadQueueManager()
