import asyncio
import json
import uuid
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, AsyncGenerator
from contextlib import asynccontextmanager

import redis.asyncio as redis
import ujson


class AsyncRedisManager:
    """Gestor Redis asíncrono para cola y eventos en tiempo real"""

    def __init__(self):
        self.redis: Optional[redis.Redis] = None
        self._connected = False
        self._pubsub = None
        self._worker_heartbeats: Dict[str, datetime] = {}

        # Keys de Redis
        self.KEYS = {
            "tasks_pending": "tasks:pending",
            "tasks_processing": "tasks:processing",
            "tasks_completed": "tasks:completed",
            "tasks_failed": "tasks:failed",
            "tasks_cancelled": "tasks:cancelled",
            "workers_active": "workers:active",
            "events_stream": "events:stream",
            "stats": "stats:global",
            "task_data": "task:data:{}",  # task:data:{task_id}
            "worker_status": "worker:status:{}",  # worker:status:{worker_id}
        }

    async def connect(self, redis_url: str = "redis://redis:6379"):
        """Conectar a Redis con reconexión automática"""
        try:
            self.redis = redis.from_url(
                redis_url,
                encoding="utf-8",
                decode_responses=True,
                retry_on_timeout=True,
                retry_on_error=[ConnectionError, TimeoutError],
                socket_keepalive=True,
                socket_keepalive_options={},
                health_check_interval=30,
            )

            # Verificar conexión
            await self.redis.ping()
            self._connected = True
            print("✅ Redis conectado exitosamente")

            # Inicializar PubSub
            self._pubsub = self.redis.pubsub()

            # Limpiar datos antiguos al conectar
            await self._cleanup_on_startup()

            return True

        except Exception as e:
            print(f"❌ Error conectando a Redis: {e}")
            self._connected = False
            return False

    async def disconnect(self):
        """Desconectar Redis"""
        try:
            if self._pubsub:
                await self._pubsub.close()

            if self.redis:
                await self.redis.close()

            self._connected = False
            print("✅ Redis desconectado")

        except Exception as e:
            print(f"❌ Error desconectando Redis: {e}")

    @property
    def is_connected(self) -> bool:
        return self._connected and self.redis is not None

    # === GESTIÓN DE TAREAS ===

    async def add_task(self, task_id: str, task_data: Dict[str, Any]) -> bool:
        """Agregar tarea a cola pendiente"""
        if not self.is_connected:
            return False

        try:
            pipe = self.redis.pipeline()

            # Agregar a cola pendiente con prioridad
            priority = task_data.get("priority", 5)
            pipe.zadd(self.KEYS["tasks_pending"], {task_id: priority})

            # Guardar datos completos de la tarea
            pipe.hset(
                self.KEYS["task_data"].format(task_id),
                mapping={
                    "task_id": task_id,
                    "task_type": task_data.get("task_type", "unknown"),
                    "status": "pending",
                    "data": ujson.dumps(task_data.get("data", {})),
                    "priority": priority,
                    "created_at": datetime.utcnow().isoformat(),
                    "retry_count": 0,
                    "max_retries": task_data.get("max_retries", 3),
                },
            )

            # Establecer TTL de 24 horas para datos de tarea
            pipe.expire(self.KEYS["task_data"].format(task_id), 86400)

            await pipe.execute()

            # Publicar evento
            await self.publish_event(
                {
                    "event_type": "task_created",
                    "task_id": task_id,
                    "task_type": task_data.get("task_type"),
                    "priority": priority,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            print(f"📝 Tarea Redis agregada: {task_id}")
            return True

        except Exception as e:
            print(f"❌ Error agregando tarea a Redis: {e}")
            return False

    async def get_next_task(self, worker_id: str) -> Optional[Dict[str, Any]]:
        """Obtener siguiente tarea con bloqueo atómico"""
        if not self.is_connected:
            return None

        try:
            # Usar BZPOPMIN para obtener tarea con menor prioridad (bloqueo atómico)
            result = await self.redis.bzpopmin(self.KEYS["tasks_pending"], timeout=5)

            if not result:
                return None

            task_id = result[1]  # result = (key, member, score)

            # Mover a processing y actualizar datos
            pipe = self.redis.pipeline()
            pipe.zadd(
                self.KEYS["tasks_processing"], {task_id: datetime.utcnow().timestamp()}
            )

            # Actualizar datos de tarea
            task_updates = {
                "status": "processing",
                "worker_id": worker_id,
                "started_at": datetime.utcnow().isoformat(),
                "locked_at": datetime.utcnow().isoformat(),
            }
            pipe.hmset(self.KEYS["task_data"].format(task_id), task_updates)

            await pipe.execute()

            # Obtener datos completos
            task_data = await self.redis.hgetall(self.KEYS["task_data"].format(task_id))

            if task_data:
                # Parsear datos JSON
                task_data["data"] = ujson.loads(task_data.get("data", "{}"))

                # Publicar evento
                await self.publish_event(
                    {
                        "event_type": "task_started",
                        "task_id": task_id,
                        "worker_id": worker_id,
                        "task_type": task_data.get("task_type"),
                        "timestamp": datetime.utcnow().isoformat(),
                    }
                )

                print(f"📋 Tarea Redis asignada a {worker_id}: {task_id}")
                return task_data

            return None

        except Exception as e:
            print(f"❌ Error obteniendo tarea de Redis: {e}")
            return None

    async def complete_task(self, task_id: str, result: Dict[str, Any] = None) -> bool:
        """Marcar tarea como completada"""
        if not self.is_connected:
            return False

        try:
            pipe = self.redis.pipeline()

            # Remover de processing y agregar a completed
            pipe.zrem(self.KEYS["tasks_processing"], task_id)
            pipe.zadd(
                self.KEYS["tasks_completed"], {task_id: datetime.utcnow().timestamp()}
            )

            # Actualizar datos
            updates = {
                "status": "completed",
                "completed_at": datetime.utcnow().isoformat(),
                "result": ujson.dumps(result or {}),
            }
            pipe.hmset(self.KEYS["task_data"].format(task_id), updates)

            await pipe.execute()

            # Publicar evento
            await self.publish_event(
                {
                    "event_type": "task_completed",
                    "task_id": task_id,
                    "timestamp": datetime.utcnow().isoformat(),
                    "result": result,
                }
            )

            print(f"✅ Tarea Redis completada: {task_id}")
            return True

        except Exception as e:
            print(f"❌ Error completando tarea en Redis: {e}")
            return False

    async def fail_task(self, task_id: str, error: str, can_retry: bool = True) -> bool:
        """Marcar tarea como fallida con posible reintento"""
        if not self.is_connected:
            return False

        try:
            # Obtener datos actuales
            task_data = await self.redis.hgetall(self.KEYS["task_data"].format(task_id))
            if not task_data:
                return False

            retry_count = int(task_data.get("retry_count", 0))
            max_retries = int(task_data.get("max_retries", 3))

            pipe = self.redis.pipeline()
            pipe.zrem(self.KEYS["tasks_processing"], task_id)

            if can_retry and retry_count < max_retries:
                # Programar reintento con delay exponencial
                delay = min(30 * (2**retry_count), 300)  # máximo 5 minutos
                retry_time = datetime.utcnow() + timedelta(seconds=delay)

                pipe.zadd(self.KEYS["tasks_pending"], {task_id: retry_time.timestamp()})

                updates = {
                    "status": "pending",
                    "retry_count": retry_count + 1,
                    "last_error": error,
                    "retry_scheduled_at": retry_time.isoformat(),
                    "worker_id": "",  # Limpiar worker
                    "started_at": "",
                    "locked_at": "",
                }

                print(
                    f"🔄 Tarea Redis programada para reintento: {task_id} (intento {retry_count + 1})"
                )

            else:
                # Fallo definitivo
                pipe.zadd(
                    self.KEYS["tasks_failed"], {task_id: datetime.utcnow().timestamp()}
                )

                updates = {
                    "status": "failed",
                    "failed_at": datetime.utcnow().isoformat(),
                    "final_error": error,
                    "retry_count": retry_count,
                }

                print(f"💀 Tarea Redis falló definitivamente: {task_id}")

            pipe.hmset(self.KEYS["task_data"].format(task_id), updates)
            await pipe.execute()

            # Publicar evento
            await self.publish_event(
                {
                    "event_type": "task_failed",
                    "task_id": task_id,
                    "error": error,
                    "retry_count": retry_count,
                    "can_retry": can_retry and retry_count < max_retries,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            return True

        except Exception as e:
            print(f"❌ Error marcando tarea como fallida en Redis: {e}")
            return False

    # === GESTIÓN DE WORKERS ===

    async def register_worker(self, worker_id: str) -> bool:
        """Registrar worker activo"""
        if not self.is_connected:
            return False

        try:
            pipe = self.redis.pipeline()
            pipe.sadd(self.KEYS["workers_active"], worker_id)

            # Datos del worker
            worker_data = {
                "worker_id": worker_id,
                "status": "idle",
                "registered_at": datetime.utcnow().isoformat(),
                "last_heartbeat": datetime.utcnow().isoformat(),
                "tasks_processed": 0,
                "current_task": "",
            }

            pipe.hset(self.KEYS["worker_status"].format(worker_id), mapping=worker_data)
            pipe.expire(
                self.KEYS["worker_status"].format(worker_id), 300
            )  # 5 minutos TTL

            await pipe.execute()

            self._worker_heartbeats[worker_id] = datetime.utcnow()

            await self.publish_event(
                {
                    "event_type": "worker_registered",
                    "worker_id": worker_id,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            print(f"🔨 Worker Redis registrado: {worker_id}")
            return True

        except Exception as e:
            print(f"❌ Error registrando worker en Redis: {e}")
            return False

    async def update_worker_status(
        self, worker_id: str, status: str, current_task: str = None
    ) -> bool:
        """Actualizar estado del worker"""
        if not self.is_connected:
            return False

        try:
            updates = {
                "status": status,
                "last_heartbeat": datetime.utcnow().isoformat(),
            }

            if current_task:
                updates["current_task"] = current_task
            elif status == "idle":
                updates["current_task"] = ""

            await self.redis.hmset(
                self.KEYS["worker_status"].format(worker_id), updates
            )
            await self.redis.expire(self.KEYS["worker_status"].format(worker_id), 300)

            self._worker_heartbeats[worker_id] = datetime.utcnow()

            return True

        except Exception as e:
            print(f"❌ Error actualizando worker en Redis: {e}")
            return False

    async def unregister_worker(self, worker_id: str) -> bool:
        """Desregistrar worker"""
        if not self.is_connected:
            return False

        try:
            pipe = self.redis.pipeline()
            pipe.srem(self.KEYS["workers_active"], worker_id)
            pipe.delete(self.KEYS["worker_status"].format(worker_id))
            await pipe.execute()

            if worker_id in self._worker_heartbeats:
                del self._worker_heartbeats[worker_id]

            await self.publish_event(
                {
                    "event_type": "worker_unregistered",
                    "worker_id": worker_id,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            print(f"🛑 Worker Redis desregistrado: {worker_id}")
            return True

        except Exception as e:
            print(f"❌ Error desregistrando worker de Redis: {e}")
            return False

    # === EVENTOS Y MONITOREO ===

    async def publish_event(self, event_data: Dict[str, Any]) -> bool:
        """Publicar evento en tiempo real"""
        if not self.is_connected:
            return False

        try:
            # Agregar timestamp si no existe
            if "timestamp" not in event_data:
                event_data["timestamp"] = datetime.utcnow().isoformat()

            # Publicar en stream
            await self.redis.publish(
                self.KEYS["events_stream"], ujson.dumps(event_data)
            )

            # Mantener historial limitado
            await self.redis.lpush(
                f"{self.KEYS['events_stream']}:history", ujson.dumps(event_data)
            )
            await self.redis.ltrim(
                f"{self.KEYS['events_stream']}:history", 0, 999
            )  # Últimos 1000 eventos

            return True

        except Exception as e:
            print(f"❌ Error publicando evento en Redis: {e}")
            return False

    async def subscribe_events(self) -> AsyncGenerator[Dict[str, Any], None]:
        """Suscribirse a eventos en tiempo real"""
        if not self.is_connected or not self._pubsub:
            return

        try:
            await self._pubsub.subscribe(self.KEYS["events_stream"])

            async for message in self._pubsub.listen():
                if message["type"] == "message":
                    try:
                        event_data = ujson.loads(message["data"])
                        yield event_data
                    except Exception as e:
                        print(f"❌ Error parseando evento: {e}")

        except Exception as e:
            print(f"❌ Error en suscripción de eventos: {e}")

    async def get_stats(self) -> Dict[str, Any]:
        """Obtener estadísticas completas"""
        if not self.is_connected:
            return {}

        try:
            pipe = self.redis.pipeline()

            # Contar tareas por estado
            pipe.zcard(self.KEYS["tasks_pending"])
            pipe.zcard(self.KEYS["tasks_processing"])
            pipe.zcard(self.KEYS["tasks_completed"])
            pipe.zcard(self.KEYS["tasks_failed"])
            pipe.zcard(self.KEYS["tasks_cancelled"])

            # Workers activos
            pipe.scard(self.KEYS["workers_active"])

            results = await pipe.execute()

            # Obtener detalles de workers
            workers = await self.get_workers_status()
            active_workers = sum(1 for w in workers if w["status"] == "active")

            stats = {
                "task_counts": {
                    "pending": results[0],
                    "processing": results[1],
                    "completed": results[2],
                    "failed": results[3],
                    "cancelled": results[4],
                },
                "total_workers": results[5],
                "active_workers": active_workers,
                "workers": workers,
                "timestamp": datetime.utcnow().isoformat(),
                "uptime_seconds": 0,  # Será calculado por el caller
            }

            # Publicar stats como evento
            await self.publish_event(
                {
                    "event_type": "queue_stats",
                    "data": stats,
                    "timestamp": datetime.utcnow().isoformat(),
                }
            )

            return stats

        except Exception as e:
            print(f"❌ Error obteniendo stats de Redis: {e}")
            return {}

    async def get_workers_status(self) -> List[Dict[str, Any]]:
        """Obtener estado de todos los workers"""
        if not self.is_connected:
            return []

        try:
            workers_ids = await self.redis.smembers(self.KEYS["workers_active"])
            workers_data = []

            for worker_id in workers_ids:
                worker_info = await self.redis.hgetall(
                    self.KEYS["worker_status"].format(worker_id)
                )
                if worker_info:
                    workers_data.append(worker_info)

            return workers_data

        except Exception as e:
            print(f"❌ Error obteniendo workers de Redis: {e}")
            return []

    async def get_recent_events(self, limit: int = 50) -> List[Dict[str, Any]]:
        """Obtener eventos recientes del historial"""
        if not self.is_connected:
            return []

        try:
            events_raw = await self.redis.lrange(
                f"{self.KEYS['events_stream']}:history", 0, limit - 1
            )
            events = []

            for event_raw in events_raw:
                try:
                    event = ujson.loads(event_raw)
                    events.append(event)
                except Exception as e:
                    print(f"❌ Error parseando evento del historial: {e}")

            return events

        except Exception as e:
            print(f"❌ Error obteniendo eventos recientes: {e}")
            return []

    # === UTILIDADES ===

    async def _cleanup_on_startup(self):
        """Limpiar datos obsoletos al iniciar"""
        try:
            # Limpiar workers inactivos (más de 5 minutos sin heartbeat)
            cutoff = datetime.utcnow() - timedelta(minutes=5)

            workers_ids = await self.redis.smembers(self.KEYS["workers_active"])
            for worker_id in workers_ids:
                worker_info = await self.redis.hgetall(
                    self.KEYS["worker_status"].format(worker_id)
                )
                if worker_info and "last_heartbeat" in worker_info:
                    try:
                        last_heartbeat = datetime.fromisoformat(
                            worker_info["last_heartbeat"]
                        )
                        if last_heartbeat < cutoff:
                            await self.unregister_worker(worker_id)
                            print(f"🧹 Worker inactivo eliminado: {worker_id}")
                    except Exception:
                        await self.unregister_worker(worker_id)

            # Recuperar tareas huérfanas (en processing sin worker activo)
            processing_tasks = await self.redis.zrange(
                self.KEYS["tasks_processing"], 0, -1
            )
            for task_id in processing_tasks:
                task_data = await self.redis.hgetall(
                    self.KEYS["task_data"].format(task_id)
                )
                if task_data:
                    worker_id = task_data.get("worker_id")
                    if worker_id and worker_id not in workers_ids:
                        # Worker no existe, mover tarea de vuelta a pending
                        await self._recover_orphaned_task(task_id, task_data)

            print("✅ Limpieza inicial de Redis completada")

        except Exception as e:
            print(f"❌ Error en limpieza inicial: {e}")

    async def _recover_orphaned_task(self, task_id: str, task_data: Dict[str, Any]):
        """Recuperar tarea huérfana"""
        try:
            pipe = self.redis.pipeline()
            pipe.zrem(self.KEYS["tasks_processing"], task_id)

            # Volver a pending con prioridad original
            priority = float(task_data.get("priority", 5))
            pipe.zadd(self.KEYS["tasks_pending"], {task_id: priority})

            # Limpiar datos de worker
            updates = {
                "status": "pending",
                "worker_id": "",
                "started_at": "",
                "locked_at": "",
            }
            pipe.hmset(self.KEYS["task_data"].format(task_id), updates)

            await pipe.execute()

            print(f"🔧 Tarea huérfana recuperada: {task_id}")

        except Exception as e:
            print(f"❌ Error recuperando tarea huérfana: {e}")

    async def health_check(self) -> Dict[str, Any]:
        """Verificación de salud de Redis"""
        try:
            if not self.is_connected:
                return {"status": "disconnected", "error": "No connection to Redis"}

            # Test básico de conectividad
            pong = await self.redis.ping()
            if not pong:
                return {"status": "error", "error": "Redis ping failed"}

            # Obtener info básica
            info = await self.redis.info()

            return {
                "status": "healthy",
                "connected": True,
                "redis_version": info.get("redis_version", "unknown"),
                "used_memory": info.get("used_memory_human", "unknown"),
                "connected_clients": info.get("connected_clients", 0),
                "uptime_in_seconds": info.get("uptime_in_seconds", 0),
            }

        except Exception as e:
            return {"status": "error", "connected": False, "error": str(e)}


# Instancia global
redis_manager = AsyncRedisManager()
