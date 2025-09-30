import asyncio
import uuid
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, and_, or_, update
from contextlib import asynccontextmanager
from enum import IntEnum

from app.config.database import async_session_factory
from app.models.task import Task


class TaskPriority(IntEnum):
    """Sistema de prioridades alineado con rangos especializados de colas"""
    
    # CRÍTICAS [1-2] - Operaciones académicas críticas
    SYSTEM_CRITICAL = 1
    ROLLBACK_OPERATION = 1
    AUTH_OPERATIONS = 1
    INSCRIPCIONES = 1    # Inscripciones son críticas
    NOTAS = 2           # Notas son críticas
    
    # IMPORTANTES [3-4] - Personas clave del sistema  
    DOCENTES = 3        # Docentes son importantes
    ESTUDIANTES = 4     # Estudiantes son importantes
    
    # MEDIAS [5-6] - Gestión académica 
    GRUPOS = 5          # Grupos académicos
    MATERIAS = 6        # Materias y asignaturas
    CARRERAS = 6        # Carreras académicas
    GESTIONES = 6       # Gestiones académicas
    
    # NORMALES [7-8] - Configuración de soporte
    HORARIOS = 7        # Horarios y cronogramas
    AULAS = 8           # Aulas y espacios físicos
    NIVELES = 8         # Niveles académicos
    PLANES_ESTUDIO = 8  # Planes de estudio
    
    # BAJAS [9-10] - Datos auxiliares
    PRERREQUISITOS = 9  # Prerrequisitos de materias
    DETALLES = 10       # Detalles varios y datos auxiliares
    
    # Prioridad por defecto
    DEFAULT = 10


class QueueType(IntEnum):
    """Tipos de colas especializadas - Base predefinida"""
    CRITICAL = 1    # Cola para tareas críticas (prioridad 1-2)
    HIGH = 2        # Cola para tareas de alta prioridad (prioridad 3)
    NORMAL = 3      # Cola para tareas normales (prioridad 4-5)
    BULK = 4        # Cola para operaciones masivas

# Clase para manejar colas dinámicas
class DynamicQueue:
    """Representa una cola dinámica personalizada"""
    def __init__(self, queue_id: str, name: str, config: dict):
        self.queue_id = queue_id
        self.name = name
        self.config = config
        self.created_at = datetime.utcnow()
        
    def __hash__(self):
        return hash(self.queue_id)
        
    def __eq__(self, other):
        return isinstance(other, DynamicQueue) and self.queue_id == other.queue_id


class DynamicThreadQueueManager:
    """
    Sistema de colas asíncrono dinámico con múltiples colas por prioridad
    y capacidad de escalar workers y colas en tiempo real
    """

    def __init__(self):
        self._running = False
        self._workers = {}  # {worker_id: worker_info}
        self._worker_tasks = {}  # {worker_id: asyncio.Task}
        self._queues = {}  # {queue_key: queue_info} - Ahora soporta tanto QueueType como DynamicQueue
        self._dynamic_queues = {}  # {queue_id: DynamicQueue} - Colas dinámicas
        self._max_workers = 4
        self._check_interval = 5.0
        self._lock_timeout = timedelta(minutes=5)
        self._task_notifications = {}  # {queue_key: asyncio.Event}
        self._next_queue_id = 100  # Para generar IDs únicos de colas dinámicas
        
        # SISTEMA DE ASIGNACIÓN EXCLUSIVA DE TAREAS
        self._task_assignments = {}  # {task_id: queue_id} - Asignación exclusiva
        self._assignment_lock = asyncio.Lock()  # Lock para asignación thread-safe
        
        # SISTEMA COMPLETAMENTE DINÁMICO - SIN COLAS PREDEFINIDAS
        self._base_queue_configs = {}  # Vacío - sin colas predefinidas
        
        # Configuración unificada - solo colas dinámicas
        self._queue_configs = {}  # Inicializar vacío
        
        # Crear una cola inicial automáticamente si no hay ninguna
        self._ensure_default_queue = False  # DESACTIVADO: Sistema manual
        
        print("🚀 INICIALIZADO: Sistema completamente dinámico")
        print("📋 Distribución automática entre colas disponibles")
        print("🔧 Auto-creación de cola por defecto cuando sea necesario")

        # Mapeo de tipos de tarea a prioridades - COMPLETO con GET/POST/PUT/DELETE
        self._task_priority_map = {
            # Sistema crítico
            "rollback_operation": TaskPriority.ROLLBACK_OPERATION,
            "auth_operation": TaskPriority.AUTH_OPERATIONS,
            "system_critical": TaskPriority.SYSTEM_CRITICAL,
            
            # Alta prioridad - Operaciones académicas críticas (PRIORIDAD 2)
            # Inscripciones - CRUD completo
            "create_inscripcion": TaskPriority.INSCRIPCIONES,
            "update_inscripcion": TaskPriority.INSCRIPCIONES,
            "delete_inscripcion": TaskPriority.INSCRIPCIONES,
            "get_inscripcion": TaskPriority.INSCRIPCIONES,
            "get_inscripciones_list": TaskPriority.INSCRIPCIONES,
            
            # Notas - CRUD completo
            "create_nota": TaskPriority.NOTAS,
            "update_nota": TaskPriority.NOTAS,
            "delete_nota": TaskPriority.NOTAS,
            "get_nota": TaskPriority.NOTAS,
            "get_notas_list": TaskPriority.NOTAS,
            
            # Horarios - CRUD completo
            "create_horario": TaskPriority.HORARIOS,
            "update_horario": TaskPriority.HORARIOS,
            "delete_horario": TaskPriority.HORARIOS,
            "get_horario": TaskPriority.HORARIOS,
            "get_horarios_list": TaskPriority.HORARIOS,
            
            # Media-alta - Personas y grupos (PRIORIDAD 3)
            # Estudiantes - CRUD completo
            "create_estudiante": TaskPriority.ESTUDIANTES,
            "update_estudiante": TaskPriority.ESTUDIANTES,
            "delete_estudiante": TaskPriority.ESTUDIANTES,
            "get_estudiante": TaskPriority.ESTUDIANTES,
            "get_estudiantes_list": TaskPriority.ESTUDIANTES,
            
            # Docentes - CRUD completo
            "create_docente": TaskPriority.DOCENTES,
            "update_docente": TaskPriority.DOCENTES,
            "delete_docente": TaskPriority.DOCENTES,
            "get_docente": TaskPriority.DOCENTES,
            "get_docentes_list": TaskPriority.DOCENTES,
            
            # Grupos - CRUD completo
            "create_grupo": TaskPriority.GRUPOS,
            "update_grupo": TaskPriority.GRUPOS,
            "delete_grupo": TaskPriority.GRUPOS,
            "get_grupo": TaskPriority.GRUPOS,
            "get_grupos_list": TaskPriority.GRUPOS,
            
            # Media - Configuration académica (PRIORIDAD 4)
            # Materias - CRUD completo
            "create_materia": TaskPriority.MATERIAS,
            "update_materia": TaskPriority.MATERIAS,
            "delete_materia": TaskPriority.MATERIAS,
            "get_materia": TaskPriority.MATERIAS,
            "get_materias_list": TaskPriority.MATERIAS,
            
            # Carreras - CRUD completo
            "create_carrera": TaskPriority.CARRERAS,
            "update_carrera": TaskPriority.CARRERAS,
            "delete_carrera": TaskPriority.CARRERAS,
            "get_carrera": TaskPriority.CARRERAS,
            "get_carreras_list": TaskPriority.CARRERAS,
            
            # Gestiones - CRUD completo
            "create_gestion": TaskPriority.GESTIONES,
            "update_gestion": TaskPriority.GESTIONES,
            "delete_gestion": TaskPriority.GESTIONES,
            "get_gestion": TaskPriority.GESTIONES,
            "get_gestiones_list": TaskPriority.GESTIONES,
            
            # Normal - Datos de soporte (PRIORIDAD 5)
            # Aulas - CRUD completo
            "create_aula": TaskPriority.AULAS,
            "update_aula": TaskPriority.AULAS,
            "delete_aula": TaskPriority.AULAS,
            "get_aula": TaskPriority.AULAS,
            "get_aulas_list": TaskPriority.AULAS,
            
            # Niveles - CRUD completo
            "create_nivel": TaskPriority.NIVELES,
            "update_nivel": TaskPriority.NIVELES,
            "delete_nivel": TaskPriority.NIVELES,
            "get_nivel": TaskPriority.NIVELES,
            "get_niveles_list": TaskPriority.NIVELES,
            
            # Planes de Estudio - CRUD completo
            "create_plan_estudio": TaskPriority.PLANES_ESTUDIO,
            "update_plan_estudio": TaskPriority.PLANES_ESTUDIO,
            "delete_plan_estudio": TaskPriority.PLANES_ESTUDIO,
            "get_plan_estudio": TaskPriority.PLANES_ESTUDIO,
            "get_planes_estudio_list": TaskPriority.PLANES_ESTUDIO,
            
            # Prerrequisitos - CRUD completo
            "create_prerrequisito": TaskPriority.PRERREQUISITOS,
            "update_prerrequisito": TaskPriority.PRERREQUISITOS,
            "delete_prerrequisito": TaskPriority.PRERREQUISITOS,
            "get_prerrequisito": TaskPriority.PRERREQUISITOS,
            "get_prerrequisitos_list": TaskPriority.PRERREQUISITOS,
            
            # Detalles - CRUD completo
            "create_detalle": TaskPriority.DETALLES,
            "update_detalle": TaskPriority.DETALLES,
            "delete_detalle": TaskPriority.DETALLES,
            "get_detalle": TaskPriority.DETALLES,
            "get_detalles_list": TaskPriority.DETALLES,
        }

        self._stats = {
            "tasks_processed": 0,
            "tasks_failed": 0,
            "tasks_completed": 0,
            "workers_active": 0,
            "uptime_start": None,
            "last_db_check": None,
            "queues_active": 0,
            "dynamic_scaling_events": 0,
        }

    async def start(self, max_workers: int = 4):
        """Iniciar el sistema de colas dinámico con múltiples colas especializadas"""
        if self._running:
            print("⚠️ Sistema de colas ya está en ejecución")
            return

        self._running = True
        self._max_workers = max_workers
        self._stats["uptime_start"] = datetime.utcnow()

        print(f"🚀 DynamicThreadQueueManager iniciando con {max_workers} workers...")

        # Inicializar colas y notificaciones
        await self._initialize_queues()
        
        # Recuperar tareas huérfanas
        await self._recover_orphaned_tasks()

        # Iniciar workers distribuidos por colas
        await self._start_initial_workers()

        # Iniciar monitores para cada cola
        await self._start_queue_monitors()

        print(f"✅ Sistema dinámico iniciado: {len(self._workers)} workers, {len(self._queues)} colas")
        self._stats["workers_active"] = len(self._workers)
        self._stats["queues_active"] = len(self._queues)

    async def _initialize_queues(self):
        """Inicializar sistema completamente dinámico"""
        print("🔧 Sistema dinámico iniciando - No hay colas predefinidas")
        
        # DESACTIVADO: No crear colas automáticamente
        # if not self._queue_configs and self._ensure_default_queue:
        #     print("📋 No hay colas disponibles, creando cola automática inicial...")
        #     await self._create_auto_default_queue()
        print("🔧 Sistema manual: No se crearán colas automáticas")
        
        # Inicializar colas existentes (si las hay)
        for queue_type, config in self._queue_configs.items():
            self._queues[queue_type] = {
                "config": config,
                "workers": [],
                "active_workers": 0,
                "pending_tasks": 0,
                "processed_tasks": 0,
                "last_activity": datetime.utcnow()
            }
            self._task_notifications[queue_type] = asyncio.Event()
            print(f"🔧 Cola dinámica {config['name']} inicializada (prioridad {config['priority_range']})")
        
        if not self._queue_configs:
            print("⚠️ Sistema iniciado sin colas - Se crearán automáticamente según demanda")

    async def _start_initial_workers(self):
        """Iniciar workers iniciales distribuidos por colas"""
        total_workers = 0
        
        for queue_type, queue_info in self._queues.items():
            config = queue_info["config"]
            initial_workers = min(config["min_workers"], self._max_workers - total_workers)
            
            for i in range(initial_workers):
                await self._add_worker_to_queue(queue_type)
                total_workers += 1
                
                if total_workers >= self._max_workers:
                    break
            
            if total_workers >= self._max_workers:
                break

    async def _start_queue_monitors(self):
        """Iniciar monitores especializados para cada cola"""
        for queue_type in self._queues.keys():
            monitor_id = f"monitor_{self._queue_configs[queue_type]['name']}"
            monitor_task = asyncio.create_task(self._queue_monitor(queue_type))
            self._worker_tasks[monitor_id] = monitor_task
            print(f"📊 Monitor {monitor_id} iniciado")

    async def _add_worker_to_queue(self, queue_type: QueueType) -> str:
        """Agregar un nuevo worker a una cola específica"""
        config = self._queue_configs[queue_type]
        worker_id = f"worker_{config['name']}_{len(self._queues[queue_type]['workers'])+1}"
        
        worker_info = {
            "queue_type": queue_type,
            "queue_name": config["name"],
            "created_at": datetime.utcnow(),
            "tasks_processed": 0,
            "last_activity": datetime.utcnow()
        }
        
        # Crear task del worker
        worker_task = asyncio.create_task(self._run_queue_worker(worker_id, queue_type))
        
        # Registrar worker
        self._workers[worker_id] = worker_info
        self._worker_tasks[worker_id] = worker_task
        self._queues[queue_type]["workers"].append(worker_id)
        self._queues[queue_type]["active_workers"] += 1
        
        print(f"🔨 Worker {worker_id} agregado a cola {config['name']}")
        return worker_id

    def stop(self):
        """Detener el sistema de colas dinámico"""
        if not self._running:
            return

        print("🛑 Deteniendo DynamicThreadQueueManager...")
        self._running = False

        # Despertar todos los workers para que puedan terminar
        for notification in self._task_notifications.values():
            notification.set()

        # Cancelar todas las tareas de workers
        for task in self._worker_tasks.values():
            task.cancel()

        # Limpiar estructuras
        self._worker_tasks.clear()
        self._workers.clear()
        self._queues.clear()
        self._task_notifications.clear()
        self._task_assignments.clear()  # Limpiar asignaciones
        self._stats["workers_active"] = 0
        self._stats["queues_active"] = 0

        print("✅ DynamicThreadQueueManager detenido")

    async def scale_workers(self, queue_key, target_workers: int) -> Dict[str, Any]:
        """Escalar workers de una cola específica dinámicamente (SIN LÍMITES)"""
        if not self._running:
            raise ValueError("El sistema de colas no está ejecutándose")
        
        if queue_key not in self._queue_configs:
            raise ValueError(f"Cola no encontrada: {queue_key}")
            
        config = self._queue_configs[queue_key]
        current_workers = self._queues[queue_key]["active_workers"]
        
        # Sin límites - Solo validar que no sea negativo
        target_workers = max(0, target_workers)
        
        result = {
            "queue_key": str(queue_key),
            "queue_name": config["name"],
            "is_dynamic": config.get("is_dynamic", False),
            "previous_workers": current_workers,
            "target_workers": target_workers,
            "actions": []
        }
        
        if target_workers > current_workers:
            # Agregar workers
            for _ in range(target_workers - current_workers):
                worker_id = await self._add_worker_to_queue(queue_key)
                result["actions"].append(f"Added worker {worker_id}")
                self._stats["dynamic_scaling_events"] += 1
                
        elif target_workers < current_workers:
            # Remover workers
            workers_to_remove = current_workers - target_workers
            queue_workers = self._queues[queue_key]["workers"].copy()
            
            for i in range(workers_to_remove):
                if queue_workers:
                    worker_id = queue_workers.pop()
                    await self._remove_worker(worker_id)
                    result["actions"].append(f"Removed worker {worker_id}")
                    self._stats["dynamic_scaling_events"] += 1
        
        result["final_workers"] = self._queues[queue_key]["active_workers"]
        return result

    async def scale_workers_by_name(self, queue_name: str, target_workers: int) -> Dict[str, Any]:
        """Escalar workers por nombre de cola"""
        # Buscar cola por nombre
        queue_key = None
        for key, config in self._queue_configs.items():
            if config["name"] == queue_name:
                queue_key = key
                break
        
        if queue_key is None:
            raise ValueError(f"Cola no encontrada con nombre: {queue_name}")
        
        return await self.scale_workers(queue_key, target_workers)

    async def _remove_worker(self, worker_id: str):
        """Remover un worker específico"""
        if worker_id not in self._workers:
            return
            
        worker_info = self._workers[worker_id]
        queue_type = worker_info["queue_type"]
        
        # Cancelar task del worker
        if worker_id in self._worker_tasks:
            self._worker_tasks[worker_id].cancel()
            del self._worker_tasks[worker_id]
        
        # Remover de estructuras
        del self._workers[worker_id]
        self._queues[queue_type]["workers"].remove(worker_id)
        self._queues[queue_type]["active_workers"] -= 1
        
        print(f"🗑️ Worker {worker_id} removido de cola {worker_info['queue_name']}")

    async def create_dynamic_queue(
        self, 
        name: str, 
        priority_min: int, 
        priority_max: int,
        min_workers: int = 0,
        max_workers: int = 5,
        check_interval: float = 5.0,
        auto_scale: bool = True
    ) -> str:
        """Crear una nueva cola completamente dinámica"""
        if not self._running:
            raise ValueError("El sistema de colas no está ejecutándose")
        
        # Generar ID único
        queue_id = f"dynamic_{self._next_queue_id}"
        self._next_queue_id += 1
        
        # Validar que no haya solapamiento de prioridades conflictivo
        for existing_key, existing_config in self._queue_configs.items():
            existing_min, existing_max = existing_config["priority_range"]
            if not (priority_max < existing_min or priority_min > existing_max):
                print(f"⚠️ Advertencia: La nueva cola '{name}' tiene solapamiento de prioridades con cola existente")
        
        # Configuración de la nueva cola
        config = {
            "name": name,
            "min_workers": min_workers,
            "max_workers": max_workers,
            "priority_range": [priority_min, priority_max],
            "check_interval": check_interval,
            "auto_scale": auto_scale,
            "is_dynamic": True,
            "created_at": datetime.utcnow().isoformat()
        }
        
        # Crear objeto de cola dinámica
        dynamic_queue = DynamicQueue(queue_id, name, config)
        self._dynamic_queues[queue_id] = dynamic_queue
        
        # Agregar a configuraciones generales
        self._queue_configs[queue_id] = config
        
        # Inicializar cola en el sistema
        self._queues[queue_id] = {
            "config": config,
            "workers": [],
            "active_workers": 0,
            "pending_tasks": 0,
            "processed_tasks": 0,
            "last_activity": datetime.utcnow(),
            "queue_object": dynamic_queue
        }
        self._task_notifications[queue_id] = asyncio.Event()
        
        # Iniciar workers iniciales
        for _ in range(min_workers):
            await self._add_worker_to_queue(queue_id)
        
        # Iniciar monitor de la cola
        monitor_id = f"monitor_{name}_{queue_id}"
        monitor_task = asyncio.create_task(self._queue_monitor(queue_id))
        self._worker_tasks[monitor_id] = monitor_task
        
        self._stats["queues_active"] = len(self._queues)
        print(f"✅ Cola dinámica '{name}' creada con ID: {queue_id}")
        return queue_id

    async def add_queue(self, queue_type: QueueType, config: Dict[str, Any]) -> bool:
        """Agregar/modificar una cola predefinida (mantenido para compatibilidad)"""
        if not self._running:
            raise ValueError("El sistema de colas no está ejecutándose")
            
        if queue_type in self._queues:
            return False  # Cola ya existe
            
        # Validar configuración
        required_keys = ["name", "min_workers", "max_workers", "priority_range", "check_interval"]
        if not all(key in config for key in required_keys):
            raise ValueError("Configuración de cola incompleta")
        
        # Agregar configuración
        config["is_dynamic"] = False
        self._queue_configs[queue_type] = config
        
        # Inicializar cola
        self._queues[queue_type] = {
            "config": config,
            "workers": [],
            "active_workers": 0,
            "pending_tasks": 0,
            "processed_tasks": 0,
            "last_activity": datetime.utcnow()
        }
        self._task_notifications[queue_type] = asyncio.Event()
        
        # Iniciar workers iniciales
        for _ in range(config["min_workers"]):
            await self._add_worker_to_queue(queue_type)
        
        # Iniciar monitor de la cola
        monitor_id = f"monitor_{config['name']}"
        monitor_task = asyncio.create_task(self._queue_monitor(queue_type))
        self._worker_tasks[monitor_id] = monitor_task
        
        self._stats["queues_active"] = len(self._queues)
        print(f"✅ Cola {config['name']} agregada dinámicamente")
        return True

    async def remove_queue(self, queue_key) -> bool:
        """Remover una cola dinámicamente (TODAS las colas son eliminables ahora)"""
        if not self._running or queue_key not in self._queues:
            return False
        
        config = self._queue_configs[queue_key]
        
        # Todas las colas son eliminables ahora - Solo advertir
        if isinstance(queue_key, QueueType):
            print(f"⚠️ Eliminando cola predefinida: {config['name']}")
        
        # Remover todos los workers de la cola
        workers_to_remove = self._queues[queue_key]["workers"].copy()
        for worker_id in workers_to_remove:
            await self._remove_worker(worker_id)
        
        # Remover monitor
        monitor_patterns = [f"monitor_{config['name']}", f"monitor_{config['name']}_{queue_key}"]
        for pattern in monitor_patterns:
            for monitor_id in list(self._worker_tasks.keys()):
                if monitor_id.startswith(pattern):
                    self._worker_tasks[monitor_id].cancel()
                    del self._worker_tasks[monitor_id]
                    break
        
        # Si es cola dinámica, remover de estructura especial
        if config.get("is_dynamic", False):
            queue_id = str(queue_key)
            if queue_id in self._dynamic_queues:
                del self._dynamic_queues[queue_id]
        
        # Limpiar estructuras generales
        del self._queues[queue_key]
        del self._task_notifications[queue_key]
        del self._queue_configs[queue_key]
        
        self._stats["queues_active"] = len(self._queues)
        print(f"🗑️ Cola {config['name']} removida dinámicamente")
        return True

    async def remove_dynamic_queue(self, queue_id: str) -> bool:
        """Remover una cola dinámica específica por ID"""
        if queue_id not in self._dynamic_queues:
            return False
        
        return await self.remove_queue(queue_id)

    async def _queue_monitor(self, queue_type: QueueType):
        """Monitor especializado para una cola específica"""
        config = self._queue_configs[queue_type]
        print(f"📊 Monitor de cola {config['name']} iniciado")

        while self._running:
            try:
                # Revisar tareas pendientes ASIGNADAS EXCLUSIVAMENTE a esta cola
                async with async_session_factory() as db:
                    # Obtener solo las tareas asignadas a esta cola específica
                    async with self._assignment_lock:
                        # CORRECCIÓN: Manejar correctamente los tipos de cola
                        queue_key_to_match = str(queue_type) if isinstance(queue_type, (int, IntEnum)) else queue_type
                        assigned_task_ids = [task_id for task_id, assigned_queue in self._task_assignments.items() 
                                           if str(assigned_queue) == queue_key_to_match]
                    
                    if not assigned_task_ids:
                        # No hay tareas asignadas a esta cola
                        pending_tasks = 0
                    else:
                        result = await db.execute(
                            select(func.count(Task.task_id)).where(
                                and_(
                                    Task.task_id.in_(assigned_task_ids),
                                    Task.status == "pending"
                                )
                            )
                        )
                        pending_tasks = result.scalar() or 0
                    
                    # Actualizar estadísticas de cola
                    self._queues[queue_type]["pending_tasks"] = pending_tasks

                    if pending_tasks > 0:
                        print(f"📋 Cola {config['name']}: {pending_tasks} tareas ASIGNADAS pendientes")
                        # Despertar workers de esta cola
                        self._task_notifications[queue_type].set()
                        
                        # Auto-escalado inteligente
                        await self._auto_scale_queue(queue_type, pending_tasks)

                    self._stats["last_db_check"] = datetime.utcnow()

                # Esperar según intervalo de la cola
                await asyncio.sleep(config["check_interval"])

            except asyncio.CancelledError:
                print(f"🛑 Monitor de cola {config['name']} cancelado")
                break
            except Exception as e:
                print(f"❌ Error en monitor de cola {config['name']}: {e}")
                await asyncio.sleep(5.0)

        print(f"🛑 Monitor de cola {config['name']} detenido")

    async def _auto_scale_queue(self, queue_type: QueueType, pending_tasks: int):
        """Auto-escalado inteligente basado en carga de trabajo"""
        config = self._queue_configs[queue_type]
        current_workers = self._queues[queue_type]["active_workers"]
        
        # Lógica de escalado simple
        if pending_tasks > current_workers * 3 and current_workers < config["max_workers"]:
            # Escalar hacia arriba si hay mucha carga
            new_workers = min(current_workers + 1, config["max_workers"])
            await self.scale_workers(queue_type, new_workers)
            print(f"🔼 Auto-escalado: Cola {config['name']} escalada a {new_workers} workers")
            
        elif pending_tasks < current_workers and current_workers > config["min_workers"]:
            # Escalar hacia abajo si hay poca carga
            new_workers = max(current_workers - 1, config["min_workers"])
            await self.scale_workers(queue_type, new_workers)
            print(f"🔽 Auto-escalado: Cola {config['name']} reducida a {new_workers} workers")

    async def _run_queue_worker(self, worker_id: str, queue_type: QueueType):
        """Worker especializado para una cola específica"""
        config = self._queue_configs[queue_type]
        print(f"🔧 Worker {worker_id} iniciado para cola {config['name']}")

        while self._running:
            try:
                # Esperar notificación de nuevas tareas en esta cola
                try:
                    await asyncio.wait_for(
                        self._task_notifications[queue_type].wait(), 
                        timeout=config["check_interval"]
                    )
                except asyncio.TimeoutError:
                    # Timeout normal, continuar
                    pass

                if not self._running:
                    break

                # Procesar tareas disponibles para esta cola
                tasks_processed = 0
                while self._running:
                    task_processed = await self._process_next_queue_task(worker_id, queue_type)

                    if not task_processed:
                        break  # No hay más tareas

                    tasks_processed += 1
                    self._workers[worker_id]["tasks_processed"] += 1
                    self._workers[worker_id]["last_activity"] = datetime.utcnow()

                    # Evitar monopolizar el worker (menos para colas críticas)
                    max_batch = 20 if queue_type == QueueType.CRITICAL else 10
                    if tasks_processed >= max_batch:
                        break

                if tasks_processed > 0:
                    print(f"🔄 Worker {worker_id} procesó {tasks_processed} tareas de cola {config['name']}")
                    self._queues[queue_type]["processed_tasks"] += tasks_processed
                    self._queues[queue_type]["last_activity"] = datetime.utcnow()

                # Reset del evento después de procesar
                if tasks_processed > 0:
                    # Despertar otros workers de la misma cola si hay más tareas
                    await asyncio.sleep(0.1)
                    self._task_notifications[queue_type].set()
                else:
                    # No hay tareas, limpiar evento de esta cola
                    self._task_notifications[queue_type].clear()

            except asyncio.CancelledError:
                print(f"🛑 Worker {worker_id} cancelado")
                break
            except Exception as e:
                print(f"❌ Error en worker {worker_id}: {e}")
                await asyncio.sleep(1.0)

        print(f"🛑 Worker {worker_id} de cola {config['name']} detenido")

    async def add_task(
        self,
        task_type: str,
        data: Dict[str, Any],
        priority: Optional[int] = None,
        max_retries: int = 0,
        rollback_data: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Agregar una tarea con prioridad inteligente basada en tipo"""
        task_id = str(uuid.uuid4())

        # Determinar prioridad automáticamente si no se especifica
        if priority is None:
            priority = self._task_priority_map.get(task_type, TaskPriority.DEFAULT)

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

            # ASIGNACIÓN EXCLUSIVA DE TAREA A COLA ESPECÍFICA
            async with self._assignment_lock:
                target_queue = await self._get_queue_for_priority_async(priority)
                if target_queue:
                    # Asignar la tarea exclusivamente a esta cola
                    self._task_assignments[task_id] = target_queue
                    
                    # Despertar workers de la cola asignada
                    self._task_notifications[target_queue].set()
                    queue_name = self._queue_configs[target_queue]["name"]
                    import logging
                    logging.info(f"📝 TAREA ASIGNADA EXCLUSIVAMENTE: {task_id} ({task_type}) [P:{priority}] → Cola: {queue_name}")
                    print(f"📝 TAREA ASIGNADA EXCLUSIVAMENTE: {task_id} ({task_type}) [P:{priority}] → Cola: {queue_name}")
                else:
                    import logging
                    logging.warning(f"⚠️ Tarea agregada pero no hay cola apropiada: {task_id} ({task_type}) [Prioridad: {priority}]")
                    print(f"⚠️ Tarea agregada pero no hay cola apropiada: {task_id} ({task_type}) [Prioridad: {priority}]")

            return task_id

    async def _get_queue_for_priority_async(self, priority: int):
        """SISTEMA DE ROUND-ROBIN PURO - VERSIÓN ASÍNCRONA"""
        
        # DESACTIVADO: No crear colas automáticamente
        if not self._queue_configs:
            print(f"❌ No hay colas disponibles - Sistema manual requiere colas pre-creadas")
            return None
        
        # ESTRATEGIA ÚNICA: SIEMPRE usar round-robin para distribución equitativa
        return self._get_best_queue_by_load(priority)
    
    def _get_queue_for_priority(self, priority: int):
        """SISTEMA DE ROUND-ROBIN PURO - VERSIÓN SÍNCRONA"""
        
        # ESTRATEGIA ÚNICA: SIEMPRE usar round-robin para distribución equitativa
        return self._get_best_queue_by_load(priority)

    
    def _get_best_queue_by_load(self, priority: int):
        """ROUND-ROBIN PURO Y DINÁMICO - Intercala tareas entre colas con misma prioridad"""
        if not self._queue_configs:
            return None
        
        # Inicializar contadores round-robin si no existen
        if not hasattr(self, '_round_robin_counters'):
            self._round_robin_counters = {}
            
        # PASO 1: Encontrar TODAS las colas compatibles con la prioridad
        compatible_queues = []
        
        for queue_key, config in self._queue_configs.items():
            priority_min, priority_max = config["priority_range"]
            
            # Verificar compatibilidad con la prioridad
            if priority_min <= priority <= priority_max:
                queue_data = {
                    "queue_key": queue_key,
                    "name": config["name"],
                    "priority_range": [priority_min, priority_max],
                    "range_key": f"{priority_min}-{priority_max}"
                }
                compatible_queues.append(queue_data)
        
        if not compatible_queues:
            print(f"❌ No hay colas compatibles para prioridad {priority}")
            return None
        
        # PASO 2: Agrupar por rango de prioridad y elegir el más específico
        range_groups = {}
        for queue in compatible_queues:
            range_key = queue["range_key"]
            if range_key not in range_groups:
                range_groups[range_key] = []
            range_groups[range_key].append(queue)
        
        # Elegir el rango más específico (menor diferencia entre min y max)
        best_range = min(range_groups.keys(), key=lambda x: 
            abs(int(x.split('-')[1]) - int(x.split('-')[0])))
        
        candidate_queues = range_groups[best_range]
        
        print(f"🎯 ROUND-ROBIN: P{priority} → Rango {best_range} con {len(candidate_queues)} colas")
        print(f"   📋 Colas disponibles: {[q['name'] for q in candidate_queues]}")
        
        # PASO 3: ROUND-ROBIN PURO - Una tarea por cola, intercalando
        if len(candidate_queues) == 1:
            # Solo una cola
            selected_queue = candidate_queues[0]
            print(f"   ✅ Única cola: {selected_queue['name']}")
        else:
            # MÚLTIPLES COLAS: ROUND-ROBIN DINÁMICO
            round_robin_key = f"P{priority}_range_{best_range}"
            
            # Inicializar contador para esta prioridad específica
            if round_robin_key not in self._round_robin_counters:
                self._round_robin_counters[round_robin_key] = 0
            
            # Calcular índice de cola usando módulo (se adapta automáticamente)
            current_counter = self._round_robin_counters[round_robin_key]
            selected_index = current_counter % len(candidate_queues)
            selected_queue = candidate_queues[selected_index]
            
            # Incrementar contador para próxima tarea
            self._round_robin_counters[round_robin_key] += 1
            
            print(f"   🎲 ROUND-ROBIN: Tarea #{current_counter + 1} → {selected_queue['name']} (índice {selected_index}/{len(candidate_queues) - 1})")
            print(f"   🔄 Próxima tarea irá a: {candidate_queues[(current_counter + 1) % len(candidate_queues)]['name']}")
            
            # Log detallado
            import logging
            logging.info(f"🎲 ROUND-ROBIN P{priority}: {current_counter + 1} → {selected_queue['name']} | Siguiente: {candidate_queues[(current_counter + 1) % len(candidate_queues)]['name']}")
        
        print(f"✅ ASIGNADA: P{priority} → {selected_queue['name']} | Rango: {selected_queue['priority_range']}")
        return selected_queue["queue_key"]

    async def optimize_queue_specialization(self):
        """Optimizar la especialización de colas existentes basado en patrones de uso"""
        try:
            if len(self._queue_configs) < 2:
                print("🔧 Necesitas al menos 2 colas para optimizar especialización")
                return False
            
            print("🎯 Iniciando optimización de especialización de colas...")
            
            # Definir rangos de prioridad especializados
            specialized_ranges = [
                (1, 2),   # Críticas: inscripciones, notas
                (3, 4),   # Importantes: docentes, estudiantes  
                (5, 6),   # Medias: grupos, materias, carreras
                (7, 8),   # Normales: aulas, niveles, planes
                (9, 10),  # Bajas: prerrequisitos, etc.
            ]
            
            # Obtener lista de colas dinámicas
            queue_keys = list(self._queue_configs.keys())
            
            # Asignar rangos especializados a las colas existentes
            assignments = []
            for i, queue_key in enumerate(queue_keys):
                if i < len(specialized_ranges):
                    range_assignment = specialized_ranges[i]
                    assignments.append((queue_key, range_assignment))
            
            # Aplicar las especializaciones
            updated_count = 0
            for queue_key, (priority_min, priority_max) in assignments:
                config = self._queue_configs[queue_key]
                old_range = config["priority_range"]
                
                # Actualizar rango de prioridad
                config["priority_range"] = [priority_min, priority_max]
                
                print(f"🔧 Cola '{config['name']}': {old_range} → [{priority_min}, {priority_max}]")
                updated_count += 1
            
            print(f"✅ Especialización completada: {updated_count} colas optimizadas")
            return True
            
        except Exception as e:
            print(f"❌ Error optimizando especialización: {e}")
            return False

    async def _create_auto_default_queue(self):
        """Crear cola automática cuando no hay ninguna disponible"""
        try:
            print(f"🔧 Creando cola automática 'auto_default'...")
            
            # Usar create_dynamic_queue en lugar de create_queue
            result = await self.create_dynamic_queue(
                name="auto_default",
                priority_min=1,
                priority_max=10,
                max_workers=3,
                min_workers=0,
                check_interval=5.0,
                auto_scale=True
            )
            
            if result:
                print(f"✅ Cola automática 'auto_default' creada exitosamente")
                return True
            else:
                print(f"❌ Error al crear cola automática")
                return False
                
        except Exception as e:
            print(f"❌ Excepción al crear cola automática: {e}")
            import traceback
            traceback.print_exc()
            return False

    async def _create_auto_queue_for_priority(self, priority: int, queue_name: str):
        """Crear automáticamente una cola dinámica para una prioridad específica"""
        try:
            # Determinar rango de prioridad automáticamente
            priority_min = priority
            priority_max = priority  # Cola específica para esta prioridad exacta
            
            # Ajustar max_workers según la prioridad
            if priority <= 2:
                max_workers = 5  # Critical priorities get more workers
            elif priority <= 5:
                max_workers = 3  # Normal priorities
            else:
                max_workers = 2  # Lower priorities
            
            # Crear la cola dinámica
            result = await self.create_dynamic_queue(
                name=queue_name,
                priority_min=priority_min,
                priority_max=priority_max,
                max_workers=max_workers
            )
            
            if result:
                print(f"✅ Cola automática creada: {queue_name} para prioridad {priority}")
            else:
                print(f"❌ Error creando cola automática para prioridad {priority}")
                
        except Exception as e:
            print(f"❌ Error en creación automática de cola: {e}")

    def get_all_queues(self) -> Dict[str, Dict]:
        """Obtener información de todas las colas (predefinidas y dinámicas)"""
        all_queues = {}
        
        for queue_key, config in self._queue_configs.items():
            queue_info = {
                "key": str(queue_key),
                "config": config,
                "is_predefined": isinstance(queue_key, QueueType),
                "is_dynamic": config.get("is_dynamic", False)
            }
            
            if queue_key in self._queues:
                queue_stats = self._queues[queue_key]
                queue_info.update({
                    "active_workers": queue_stats["active_workers"],
                    "pending_tasks": queue_stats["pending_tasks"],
                    "processed_tasks": queue_stats["processed_tasks"],
                    "last_activity": queue_stats["last_activity"].isoformat() if queue_stats["last_activity"] else None,
                    "workers": queue_stats["workers"]
                })
            
            all_queues[str(queue_key)] = queue_info
        
        return all_queues

    def get_task_priority(self, task_type: str) -> int:
        """Obtener la prioridad para un tipo de tarea"""
        return self._task_priority_map.get(task_type, TaskPriority.DEFAULT)

    def set_task_priority(self, task_type: str, priority: int):
        """Establecer la prioridad para un tipo de tarea"""
        self._task_priority_map[task_type] = priority
        print(f"🔧 Prioridad actualizada: {task_type} → {priority}")

    async def _process_next_queue_task(self, worker_id: str, queue_type: QueueType) -> bool:
        """Obtener y procesar la siguiente tarea disponible para una cola específica"""
        try:
            async with async_session_factory() as db:
                # Obtener siguiente tarea de la cola específica
                task = await self._get_and_lock_queue_task(db, worker_id, queue_type)

                if not task:
                    return False

                print(f"📋 Worker {worker_id} procesando: {task.task_id} (Prioridad: {task.priority})")

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
            print(f"❌ Error crítico en _process_next_queue_task: {e}")
            return False

    async def _get_and_lock_queue_task(
        self, db: AsyncSession, worker_id: str, queue_type: QueueType
    ) -> Optional[Task]:
        """Obtener y bloquear atómicamente la siguiente tarea de una cola específica"""
        try:
            config = self._queue_configs[queue_type]
            priority_min, priority_max = config["priority_range"]
            lock_expiry = datetime.utcnow() - self._lock_timeout

            # BUSCAR SOLO TAREAS ASIGNADAS EXCLUSIVAMENTE A ESTA COLA
            async with self._assignment_lock:
                # Obtener todas las tareas asignadas a esta cola
                # CORRECCIÓN: Convertir queue_type a string si es necesario para comparación correcta
                queue_key_to_match = str(queue_type) if isinstance(queue_type, (int, IntEnum)) else queue_type
                assigned_task_ids = [task_id for task_id, assigned_queue in self._task_assignments.items() 
                                   if str(assigned_queue) == queue_key_to_match]
                
                # Debug logging
                import logging
                logging.info(f"🔍 WORKER DEBUG - Buscando tareas para cola: {queue_key_to_match}")
                logging.info(f"🔍 WORKER DEBUG - Asignaciones actuales: {list(self._task_assignments.items())}")
                logging.info(f"🔍 WORKER DEBUG - Tareas encontradas para esta cola: {len(assigned_task_ids)}")
                print(f"🔍 WORKER DEBUG - Buscando tareas para cola: {queue_key_to_match}, encontradas: {len(assigned_task_ids)}")
            
            if not assigned_task_ids:
                # No hay tareas asignadas a esta cola
                return None

            result = await db.execute(
                select(Task)
                .where(
                    and_(
                        Task.task_id.in_(assigned_task_ids),  # SOLO TAREAS ASIGNADAS A ESTA COLA
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
            print(f"Error obteniendo tarea de cola {config['name']}: {e}")
            await db.rollback()
            return None

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

            # LIMPIAR ASIGNACIÓN EXCLUSIVA AL COMPLETAR
            async with self._assignment_lock:
                if task.task_id in self._task_assignments:
                    assigned_queue = self._task_assignments.pop(task.task_id)
                    queue_name = self._queue_configs[assigned_queue]["name"]
                    print(f"🧹 Asignación limpiada: {task.task_id} de cola {queue_name}")

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
            # Programar reintento (mantener asignación para el reintento)
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
            # Fallo definitivo - limpiar asignación
            task.status = "failed"
            task.completed_at = datetime.utcnow()
            task.needs_rollback = True
            
            # LIMPIAR ASIGNACIÓN EXCLUSIVA AL FALLAR DEFINITIVAMENTE
            async with self._assignment_lock:
                if task.task_id in self._task_assignments:
                    assigned_queue = self._task_assignments.pop(task.task_id)
                    queue_name = self._queue_configs[assigned_queue]["name"]
                    print(f"🧹 Asignación limpiada por fallo: {task.task_id} de cola {queue_name}")
            
            print(f"💀 Tarea falló definitivamente: {task.task_id}")

            # Programar rollback si hay datos (DESACTIVADO TEMPORALMENTE)
            # if task.rollback_data:
            #     await self._schedule_rollback(task)
            print(f"⚠️ Rollback desactivado para depuración: {task.task_id}")

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
        """Obtener estadísticas detalladas del sistema de colas dinámico"""
        async with async_session_factory() as db:
            status_counts = {}
            statuses = ["pending", "processing", "completed", "failed", "cancelled"]

            for status in statuses:
                result = await db.execute(
                    select(func.count(Task.task_id)).where(Task.status == status)
                )
                status_counts[status] = result.scalar() or 0

            # Estadísticas por prioridad
            priority_counts = {}
            for priority in range(1, 11):
                result = await db.execute(
                    select(func.count(Task.task_id)).where(
                        and_(Task.priority == priority, Task.status == "pending")
                    )
                )
                count = result.scalar() or 0
                if count > 0:
                    priority_counts[priority] = count

            uptime = None
            if self._stats["uptime_start"]:
                uptime = datetime.utcnow() - self._stats["uptime_start"]

            # Estadísticas por cola
            queue_stats = {}
            for queue_type, queue_info in self._queues.items():
                config = self._queue_configs[queue_type]
                queue_stats[config["name"]] = {
                    "active_workers": queue_info["active_workers"],
                    "max_workers": config["max_workers"],
                    "min_workers": config["min_workers"],
                    "priority_range": config["priority_range"],
                    "pending_tasks": queue_info["pending_tasks"],
                    "processed_tasks": queue_info["processed_tasks"],
                    "last_activity": queue_info["last_activity"].isoformat() if queue_info["last_activity"] else None,
                    "workers": queue_info["workers"]
                }

            # Estadísticas de asignaciones exclusivas
            assignments_by_queue = {}
            async with self._assignment_lock:
                for task_id, queue_id in self._task_assignments.items():
                    if queue_id not in assignments_by_queue:
                        assignments_by_queue[queue_id] = []
                    assignments_by_queue[queue_id].append(task_id)

            return {
                "queue_status": "running" if self._running else "stopped",
                "system_type": "Dynamic Queue System - EXCLUSIVE ASSIGNMENT",
                "total_workers": len(self._workers),
                "total_queues": len(self._queues),
                "active_workers": self._stats["workers_active"],
                "active_queues": self._stats["queues_active"],
                "task_counts": status_counts,
                "priority_counts": priority_counts,
                "total_tasks": sum(status_counts.values()),
                "assigned_tasks": len(self._task_assignments),
                "assignments_by_queue": {str(k): len(v) for k, v in assignments_by_queue.items()},
                "uptime_seconds": uptime.total_seconds() if uptime else 0,
                "last_db_check": (
                    self._stats["last_db_check"].isoformat()
                    if self._stats["last_db_check"]
                    else None
                ),
                "queue_details": queue_stats,
                "dynamic_scaling_events": self._stats["dynamic_scaling_events"],
                "stats": self._stats,
            }

    async def get_queue_info(self, queue_type: QueueType) -> Optional[Dict[str, Any]]:
        """Obtener información detallada de una cola específica"""
        if queue_type not in self._queues:
            return None
            
        queue_info = self._queues[queue_type]
        config = self._queue_configs[queue_type]
        
        # Estadísticas de workers
        worker_stats = []
        for worker_id in queue_info["workers"]:
            if worker_id in self._workers:
                worker = self._workers[worker_id]
                worker_stats.append({
                    "worker_id": worker_id,
                    "tasks_processed": worker["tasks_processed"],
                    "created_at": worker["created_at"].isoformat(),
                    "last_activity": worker["last_activity"].isoformat()
                })
        
        return {
            "queue_name": config["name"],
            "queue_type": queue_type.name,
            "config": config,
            "stats": {
                "active_workers": queue_info["active_workers"],
                "pending_tasks": queue_info["pending_tasks"],
                "processed_tasks": queue_info["processed_tasks"],
                "last_activity": queue_info["last_activity"].isoformat() if queue_info["last_activity"] else None
            },
            "workers": worker_stats
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

    async def clear_all_tasks(self) -> int:
        """Limpiar TODAS las tareas de TODAS las colas sin importar su estado Y resetear estadísticas"""
        async with async_session_factory() as db:
            try:
                # Obtener el conteo total de tareas antes de eliminar
                count_result = await db.execute(select(func.count(Task.task_id)))
                total_tasks = count_result.scalar() or 0

                if total_tasks == 0:
                    print("🧹 No hay tareas para eliminar")
                    # Aún así, resetear estadísticas
                    self._reset_all_statistics()
                    return 0

                # Obtener todas las tareas y eliminarlas
                result = await db.execute(select(Task))
                all_tasks = result.scalars().all()
                
                for task in all_tasks:
                    await db.delete(task)

                await db.commit()
                print(f"🧹 TODAS las tareas eliminadas: {total_tasks}")
                
                # RESETEAR TODAS LAS ESTADÍSTICAS
                self._reset_all_statistics()
                print("📊 Estadísticas reseteadas completamente")
                    
                return total_tasks

            except Exception as e:
                await db.rollback()
                print(f"❌ Error limpiando todas las tareas: {e}")
                raise e

    def _reset_all_statistics(self):
        """Resetear todas las estadísticas del sistema"""
        # Resetear estadísticas globales
        self._stats = {
            "tasks_processed": 0,
            "tasks_failed": 0,
            "tasks_completed": 0,
            "workers_active": 0,
            "uptime_start": datetime.utcnow(),  # DATETIME OBJECT, NO STRING
            "last_db_check": datetime.utcnow().isoformat(),
            "queues_active": len(self._queues),
            "dynamic_scaling_events": 0,  # RESETEAR ESCALADOS
        }
        
        # Resetear estadísticas de cada cola
        for queue_key, queue_info in self._queues.items():
            queue_info["pending_tasks"] = 0
            queue_info["processed_tasks"] = 0  # RESETEAR TAREAS PROCESADAS
            queue_info["last_activity"] = datetime.utcnow()
        
        # Resetear estadísticas de workers
        for worker_id, worker_info in self._workers.items():
            worker_info["tasks_processed"] = 0
            worker_info["tasks_failed"] = 0
            worker_info["last_activity"] = datetime.utcnow()
        
        print("📊 Todas las estadísticas reseteadas a cero")

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


# Instancia global del sistema dinámico
dynamic_thread_queue_manager = DynamicThreadQueueManager()

# Alias para compatibilidad hacia atrás
optimized_thread_queue_manager = dynamic_thread_queue_manager
