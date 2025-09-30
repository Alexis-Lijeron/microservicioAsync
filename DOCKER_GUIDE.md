# 🚀 Sistema de Colas Dinámico - Guía Docker

## Instrucciones para ejecutar con Docker

### 1. Construir y ejecutar todos los servicios

```bash
# Construir e iniciar todos los servicios
docker-compose up --build

# Para ejecutar en segundo plano
docker-compose up -d --build
```

### 2. Acceder al Dashboard

Una vez que los servicios estén ejecutándose:

- **API Principal**: http://localhost:8000
- **Dashboard Dinámico**: http://localhost:8000/static/dashboard.html  
- **Dashboard Avanzado**: http://localhost:8000/static/dashboard_dynamic.html
- **Documentación API**: http://localhost:8000/docs
- **Redoc**: http://localhost:8000/redoc

### 3. Verificar el estado del sistema

```bash
# Ver logs de todos los servicios
docker-compose logs -f

# Ver logs específicos del servicio app
docker-compose logs -f app

# Ver logs del worker
docker-compose logs -f worker

# Verificar servicios activos
docker-compose ps
```

### 4. Comandos útiles

```bash
# Parar todos los servicios
docker-compose down

# Parar y eliminar volúmenes (limpieza completa)
docker-compose down -v

# Reconstruir un servicio específico
docker-compose build app

# Ejecutar comando dentro del contenedor
docker-compose exec app bash

# Ver estadísticas de containers
docker stats
```

### 5. Configuración de red

Los servicios están configurados con las siguientes URLs internas:

- **PostgreSQL**: `postgresql+asyncpg://postgres:password@db:5432/academic_db`
- **Redis**: `redis://redis:6379`
- **API**: Disponible en puerto 8000

### 6. Dashboard en Docker

El dashboard está completamente configurado para funcionar en Docker y incluye:

✅ **Monitoreo en tiempo real** de colas dinámicas  
✅ **Escalado dinámico** de workers  
✅ **Creación/eliminación** de colas en tiempo real  
✅ **Visualización de métricas** por prioridad  
✅ **Log de eventos** del sistema  
✅ **Control total** desde navegador  

### 7. Características del Sistema Dinámico

**Colas Predefinidas:**
- 🔴 **Critical** (Prioridad 1-2): Inscripciones
- 🟠 **High** (Prioridad 3): Estudiantes  
- 🟢 **Normal** (Prioridad 4-5): Docentes, Materias, Grupos
- ⚫ **Bulk** (Prioridad 6-10): Reportes, Procesos masivos

**Colas Dinámicas:**
- 🟣 **Custom** (Prioridad configurable): Creadas por demanda
- ⚡ **Auto-escalado** basado en carga de trabajo
- 🎯 **Eliminación automática** si no hay uso

### 8. API Endpoints para Control

```bash
# Ver todas las colas
curl -H "Authorization: Bearer TOKEN" http://localhost:8000/queue/queues/all

# Escalar workers de una cola
curl -X POST -H "Authorization: Bearer TOKEN" \
  "http://localhost:8000/queue/workers/scale-by-name?queue_name=Critical&target_workers=5"

# Crear nueva cola dinámica
curl -X POST -H "Authorization: Bearer TOKEN" \
  "http://localhost:8000/queue/queues/create?name=Reportes&priority_min=11&priority_max=15&max_workers=3"
```

### 9. Troubleshooting

**Si el dashboard no carga:**
1. Verificar que el servicio app esté corriendo: `docker-compose ps`
2. Revisar logs: `docker-compose logs app`
3. Comprobar puerto 8000: `curl http://localhost:8000/health`

**Si Redis no conecta:**
1. Verificar servicio Redis: `docker-compose ps redis`
2. Testear conexión: `docker-compose exec redis redis-cli ping`

**Si PostgreSQL falla:**
1. Verificar logs: `docker-compose logs db`
2. Limpiar volúmenes: `docker-compose down -v && docker-compose up -d`

### 10. Desarrollo

Para desarrollo con hot-reload:

```bash
# El volumen está montado, cambios se reflejan automáticamente
# Para reiniciar solo el servicio app:
docker-compose restart app
```

¡Tu sistema de colas dinámico está listo para funcionar en Docker! 🎉