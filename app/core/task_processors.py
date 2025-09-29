from typing import Dict, Any, Optional, Callable
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config.database import async_session_factory
from app.models.task import Task


def get_entity_code_from_task_data(task_data: Dict[str, Any], entity_type: str) -> Optional[str]:
    """
    Obtiene el código de entidad desde task_data, manejando tanto el formato 
    'codigo' como el formato específico de entidad (ej: 'codigo_aula')
    """
    # Mapping de entidad a su clave específica
    entity_key_mapping = {
        "aula": "codigo_aula",
        "carrera": "codigo",
        "docente": "codigo_docente", 
        "estudiante": "registro",
        "materia": "sigla",
        "grupo": "codigo_grupo",
        "horario": "codigo_horario",
        "gestion": "codigo_gestion",
        "nivel": "codigo_nivel",
        "inscripcion": "codigo_inscripcion",
        "nota": "codigo_nota",
        "plan_estudio": "codigo",
        "prerrequisito": "codigo_prerrequisito",
        "detalle": "codigo_detalle"
    }
    
    # Intentar obtener con la clave específica primero
    specific_key = entity_key_mapping.get(entity_type)
    if specific_key and specific_key in task_data:
        return task_data[specific_key]
    
    # Fallback a "codigo"
    if "codigo" in task_data:
        return task_data["codigo"]
    
    return None


class RollbackManager:
    """Maneja operaciones de rollback"""

    @staticmethod
    async def rollback_create_operation(table_name: str, record_code: str):
        """Rollback de operación CREATE - eliminar registro"""
        try:
            model_map = {
                "estudiantes": ("app.models.estudiante", "Estudiante", "registro"),
                "docentes": ("app.models.docente", "Docente", "codigo_docente"),
                "carreras": ("app.models.carrera", "Carrera", "codigo"),
                "materias": ("app.models.materia", "Materia", "sigla"),
                "grupos": ("app.models.grupo", "Grupo", "codigo_grupo"),
                "inscripciones": ("app.models.inscripcion", "Inscripcion", "codigo_inscripcion"),
                "notas": ("app.models.nota", "Nota", "codigo_nota"),
                "aulas": ("app.models.aula", "Aula", "codigo_aula"),
                "horarios": ("app.models.horario", "Horario", "codigo_horario"),
                "gestiones": ("app.models.gestion", "Gestion", "codigo_gestion"),
                "niveles": ("app.models.nivel", "Nivel", "codigo_nivel"),
                "planes_estudio": ("app.models.plan_estudio", "PlanEstudio", "codigo"),
                "prerrequisitos": ("app.models.prerrequisito", "Prerrequisito", "codigo_prerrequisito"),
                "detalles": ("app.models.detalle", "Detalle", "codigo_detalle"),
            }

            if table_name not in model_map:
                raise ValueError(f"Tabla no soportada: {table_name}")

            module_path, class_name, pk_field = model_map[table_name]
            module = __import__(module_path, fromlist=[class_name])
            model_class = getattr(module, class_name)

            async with async_session_factory() as db:
                pk_column = getattr(model_class, pk_field)
                result = await db.execute(
                    select(model_class).where(pk_column == record_code)
                )
                record = result.scalar_one_or_none()

                if record:
                    await db.delete(record)
                    await db.commit()
                    print(f"🔙 Rollback completado: Eliminado {table_name}.{pk_field}={record_code}")
                    return True
                else:
                    print(f"⚠️ Registro no encontrado: {table_name}.{pk_field}={record_code}")
                    return True

        except Exception as e:
            print(f"❌ Error en rollback: {e}")
            return False

    @staticmethod
    async def rollback_update_operation(
        table_name: str, record_code: str, original_data: Dict[str, Any]
    ):
        """Rollback de operación UPDATE - restaurar datos originales"""
        try:
            model_map = {
                "estudiantes": ("app.models.estudiante", "Estudiante", "registro"),
                "docentes": ("app.models.docente", "Docente", "codigo_docente"),
                "carreras": ("app.models.carrera", "Carrera", "codigo"),
                "materias": ("app.models.materia", "Materia", "sigla"),
                "grupos": ("app.models.grupo", "Grupo", "codigo_grupo"),
                "inscripciones": ("app.models.inscripcion", "Inscripcion", "codigo_inscripcion"),
                "notas": ("app.models.nota", "Nota", "codigo_nota"),
                "aulas": ("app.models.aula", "Aula", "codigo_aula"),
                "horarios": ("app.models.horario", "Horario", "codigo_horario"),
                "gestiones": ("app.models.gestion", "Gestion", "codigo_gestion"),
                "niveles": ("app.models.nivel", "Nivel", "codigo_nivel"),
                "planes_estudio": ("app.models.plan_estudio", "PlanEstudio", "codigo"),
                "prerrequisitos": ("app.models.prerrequisito", "Prerrequisito", "codigo_prerrequisito"),
                "detalles": ("app.models.detalle", "Detalle", "codigo_detalle"),
            }

            if table_name not in model_map:
                raise ValueError(f"Tabla no soportada: {table_name}")

            module_path, class_name, pk_field = model_map[table_name]
            module = __import__(module_path, fromlist=[class_name])
            model_class = getattr(module, class_name)

            async with async_session_factory() as db:
                pk_column = getattr(model_class, pk_field)
                result = await db.execute(
                    select(model_class).where(pk_column == record_code)
                )
                record = result.scalar_one_or_none()

                if record:
                    for field, value in original_data.items():
                        if hasattr(record, field):
                            setattr(record, field, value)

                    await db.commit()
                    print(f"🔙 Rollback completado: Restaurado {table_name}.{pk_field}={record_code}")
                    return True
                else:
                    print(f"⚠️ Registro no encontrado: {table_name}.{pk_field}={record_code}")
                    return False

        except Exception as e:
            print(f"❌ Error en rollback: {e}")
            return False


# ================================
# ESTUDIANTE PROCESSORS
# ================================
async def process_create_estudiante_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de estudiante"""
    try:
        from app.crud.estudiante import estudiante
        from app.schemas.estudiante import EstudianteCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            estudiante_data = EstudianteCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_estudiante = await estudiante.create(db, obj_in=estudiante_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "estudiantes",
                "record_code": new_estudiante.registro,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "estudiante_registro": new_estudiante.registro,
                "message": f"Estudiante {new_estudiante.registro} creado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_estudiante_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de estudiante"""
    try:
        from app.crud.estudiante import estudiante
        from app.schemas.estudiante import EstudianteUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            registro = task_data.pop("registro")
            update_data = EstudianteUpdate(**task_data)
            
            # Obtener datos originales para rollback
            original_estudiante = await estudiante.get(db, code=registro)
            if not original_estudiante:
                return {"success": False, "error": "Estudiante no encontrado"}

            original_data = {
                "nombre": original_estudiante.nombre,
                "apellido": original_estudiante.apellido,
                "ci": original_estudiante.ci,
                "carrera_codigo": original_estudiante.carrera_codigo,
            }

            task.progress = 50.0
            await db.commit()

            updated_estudiante = await estudiante.update(db, db_obj=original_estudiante, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "estudiantes",
                "record_code": registro,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "estudiante_registro": updated_estudiante.registro,
                "message": f"Estudiante {updated_estudiante.registro} actualizado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_estudiante_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de estudiante"""
    try:
        from app.crud.estudiante import estudiante

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            registro = task_data["registro"]
            deleted_estudiante = await estudiante.remove(db, code=registro)

            if not deleted_estudiante:
                return {"success": False, "error": "Estudiante no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Estudiante {registro} eliminado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_estudiante_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de estudiante"""
    try:
        from app.crud.estudiante import estudiante

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            registro = task_data["registro"]
            estudiante_obj = await estudiante.get(db, code=registro)

            if not estudiante_obj:
                return {"success": False, "error": "Estudiante no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "registro": estudiante_obj.registro,
                    "nombre": estudiante_obj.nombre,
                    "apellido": estudiante_obj.apellido,
                    "ci": estudiante_obj.ci,
                    "carrera_codigo": estudiante_obj.carrera_codigo,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_estudiantes_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de estudiantes"""
    try:
        from app.crud.estudiante import estudiante

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            estudiantes_list = await estudiante.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            estudiantes_data = [
                {
                    "registro": e.registro,
                    "nombre": e.nombre,
                    "apellido": e.apellido,
                    "ci": e.ci,
                    "carrera_codigo": e.carrera_codigo,
                }
                for e in estudiantes_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": estudiantes_data,
                "total": len(estudiantes_data),
                "skip": skip,
                "limit": limit,
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_carreras_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de carreras"""
    try:
        from app.crud.carrera import carrera

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            carreras_list = await carrera.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            carreras_data = [
                {
                    "codigo": c.codigo,
                    "nombre": c.nombre,
                }
                for c in carreras_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": carreras_data,
                "total": len(carreras_data),
                "skip": skip,
                "limit": limit,
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# CARRERA PROCESSORS
# ================================
async def process_create_carrera_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de carrera"""
    try:
        from app.crud.carrera import carrera
        from app.schemas.carrera import CarreraCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            carrera_data = CarreraCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_carrera = await carrera.create(db, obj_in=carrera_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "carreras",
                "record_code": new_carrera.codigo,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "carrera_codigo": new_carrera.codigo,
                "message": f"Carrera {new_carrera.codigo} creada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_carrera_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de carrera"""
    try:
        from app.crud.carrera import carrera
        from app.schemas.carrera import CarreraUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo = get_entity_code_from_task_data(task_data, "carrera")
            if not codigo:
                return {"success": False, "error": "Código de carrera no encontrado en task_data"}
            update_data_dict = task_data["update_data"]
            update_data = CarreraUpdate(**update_data_dict)
            
            # Obtener datos originales para rollback
            original_carrera = await carrera.get(db, code=codigo)
            if not original_carrera:
                return {"success": False, "error": "Carrera no encontrada"}

            original_data = {
                "nombre": original_carrera.nombre,
            }

            task.progress = 50.0
            await db.commit()

            updated_carrera = await carrera.update(db, db_obj=original_carrera, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "carreras",
                "record_code": codigo,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "carrera_codigo": updated_carrera.codigo,
                "message": f"Carrera {updated_carrera.codigo} actualizada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_carrera_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de carrera"""
    try:
        from app.crud.carrera import carrera

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo = task_data["codigo"]
            deleted_carrera = await carrera.remove(db, code=codigo)

            if not deleted_carrera:
                return {"success": False, "error": "Carrera no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Carrera {codigo} eliminada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_carrera_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de carrera"""
    try:
        from app.crud.carrera import carrera

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo = task_data["codigo"]
            carrera_obj = await carrera.get(db, code=codigo)

            if not carrera_obj:
                return {"success": False, "error": "Carrera no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo": carrera_obj.codigo,
                    "nombre": carrera_obj.nombre,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# DOCENTE PROCESSORS
# ================================
async def process_create_docente_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de docente"""
    try:
        from app.crud.docente import docente
        from app.schemas.docente import DocenteCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            docente_data = DocenteCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_docente = await docente.create(db, obj_in=docente_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "docentes",
                "record_code": new_docente.codigo_docente,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "docente_codigo": new_docente.codigo_docente,
                "message": f"Docente {new_docente.codigo_docente} creado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_docente_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de docente"""
    try:
        from app.crud.docente import docente
        from app.schemas.docente import DocenteUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_docente = task_data["codigo"]
            update_data_dict = task_data["update_data"]
            update_data = DocenteUpdate(**update_data_dict)
            
            # Obtener datos originales para rollback
            original_docente = await docente.get(db, code=codigo_docente)
            if not original_docente:
                return {"success": False, "error": "Docente no encontrado"}

            original_data = {
                "nombre": original_docente.nombre,
                "apellido": original_docente.apellido,
            }

            task.progress = 50.0
            await db.commit()

            updated_docente = await docente.update(db, db_obj=original_docente, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "docentes",
                "record_code": codigo_docente,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "docente_codigo": updated_docente.codigo_docente,
                "message": f"Docente {updated_docente.codigo_docente} actualizado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_docente_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de docente"""
    try:
        from app.crud.docente import docente

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_docente = task_data["codigo"]
            deleted_docente = await docente.remove(db, code=codigo_docente)

            if not deleted_docente:
                return {"success": False, "error": "Docente no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Docente {codigo_docente} eliminado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_docente_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de docente"""
    try:
        from app.crud.docente import docente

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo_docente = task_data["codigo"]
            docente_obj = await docente.get(db, code=codigo_docente)

            if not docente_obj:
                return {"success": False, "error": "Docente no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo_docente": docente_obj.codigo_docente,
                    "nombre": docente_obj.nombre,
                    "apellido": docente_obj.apellido,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# MATERIA PROCESSORS
# ================================
async def process_create_materia_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de materia"""
    try:
        from app.crud.materia import materia
        from app.schemas.materia import MateriaCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            materia_data = MateriaCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_materia = await materia.create(db, obj_in=materia_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "materias",
                "record_code": new_materia.sigla,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "materia_sigla": new_materia.sigla,
                "message": f"Materia {new_materia.sigla} creada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_materia_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de materia"""
    try:
        from app.crud.materia import materia
        from app.schemas.materia import MateriaUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            sigla = task_data.pop("sigla")
            update_data = MateriaUpdate(**task_data)
            
            # Obtener datos originales para rollback
            original_materia = await materia.get(db, code=sigla)
            if not original_materia:
                return {"success": False, "error": "Materia no encontrada"}

            original_data = {
                "nombre": original_materia.nombre,
                "nivel_codigo": original_materia.nivel_codigo,
            }

            task.progress = 50.0
            await db.commit()

            updated_materia = await materia.update(db, db_obj=original_materia, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "materias",
                "record_code": sigla,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "materia_sigla": updated_materia.sigla,
                "message": f"Materia {updated_materia.sigla} actualizada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_materia_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de materia"""
    try:
        from app.crud.materia import materia

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            sigla = task_data["sigla"]
            deleted_materia = await materia.remove(db, code=sigla)

            if not deleted_materia:
                return {"success": False, "error": "Materia no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Materia {sigla} eliminada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_materia_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de materia"""
    try:
        from app.crud.materia import materia

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            sigla = task_data["sigla"]
            materia_obj = await materia.get(db, code=sigla)

            if not materia_obj:
                return {"success": False, "error": "Materia no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "sigla": materia_obj.sigla,
                    "nombre": materia_obj.nombre,
                    "creditos": materia_obj.creditos,
                    "es_electiva": materia_obj.es_electiva,
                    "nivel_codigo": materia_obj.nivel_codigo,
                    "plan_estudio_codigo": materia_obj.plan_estudio_codigo,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_materias_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de materias"""
    try:
        from app.crud.materia import materia

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            materias_list = await materia.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            materias_data = [
                {
                    "sigla": m.sigla,
                    "nombre": m.nombre,
                    "creditos": m.creditos,
                    "es_electiva": m.es_electiva,
                    "nivel_codigo": m.nivel_codigo,
                    "plan_estudio_codigo": m.plan_estudio_codigo,
                }
                for m in materias_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": materias_data,
                "total": len(materias_data),
                "skip": skip,
                "limit": limit,
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_docentes_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de docentes"""
    try:
        from app.crud.docente import docente

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            docentes_list = await docente.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            docentes_data = [
                {
                    "codigo_docente": e.codigo_docente,
                    "nombre": e.nombre,
                    "apellido": e.apellido,
                }
                for e in docentes_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": docentes_data,
                "total": len(docentes_data),
                "skip": skip,
                "limit": limit,
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_grupos_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de grupos"""
    try:
        from app.crud.grupo import grupo

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            grupos_list = await grupo.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            grupos_data = [
                {
                    "codigo_grupo": e.codigo_grupo,
                    "descripcion": e.descripcion,
                    "materia_sigla": e.materia_sigla,
                    "docente_codigo": e.docente_codigo,
                    "gestion_codigo": e.gestion_codigo,
                    "horario_codigo": e.horario_codigo,
                }
                for e in grupos_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": grupos_data,
                "total": len(grupos_data),
                "skip": skip,
                "limit": limit,
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_inscripciones_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de inscripciones"""
    try:
        from app.crud.inscripcion import inscripcion

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            inscripciones_list = await inscripcion.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            inscripciones_data = [
                {
                    "codigo_inscripcion": e.codigo_inscripcion,
                    "semestre": e.semestre,
                    "estudiante_registro": e.estudiante_registro,
                    "grupo_codigo": e.grupo_codigo,
                    "gestion_codigo": e.gestion_codigo,
                }
                for e in inscripciones_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": inscripciones_data,
                "total": len(inscripciones_data),
                "skip": skip,
                "limit": limit,
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_notas_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de notas"""
    try:
        from app.crud.nota import nota

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            notas_list = await nota.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            notas_data = [
                {
                    "codigo_nota": e.codigo_nota,
                    "nota": e.nota,
                    "estudiante_registro": e.estudiante_registro,
                }
                for e in notas_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": notas_data,
                "total": len(notas_data),
                "skip": skip,
                "limit": limit,
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# GRUPO PROCESSORS
# ================================
async def process_create_grupo_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de grupo"""
    try:
        from app.crud.grupo import grupo
        from app.schemas.grupo import GrupoCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            grupo_data = GrupoCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_grupo = await grupo.create(db, obj_in=grupo_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "grupos",
                "record_code": new_grupo.codigo_grupo,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "grupo_codigo": new_grupo.codigo_grupo,
                "message": f"Grupo {new_grupo.codigo_grupo} creado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_grupo_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de grupo"""
    try:
        from app.crud.grupo import grupo
        from app.schemas.grupo import GrupoUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_grupo = task_data["codigo"]
            update_data_dict = task_data["update_data"]
            update_data = GrupoUpdate(**update_data_dict)
            
            # Obtener datos originales para rollback
            original_grupo = await grupo.get(db, code=codigo_grupo)
            if not original_grupo:
                return {"success": False, "error": "Grupo no encontrado"}

            original_data = {
                "descripcion": original_grupo.descripcion,
                "materia_sigla": original_grupo.materia_sigla,
                "docente_codigo": original_grupo.docente_codigo,
                "gestion_codigo": original_grupo.gestion_codigo,
                "horario_codigo": original_grupo.horario_codigo,
            }

            task.progress = 50.0
            await db.commit()

            updated_grupo = await grupo.update(db, db_obj=original_grupo, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "grupos",
                "record_code": codigo_grupo,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "grupo_codigo": updated_grupo.codigo_grupo,
                "message": f"Grupo {updated_grupo.codigo_grupo} actualizado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_grupo_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de grupo"""
    try:
        from app.crud.grupo import grupo

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_grupo = task_data["codigo"]
            deleted_grupo = await grupo.remove(db, code=codigo_grupo)

            if not deleted_grupo:
                return {"success": False, "error": "Grupo no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Grupo {codigo_grupo} eliminado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_grupo_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de grupo"""
    try:
        from app.crud.grupo import grupo

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo_grupo = task_data["codigo_grupo"]
            grupo_obj = await grupo.get(db, code=codigo_grupo)

            if not grupo_obj:
                return {"success": False, "error": "Grupo no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo_grupo": grupo_obj.codigo_grupo,
                    "descripcion": grupo_obj.descripcion,
                    "materia_sigla": grupo_obj.materia_sigla,
                    "docente_codigo": grupo_obj.docente_codigo,
                    "gestion_codigo": grupo_obj.gestion_codigo,
                    "horario_codigo": grupo_obj.horario_codigo,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# INSCRIPCION PROCESSORS
# ================================
async def process_create_inscripcion_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de inscripción"""
    try:
        from app.crud.inscripcion import inscripcion
        from app.schemas.inscripcion import InscripcionCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            inscripcion_data = InscripcionCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_inscripcion = await inscripcion.create(db, obj_in=inscripcion_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "inscripciones",
                "record_code": new_inscripcion.codigo_inscripcion,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "inscripcion_codigo": new_inscripcion.codigo_inscripcion,
                "message": f"Inscripción {new_inscripcion.codigo_inscripcion} creada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_inscripcion_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de inscripción"""
    try:
        from app.crud.inscripcion import inscripcion
        from app.schemas.inscripcion import InscripcionUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_inscripcion = task_data["codigo_inscripcion"]
            update_data_dict = task_data["update_data"]
            update_data = InscripcionUpdate(**update_data_dict)
            
            # Obtener datos originales para rollback
            original_inscripcion = await inscripcion.get(db, code=codigo_inscripcion)
            if not original_inscripcion:
                return {"success": False, "error": "Inscripción no encontrada"}

            original_data = {
                "semestre": original_inscripcion.semestre,
                "gestion_codigo": original_inscripcion.gestion_codigo,
                "estudiante_registro": original_inscripcion.estudiante_registro,
                "grupo_codigo": original_inscripcion.grupo_codigo,
            }

            task.progress = 50.0
            await db.commit()

            updated_inscripcion = await inscripcion.update(db, db_obj=original_inscripcion, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "inscripciones",
                "record_code": codigo_inscripcion,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "inscripcion_codigo": updated_inscripcion.codigo_inscripcion,
                "message": f"Inscripción {updated_inscripcion.codigo_inscripcion} actualizada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_inscripcion_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de inscripción"""
    try:
        from app.crud.inscripcion import inscripcion

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_inscripcion = task_data["codigo_inscripcion"]
            deleted_inscripcion = await inscripcion.remove(db, code=codigo_inscripcion)

            if not deleted_inscripcion:
                return {"success": False, "error": "Inscripción no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Inscripción {codigo_inscripcion} eliminada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_inscripcion_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de inscripción"""
    try:
        from app.crud.inscripcion import inscripcion

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo_inscripcion = task_data["codigo_inscripcion"]
            inscripcion_obj = await inscripcion.get(db, code=codigo_inscripcion)

            if not inscripcion_obj:
                return {"success": False, "error": "Inscripción no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo_inscripcion": inscripcion_obj.codigo_inscripcion,
                    "semestre": inscripcion_obj.semestre,
                    "gestion_codigo": inscripcion_obj.gestion_codigo,
                    "estudiante_registro": inscripcion_obj.estudiante_registro,
                    "grupo_codigo": inscripcion_obj.grupo_codigo,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# NOTA PROCESSORS
# ================================
async def process_create_nota_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de nota"""
    try:
        from app.crud.nota import nota
        from app.schemas.nota import NotaCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            nota_data = NotaCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_nota = await nota.create(db, obj_in=nota_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "notas",
                "record_code": new_nota.codigo_nota,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "nota_codigo": new_nota.codigo_nota,
                "message": f"Nota {new_nota.codigo_nota} creada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_nota_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de nota"""
    try:
        from app.crud.nota import nota
        from app.schemas.nota import NotaUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_nota = get_entity_code_from_task_data(task_data, "nota")
            if not codigo_nota:
                return {"success": False, "error": "Código de nota no encontrado en task_data"}
            
            update_data_dict = task_data["update_data"]
            update_data = NotaUpdate(**update_data_dict)
            
            # Obtener datos originales para rollback
            original_nota = await nota.get(db, code=codigo_nota)
            if not original_nota:
                return {"success": False, "error": "Nota no encontrada"}

            original_data = {
                "nota": float(original_nota.nota) if original_nota.nota else None,
                "estudiante_registro": original_nota.estudiante_registro,
            }

            task.progress = 50.0
            await db.commit()

            updated_nota = await nota.update(db, db_obj=original_nota, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "notas",
                "record_code": codigo_nota,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "nota_codigo": updated_nota.codigo_nota,
                "message": f"Nota {updated_nota.codigo_nota} actualizada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_nota_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de nota"""
    try:
        from app.crud.nota import nota

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_nota = get_entity_code_from_task_data(task_data, "nota")
            if not codigo_nota:
                return {"success": False, "error": "Código de nota no encontrado en task_data"}
            deleted_nota = await nota.remove(db, code=codigo_nota)

            if not deleted_nota:
                return {"success": False, "error": "Nota no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Nota {codigo_nota} eliminada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_nota_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de nota"""
    try:
        from app.crud.nota import nota

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo_nota = task_data["codigo_nota"]
            nota_obj = await nota.get(db, code=codigo_nota)

            if not nota_obj:
                return {"success": False, "error": "Nota no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo_nota": nota_obj.codigo_nota,
                    "nota": float(nota_obj.nota) if nota_obj.nota else None,
                    "estudiante_registro": nota_obj.estudiante_registro,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# AULA PROCESSORS
# ================================
async def process_create_aula_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de aula"""
    try:
        from app.crud.aula import aula
        from app.schemas.aula import AulaCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            aula_data = AulaCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_aula = await aula.create(db, obj_in=aula_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "aulas",
                "record_code": new_aula.codigo_aula,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "aula_codigo": new_aula.codigo_aula,
                "message": f"Aula {new_aula.codigo_aula} creada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_aula_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de aula"""
    try:
        from app.crud.aula import aula
        from app.schemas.aula import AulaUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            # Handle both "codigo" and "codigo_aula" keys for backward compatibility
            codigo_aula = task_data.get("codigo") or task_data.get("codigo_aula")
            if not codigo_aula:
                return {"success": False, "error": "Código de aula no encontrado en task_data"}
            update_data_dict = task_data["update_data"]
            update_data = AulaUpdate(**update_data_dict)
            
            # Obtener datos originales para rollback
            original_aula = await aula.get(db, code=codigo_aula)
            if not original_aula:
                return {"success": False, "error": "Aula no encontrada"}

            original_data = {
                "modulo": original_aula.modulo,
                "aula": original_aula.aula,
            }

            task.progress = 50.0
            await db.commit()

            updated_aula = await aula.update(db, db_obj=original_aula, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "aulas",
                "record_code": codigo_aula,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "aula_codigo": updated_aula.codigo_aula,
                "message": f"Aula {updated_aula.codigo_aula} actualizada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_aula_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de aula"""
    try:
        from app.crud.aula import aula

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            # Handle both "codigo" and "codigo_aula" keys for backward compatibility
            codigo_aula = task_data.get("codigo") or task_data.get("codigo_aula")
            if not codigo_aula:
                return {"success": False, "error": "Código de aula no encontrado en task_data"}
            deleted_aula = await aula.remove(db, code=codigo_aula)

            if not deleted_aula:
                return {"success": False, "error": "Aula no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Aula {codigo_aula} eliminada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_aulas_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de aulas"""
    try:
        from app.crud.aula import aula

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            aulas_list = await aula.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            aulas_data = [
                {
                    "codigo_aula": a.codigo_aula,
                    "modulo": a.modulo,
                    "aula": a.aula,
                }
                for a in aulas_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": aulas_data,
                "total": len(aulas_data)
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_aula_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de aula"""
    try:
        from app.crud.aula import aula

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo_aula = task_data["codigo_aula"]
            aula_obj = await aula.get(db, code=codigo_aula)

            if not aula_obj:
                return {"success": False, "error": "Aula no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo_aula": aula_obj.codigo_aula,
                    "modulo": aula_obj.modulo,
                    "aula": aula_obj.aula,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# HORARIO PROCESSORS
# ================================
async def process_create_horario_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de horario"""
    try:
        from app.crud.horario import horario
        from app.schemas.horario import HorarioCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            # Convert time strings back to time objects if needed
            from datetime import time
            print(f"DEBUG: task_data before conversion: {task_data}")
            
            # Create a copy to avoid modifying the original
            processed_data = task_data.copy()
            
            if 'hora_inicio' in processed_data and isinstance(processed_data['hora_inicio'], str):
                hora_inicio_str = processed_data['hora_inicio']
                print(f"DEBUG: Converting hora_inicio: {hora_inicio_str}")
                # Handle both HH:MM:SS and HH:MM:SS.sss formats
                if '.' in hora_inicio_str:
                    hora_inicio_str = hora_inicio_str.split('.')[0]  # Remove milliseconds
                hora_inicio_parts = hora_inicio_str.split(':')
                processed_data['hora_inicio'] = time(int(hora_inicio_parts[0]), int(hora_inicio_parts[1]), int(hora_inicio_parts[2]) if len(hora_inicio_parts) > 2 else 0)
                print(f"DEBUG: Converted hora_inicio to: {processed_data['hora_inicio']}")
                
            if 'hora_final' in processed_data and isinstance(processed_data['hora_final'], str):
                hora_final_str = processed_data['hora_final']
                print(f"DEBUG: Converting hora_final: {hora_final_str}")
                # Handle both HH:MM:SS and HH:MM:SS.sss formats
                if '.' in hora_final_str:
                    hora_final_str = hora_final_str.split('.')[0]  # Remove milliseconds
                hora_final_parts = hora_final_str.split(':')
                processed_data['hora_final'] = time(int(hora_final_parts[0]), int(hora_final_parts[1]), int(hora_final_parts[2]) if len(hora_final_parts) > 2 else 0)
                print(f"DEBUG: Converted hora_final to: {processed_data['hora_final']}")
            
            print(f"DEBUG: processed_data after conversion: {processed_data}")

            # Create schema object with converted data
            horario_data = HorarioCreate(**processed_data)
            print(f"DEBUG: horario_data object: {horario_data}")
            print(f"DEBUG: horario_data.hora_inicio type: {type(horario_data.hora_inicio)}")
            print(f"DEBUG: horario_data.hora_final type: {type(horario_data.hora_final)}")
            
            task.progress = 50.0
            await db.commit()

            new_horario = await horario.create(db, obj_in=horario_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "horarios",
                "record_code": new_horario.codigo_horario,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "horario_codigo": new_horario.codigo_horario,
                "message": f"Horario {new_horario.codigo_horario} creado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_horario_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de horario"""
    try:
        from app.crud.horario import horario
        from app.schemas.horario import HorarioUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            # Handle both "codigo" and "codigo_horario" keys for backward compatibility
            codigo_horario = task_data.get("codigo") or task_data.get("codigo_horario")
            if not codigo_horario:
                return {"success": False, "error": "Código de horario no encontrado en task_data"}
            
            update_data_dict = task_data["update_data"]
            
            # Convert time strings back to time objects if needed
            from datetime import time
            if 'hora_inicio' in update_data_dict and isinstance(update_data_dict['hora_inicio'], str):
                hora_inicio_str = update_data_dict['hora_inicio']
                # Handle both HH:MM:SS and HH:MM:SS.sss formats
                if '.' in hora_inicio_str:
                    hora_inicio_str = hora_inicio_str.split('.')[0]  # Remove milliseconds
                hora_inicio_parts = hora_inicio_str.split(':')
                update_data_dict['hora_inicio'] = time(int(hora_inicio_parts[0]), int(hora_inicio_parts[1]), int(hora_inicio_parts[2]) if len(hora_inicio_parts) > 2 else 0)
                
            if 'hora_final' in update_data_dict and isinstance(update_data_dict['hora_final'], str):
                hora_final_str = update_data_dict['hora_final']
                # Handle both HH:MM:SS and HH:MM:SS.sss formats
                if '.' in hora_final_str:
                    hora_final_str = hora_final_str.split('.')[0]  # Remove milliseconds
                hora_final_parts = hora_final_str.split(':')
                update_data_dict['hora_final'] = time(int(hora_final_parts[0]), int(hora_final_parts[1]), int(hora_final_parts[2]) if len(hora_final_parts) > 2 else 0)
            
            update_data = HorarioUpdate(**update_data_dict)
            
            # Obtener datos originales para rollback
            original_horario = await horario.get(db, code=codigo_horario)
            if not original_horario:
                return {"success": False, "error": "Horario no encontrado"}

            original_data = {
                "dia": original_horario.dia,
                "hora_inicio": original_horario.hora_inicio.strftime("%H:%M:%S") if original_horario.hora_inicio else None,
                "hora_final": original_horario.hora_final.strftime("%H:%M:%S") if original_horario.hora_final else None,
                "aula_codigo": original_horario.aula_codigo,
            }

            task.progress = 50.0
            await db.commit()

            updated_horario = await horario.update(db, db_obj=original_horario, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "horarios",
                "record_code": codigo_horario,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "horario_codigo": updated_horario.codigo_horario,
                "message": f"Horario {updated_horario.codigo_horario} actualizado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_horario_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de horario"""
    try:
        from app.crud.horario import horario

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            # Handle both "codigo" and "codigo_horario" keys for backward compatibility
            codigo_horario = task_data.get("codigo") or task_data.get("codigo_horario")
            if not codigo_horario:
                return {"success": False, "error": "Código de horario no encontrado en task_data"}
            deleted_horario = await horario.remove(db, code=codigo_horario)

            if not deleted_horario:
                return {"success": False, "error": "Horario no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Horario {codigo_horario} eliminado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_horarios_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de horarios"""
    try:
        from app.crud.horario import horario

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            horarios_list = await horario.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

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

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": horarios_data,
                "total": len(horarios_data)
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_horario_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de horario"""
    try:
        from app.crud.horario import horario

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo_horario = task_data["codigo_horario"]
            horario_obj = await horario.get(db, code=codigo_horario)

            if not horario_obj:
                return {"success": False, "error": "Horario no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo_horario": horario_obj.codigo_horario,
                    "dia": horario_obj.dia,
                    "hora_inicio": horario_obj.hora_inicio.isoformat() if horario_obj.hora_inicio else None,
                    "hora_final": horario_obj.hora_final.isoformat() if horario_obj.hora_final else None,
                    "aula_codigo": horario_obj.aula_codigo,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# GESTION PROCESSORS
# ================================
async def process_create_gestion_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de gestión"""
    try:
        from app.crud.gestion import gestion
        from app.schemas.gestion import GestionCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            gestion_data = GestionCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_gestion = await gestion.create(db, obj_in=gestion_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "gestiones",
                "record_code": new_gestion.codigo_gestion,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "gestion_codigo": new_gestion.codigo_gestion,
                "message": f"Gestión {new_gestion.codigo_gestion} creada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_gestion_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de gestión"""
    try:
        from app.crud.gestion import gestion
        from app.schemas.gestion import GestionUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_gestion = get_entity_code_from_task_data(task_data, "gestion")
            if not codigo_gestion:
                return {"success": False, "error": "Código de gestión no encontrado en task_data"}
            update_data_dict = task_data["update_data"]
            update_data = GestionUpdate(**update_data_dict)
            
            # Obtener datos originales para rollback
            original_gestion = await gestion.get(db, code=codigo_gestion)
            if not original_gestion:
                return {"success": False, "error": "Gestión no encontrada"}

            original_data = {
                "año": original_gestion.año,
                "semestre": original_gestion.semestre,
            }

            task.progress = 50.0
            await db.commit()

            updated_gestion = await gestion.update(db, db_obj=original_gestion, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "gestiones",
                "record_code": codigo_gestion,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "gestion_codigo": updated_gestion.codigo_gestion,
                "message": f"Gestión {updated_gestion.codigo_gestion} actualizada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_gestion_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de gestión"""
    try:
        from app.crud.gestion import gestion

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_gestion = get_entity_code_from_task_data(task_data, "gestion")
            if not codigo_gestion:
                return {"success": False, "error": "Código de gestión no encontrado en task_data"}
            deleted_gestion = await gestion.remove(db, code=codigo_gestion)

            if not deleted_gestion:
                return {"success": False, "error": "Gestión no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Gestión {codigo_gestion} eliminada",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_gestiones_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de gestiones"""
    try:
        from app.crud.gestion import gestion

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            gestiones_list = await gestion.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            gestiones_data = [
                {
                    "codigo_gestion": g.codigo_gestion,
                    "semestre": g.semestre,
                    "año": g.año,
                }
                for g in gestiones_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": gestiones_data,
                "total": len(gestiones_data)
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_gestion_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de gestión"""
    try:
        from app.crud.gestion import gestion

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo_gestion = task_data["codigo_gestion"]
            gestion_obj = await gestion.get(db, code=codigo_gestion)

            if not gestion_obj:
                return {"success": False, "error": "Gestión no encontrada"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo_gestion": gestion_obj.codigo_gestion,
                    "semestre": gestion_obj.semestre,
                    "año": gestion_obj.año,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# NIVEL PROCESSORS
# ================================
async def process_create_nivel_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de nivel"""
    try:
        from app.crud.nivel import nivel
        from app.schemas.nivel import NivelCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            nivel_data = NivelCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_nivel = await nivel.create(db, obj_in=nivel_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "niveles",
                "record_code": new_nivel.codigo_nivel,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "nivel_codigo": new_nivel.codigo_nivel,
                "message": f"Nivel {new_nivel.codigo_nivel} creado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_nivel_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de nivel"""
    try:
        from app.crud.nivel import nivel
        from app.schemas.nivel import NivelUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_nivel = get_entity_code_from_task_data(task_data, "nivel")
            if not codigo_nivel:
                return {"success": False, "error": "Código de nivel no encontrado en task_data"}
            
            # Handle different task data structures
            if "update_data" in task_data:
                update_data_dict = task_data["update_data"]
            else:
                # Create update data from top-level fields, excluding the codigo
                update_data_dict = {k: v for k, v in task_data.items() if k != "codigo_nivel" and k != "codigo"}
            
            update_data = NivelUpdate(**update_data_dict)
            
            # Obtener datos originales para rollback
            original_nivel = await nivel.get(db, code=codigo_nivel)
            if not original_nivel:
                return {"success": False, "error": "Nivel no encontrado"}

            original_data = {
                "nivel": original_nivel.nivel,
            }

            task.progress = 50.0
            await db.commit()

            updated_nivel = await nivel.update(db, db_obj=original_nivel, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "niveles",
                "record_code": codigo_nivel,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "nivel_codigo": updated_nivel.codigo_nivel,
                "message": f"Nivel {updated_nivel.codigo_nivel} actualizado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_nivel_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de nivel"""
    try:
        from app.crud.nivel import nivel

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_nivel = get_entity_code_from_task_data(task_data, "nivel")
            if not codigo_nivel:
                return {"success": False, "error": "Código de nivel no encontrado en task_data"}
            deleted_nivel = await nivel.remove(db, code=codigo_nivel)

            if not deleted_nivel:
                return {"success": False, "error": "Nivel no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Nivel {codigo_nivel} eliminado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_niveles_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de niveles"""
    try:
        from app.crud.nivel import nivel

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            niveles_list = await nivel.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            niveles_data = [
                {
                    "codigo_nivel": n.codigo_nivel,
                    "nivel": n.nivel,
                }
                for n in niveles_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": niveles_data,
                "total": len(niveles_data)
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_nivel_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de nivel"""
    try:
        from app.crud.nivel import nivel

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo_nivel = task_data["codigo_nivel"]
            nivel_obj = await nivel.get(db, code=codigo_nivel)

            if not nivel_obj:
                return {"success": False, "error": "Nivel no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo_nivel": nivel_obj.codigo_nivel,
                    "nivel": nivel_obj.nivel,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# PLAN ESTUDIO PROCESSORS
# ================================
async def process_create_plan_estudio_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de plan de estudio"""
    try:
        from app.crud.plan_estudio import plan_estudio
        from app.schemas.plan_estudio import PlanEstudioCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            plan_data = PlanEstudioCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_plan = await plan_estudio.create(db, obj_in=plan_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "planes_estudio",
                "record_code": new_plan.codigo,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "plan_codigo": new_plan.codigo,
                "message": f"Plan de estudio {new_plan.codigo} creado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_plan_estudio_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de plan de estudio"""
    try:
        from app.crud.plan_estudio import plan_estudio
        from app.schemas.plan_estudio import PlanEstudioUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo = task_data["codigo"]
            update_data_dict = task_data["update_data"]
            update_data = PlanEstudioUpdate(**update_data_dict)
            
            # Obtener datos originales para rollback
            original_plan = await plan_estudio.get(db, code=codigo)
            if not original_plan:
                return {"success": False, "error": "Plan de estudio no encontrado"}

            original_data = {
                "cant_semestre": original_plan.cant_semestre,
                "plan": original_plan.plan,
                "carrera_codigo": original_plan.carrera_codigo,
            }

            task.progress = 50.0
            await db.commit()

            updated_plan = await plan_estudio.update(db, db_obj=original_plan, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "planes_estudio",
                "record_code": codigo,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "plan_codigo": updated_plan.codigo,
                "message": f"Plan de estudio {updated_plan.codigo} actualizado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_plan_estudio_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de plan de estudio"""
    try:
        from app.crud.plan_estudio import plan_estudio

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo = task_data["codigo"]
            deleted_plan = await plan_estudio.remove(db, code=codigo)

            if not deleted_plan:
                return {"success": False, "error": "Plan de estudio no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Plan de estudio {codigo} eliminado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_plan_estudio_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de plan de estudio"""
    try:
        from app.crud.plan_estudio import plan_estudio

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo = task_data["codigo"]
            plan_obj = await plan_estudio.get(db, code=codigo)

            if not plan_obj:
                return {"success": False, "error": "Plan de estudio no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo": plan_obj.codigo,
                    "cant_semestre": plan_obj.cant_semestre,
                    "plan": plan_obj.plan,
                    "carrera_codigo": plan_obj.carrera_codigo,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_planes_estudio_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de planes de estudio"""
    try:
        from app.crud.plan_estudio import plan_estudio

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 100)

            planes_estudio_list = await plan_estudio.get_multi(db, skip=skip, limit=limit)

            planes_estudio_data = [
                {
                    "codigo": p.codigo,
                    "cant_semestre": p.cant_semestre,
                    "plan": p.plan,
                    "carrera_codigo": p.carrera_codigo,
                }
                for p in planes_estudio_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": planes_estudio_data,
                "total": len(planes_estudio_data),
                "skip": skip,
                "limit": limit,
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# PRERREQUISITO PROCESSORS
# ================================
async def process_create_prerrequisito_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de prerrequisito"""
    try:
        from app.crud.prerrequisito import prerrequisito
        from app.schemas.prerrequisito import PrerequisitoCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            prerrequisito_data = PrerequisitoCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_prerrequisito = await prerrequisito.create(db, obj_in=prerrequisito_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "prerrequisitos",
                "record_code": new_prerrequisito.codigo_prerrequisito,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "prerrequisito_codigo": new_prerrequisito.codigo_prerrequisito,
                "message": f"Prerrequisito {new_prerrequisito.codigo_prerrequisito} creado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_prerrequisito_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de prerrequisito"""
    try:
        from app.crud.prerrequisito import prerrequisito
        from app.schemas.prerrequisito import PrerequisitoUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_prerrequisito = task_data.pop("codigo_prerrequisito")
            update_data = PrerequisitoUpdate(**task_data)
            
            # Obtener datos originales para rollback
            original_prerrequisito = await prerrequisito.get(db, code=codigo_prerrequisito)
            if not original_prerrequisito:
                return {"success": False, "error": "Prerrequisito no encontrado"}

            original_data = {
                "materia_sigla": original_prerrequisito.materia_sigla,
                "sigla_prerrequisito": original_prerrequisito.sigla_prerrequisito,
            }

            task.progress = 50.0
            await db.commit()

            updated_prerrequisito = await prerrequisito.update(db, db_obj=original_prerrequisito, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "prerrequisitos",
                "record_code": codigo_prerrequisito,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "prerrequisito_codigo": updated_prerrequisito.codigo_prerrequisito,
                "message": f"Prerrequisito {updated_prerrequisito.codigo_prerrequisito} actualizado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_prerrequisito_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de prerrequisito"""
    try:
        from app.crud.prerrequisito import prerrequisito

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_prerrequisito = task_data["codigo_prerrequisito"]
            deleted_prerrequisito = await prerrequisito.remove(db, code=codigo_prerrequisito)

            if not deleted_prerrequisito:
                return {"success": False, "error": "Prerrequisito no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Prerrequisito {codigo_prerrequisito} eliminado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_prerrequisito_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de prerrequisito"""
    try:
        from app.crud.prerrequisito import prerrequisito

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo_prerrequisito = task_data["codigo_prerrequisito"]
            prerrequisito_obj = await prerrequisito.get(db, code=codigo_prerrequisito)

            if not prerrequisito_obj:
                return {"success": False, "error": "Prerrequisito no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo_prerrequisito": prerrequisito_obj.codigo_prerrequisito,
                    "materia_sigla": prerrequisito_obj.materia_sigla,
                    "sigla_prerrequisito": prerrequisito_obj.sigla_prerrequisito,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_prerrequisitos_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de prerrequisitos"""
    try:
        from app.crud.prerrequisito import prerrequisito

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            prerrequisitos_list = await prerrequisito.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            prerrequisitos_data = [
                {
                    "codigo_prerrequisito": p.codigo_prerrequisito,
                    "materia_sigla": p.materia_sigla,
                    "sigla_prerrequisito": p.sigla_prerrequisito,
                }
                for p in prerrequisitos_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": prerrequisitos_data,
                "total": len(prerrequisitos_data),
                "skip": skip,
                "limit": limit,
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# DETALLE PROCESSORS
# ================================
async def process_create_detalle_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar creación de detalle"""
    try:
        from app.crud.detalle import detalle
        from app.schemas.detalle import DetalleCreate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            detalle_data = DetalleCreate(**task_data)
            task.progress = 50.0
            await db.commit()

            new_detalle = await detalle.create(db, obj_in=detalle_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "create",
                "table": "detalles",
                "record_code": new_detalle.codigo_detalle,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "detalle_codigo": new_detalle.codigo_detalle,
                "message": f"Detalle {new_detalle.codigo_detalle} creado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_update_detalle_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar actualización de detalle"""
    try:
        from app.crud.detalle import detalle
        from app.schemas.detalle import DetalleUpdate

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_detalle = get_entity_code_from_task_data(task_data, "detalle")
            if not codigo_detalle:
                return {"success": False, "error": "Código de detalle no encontrado en task_data"}
            
            update_data_dict = task_data["update_data"]
            
            # Convert date/time strings back to objects if needed
            from datetime import datetime, date, time
            
            if 'fecha' in update_data_dict and isinstance(update_data_dict['fecha'], str):
                try:
                    update_data_dict['fecha'] = datetime.fromisoformat(update_data_dict['fecha']).date()
                except ValueError:
                    # Try other formats if needed
                    pass
            
            if 'hora' in update_data_dict and isinstance(update_data_dict['hora'], str):
                hora_str = update_data_dict['hora']
                if 'T' in hora_str:  # ISO format
                    hora_str = hora_str.split('T')[-1].rstrip('Z')
                if '.' in hora_str:
                    hora_str = hora_str.split('.')[0]  # Remove milliseconds
                time_parts = hora_str.split(':')
                update_data_dict['hora'] = time(
                    int(time_parts[0]), 
                    int(time_parts[1]), 
                    int(time_parts[2]) if len(time_parts) > 2 else 0
                )
            
            update_data = DetalleUpdate(**update_data_dict)
            
            # Obtener datos originales para rollback
            original_detalle = await detalle.get(db, code=codigo_detalle)
            if not original_detalle:
                return {"success": False, "error": "Detalle no encontrado"}

            original_data = {
                "fecha": original_detalle.fecha.isoformat() if original_detalle.fecha else None,
                "hora": original_detalle.hora.strftime("%H:%M:%S") if original_detalle.hora else None,
                "grupo_codigo": original_detalle.grupo_codigo,
            }

            task.progress = 50.0
            await db.commit()

            updated_detalle = await detalle.update(db, db_obj=original_detalle, obj_in=update_data)

            # Configurar rollback
            task.set_rollback_data({
                "operation": "update",
                "table": "detalles",
                "record_code": codigo_detalle,
                "original_data": original_data,
            })

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "detalle_codigo": updated_detalle.codigo_detalle,
                "message": f"Detalle {updated_detalle.codigo_detalle} actualizado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_delete_detalle_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar eliminación de detalle"""
    try:
        from app.crud.detalle import detalle

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            codigo_detalle = get_entity_code_from_task_data(task_data, "detalle")
            if not codigo_detalle:
                return {"success": False, "error": "Código de detalle no encontrado en task_data"}
            deleted_detalle = await detalle.remove(db, code=codigo_detalle)

            if not deleted_detalle:
                return {"success": False, "error": "Detalle no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "message": f"Detalle {codigo_detalle} eliminado",
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_detalles_list_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de lista de detalles"""
    try:
        from app.crud.detalle import detalle

        async with async_session_factory() as db:
            task.progress = 20.0
            await db.commit()

            skip = task_data.get("skip", 0)
            limit = task_data.get("limit", 50)
            
            task.progress = 50.0
            await db.commit()

            detalles_list = await detalle.get_multi(db, skip=skip, limit=limit)

            task.progress = 80.0
            await db.commit()

            detalles_data = [
                {
                    "codigo_detalle": d.codigo_detalle,
                    "fecha": d.fecha.isoformat() if d.fecha else None,
                    "hora": d.hora.isoformat() if d.hora else None,
                    "grupo_codigo": d.grupo_codigo,
                }
                for d in detalles_list
            ]

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": detalles_data,
                "total": len(detalles_data)
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


async def process_get_detalle_task(task_data: Dict[str, Any], task: Task) -> Dict[str, Any]:
    """Procesar obtención de detalle"""
    try:
        from app.crud.detalle import detalle

        async with async_session_factory() as db:
            task.progress = 50.0
            await db.commit()

            codigo_detalle = task_data["codigo_detalle"]
            detalle_obj = await detalle.get(db, code=codigo_detalle)

            if not detalle_obj:
                return {"success": False, "error": "Detalle no encontrado"}

            task.progress = 100.0
            await db.commit()

            return {
                "success": True,
                "data": {
                    "codigo_detalle": detalle_obj.codigo_detalle,
                    "fecha": detalle_obj.fecha.isoformat() if detalle_obj.fecha else None,
                    "hora": detalle_obj.hora.isoformat() if detalle_obj.hora else None,
                    "grupo_codigo": detalle_obj.grupo_codigo,
                }
            }

    except Exception as e:
        return {"success": False, "error": str(e)}


# ================================
# MAPEO DE TASK PROCESSORS
# ================================
TASK_PROCESSORS: Dict[str, Callable] = {
    # Estudiantes - CRUD completo
    "create_estudiante": process_create_estudiante_task,
    "update_estudiante": process_update_estudiante_task,
    "delete_estudiante": process_delete_estudiante_task,
    "get_estudiante": process_get_estudiante_task,
    "get_estudiantes_list": process_get_estudiantes_list_task,
    
    # Carreras - CRUD completo
    "create_carrera": process_create_carrera_task,
    "update_carrera": process_update_carrera_task,
    "delete_carrera": process_delete_carrera_task,
    "get_carrera": process_get_carrera_task,
    "get_carreras_list": process_get_carreras_list_task,
    
    # Docentes - CRUD completo
    "create_docente": process_create_docente_task,
    "update_docente": process_update_docente_task,
    "delete_docente": process_delete_docente_task,
    "get_docente": process_get_docente_task,
    "get_docentes_list": process_get_docentes_list_task,
    
    # Materias - CRUD completo
    "create_materia": process_create_materia_task,
    "update_materia": process_update_materia_task,
    "delete_materia": process_delete_materia_task,
    "get_materia": process_get_materia_task,
    "get_materias_list": process_get_materias_list_task,
    
    # Grupos - CRUD completo
    "create_grupo": process_create_grupo_task,
    "update_grupo": process_update_grupo_task,
    "delete_grupo": process_delete_grupo_task,
    "get_grupo": process_get_grupo_task,
    "get_grupos_list": process_get_grupos_list_task,
    
    # Inscripciones - CRUD completo
    "create_inscripcion": process_create_inscripcion_task,
    "update_inscripcion": process_update_inscripcion_task,
    "delete_inscripcion": process_delete_inscripcion_task,
    "get_inscripcion": process_get_inscripcion_task,
    "get_inscripciones_list": process_get_inscripciones_list_task,
    
    # Notas - CRUD completo
    "create_nota": process_create_nota_task,
    "update_nota": process_update_nota_task,
    "delete_nota": process_delete_nota_task,
    "get_nota": process_get_nota_task,
    "get_notas_list": process_get_notas_list_task,
    
    # Aulas - CRUD completo
    "create_aula": process_create_aula_task,
    "update_aula": process_update_aula_task,
    "delete_aula": process_delete_aula_task,
    "get_aula": process_get_aula_task,
    "get_aulas_list": process_get_aulas_list_task,
    
    # Horarios - CRUD completo
    "create_horario": process_create_horario_task,
    "update_horario": process_update_horario_task,
    "delete_horario": process_delete_horario_task,
    "get_horario": process_get_horario_task,
    "get_horarios_list": process_get_horarios_list_task,
    
    # Gestiones - CRUD completo
    "create_gestion": process_create_gestion_task,
    "update_gestion": process_update_gestion_task,
    "delete_gestion": process_delete_gestion_task,
    "get_gestion": process_get_gestion_task,
    "get_gestiones_list": process_get_gestiones_list_task,
    
    # Niveles - CRUD completo
    "create_nivel": process_create_nivel_task,
    "update_nivel": process_update_nivel_task,
    "delete_nivel": process_delete_nivel_task,
    "get_nivel": process_get_nivel_task,
    "get_niveles_list": process_get_niveles_list_task,
    
    # Planes de Estudio - CRUD completo
    "create_plan_estudio": process_create_plan_estudio_task,
    "update_plan_estudio": process_update_plan_estudio_task,
    "delete_plan_estudio": process_delete_plan_estudio_task,
    "get_plan_estudio": process_get_plan_estudio_task,
    "get_planes_estudio_list": process_get_planes_estudio_list_task,
    
    # Prerrequisitos - CRUD completo
    "create_prerrequisito": process_create_prerrequisito_task,
    "update_prerrequisito": process_update_prerrequisito_task,
    "delete_prerrequisito": process_delete_prerrequisito_task,
    "get_prerrequisito": process_get_prerrequisito_task,
    "get_prerrequisitos_list": process_get_prerrequisitos_list_task,
    
    # Detalles - CRUD completo
    "create_detalle": process_create_detalle_task,
    "update_detalle": process_update_detalle_task,
    "delete_detalle": process_delete_detalle_task,
    "get_detalle": process_get_detalle_task,
    "get_detalles_list": process_get_detalles_list_task,
}


def get_task_processor(task_type: str) -> Optional[Callable]:
    """Obtener procesador para un tipo de tarea"""
    return TASK_PROCESSORS.get(task_type)


def register_task_processor(task_type: str, processor: Callable):
    """Registrar nuevo procesador de tarea"""
    TASK_PROCESSORS[task_type] = processor
    print(f"📝 Procesador registrado: {task_type}")