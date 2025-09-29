import hashlib
import json
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.models.pagination_state import PaginationState
from app.config.database import async_session_factory


class CorrectedSmartPaginator:
    """Sistema de paginación inteligente corregido"""

    def __init__(self, session_ttl_hours: int = 24, default_page_size: int = 20):
        self.session_ttl_hours = session_ttl_hours
        self.default_page_size = default_page_size

    def _generate_query_hash(self, endpoint: str, params: Dict[str, Any]) -> str:
        """Generar hash único para una consulta"""
        normalized_params = {
            k: v
            for k, v in sorted(params.items())
            if k not in ["page", "limit", "session_id", "page_size"]
        }
        query_string = (
            f"{endpoint}:{json.dumps(normalized_params, sort_keys=True, default=str)}"
        )
        return hashlib.sha256(query_string.encode()).hexdigest()

    async def get_or_create_session(
        self,
        session_id: str,
        endpoint: str,
        query_params: Dict[str, Any],
        page_size: Optional[int] = None,
    ) -> PaginationState:
        """Obtener o crear sesión de paginación"""

        if page_size is None:
            page_size = self.default_page_size

        query_hash = self._generate_query_hash(endpoint, query_params)

        async with async_session_factory() as db:
            try:
                # Buscar sesión existente
                result = await db.execute(
                    select(PaginationState).where(
                        PaginationState.session_id == session_id,
                        PaginationState.endpoint == endpoint,
                        PaginationState.query_hash == query_hash,
                        PaginationState.is_active == True,
                    )
                )

                pagination_state = result.scalar_one_or_none()

                if pagination_state and not pagination_state.is_expired():
                    # Actualizar último acceso
                    pagination_state.last_accessed = datetime.utcnow()
                    await db.commit()
                    print(f"📄 Sesión existente encontrada: {session_id}")
                    return pagination_state
                else:
                    # Crear nueva sesión
                    if pagination_state:
                        # Desactivar sesión expirada
                        pagination_state.is_active = False

                    new_state = PaginationState(
                        session_id=session_id,
                        endpoint=endpoint,
                        query_hash=query_hash,
                        items_per_page=page_size,
                        expires_at=datetime.utcnow()
                        + timedelta(hours=self.session_ttl_hours),
                    )
                    new_state.set_query_params(query_params)
                    new_state.set_returned_items([])

                    db.add(new_state)
                    await db.commit()
                    await db.refresh(new_state)

                    print(f"📄 Nueva sesión creada: {session_id}")
                    return new_state

            except Exception as e:
                await db.rollback()
                print(f"❌ Error en get_or_create_session: {e}")
                raise e

    async def get_next_page(
        self,
        session_id: str,
        endpoint: str,
        query_function,
        query_params: Dict[str, Any],
        page_size: Optional[int] = None,
    ) -> Tuple[List[Any], Dict[str, Any]]:
        """Obtener siguiente página de resultados"""

        try:
            pagination_state = await self.get_or_create_session(
                session_id, endpoint, query_params, page_size
            )

            async with async_session_factory() as db:
                returned_items = pagination_state.get_returned_items()
                offset = len(returned_items)

                # Ejecutar consulta
                results = await query_function(
                    db=db,
                    offset=offset,
                    limit=pagination_state.items_per_page,
                    **query_params,
                )

                # Extraer IDs de los resultados
                new_item_ids = []
                if results:
                    if isinstance(results[0], dict):
                        new_item_ids = [
                            item.get("id") for item in results if "id" in item
                        ]
                    else:
                        new_item_ids = [
                            getattr(item, "id", None)
                            for item in results
                            if hasattr(item, "id")
                        ]

                # Actualizar estado
                pagination_state.add_returned_items(new_item_ids)
                pagination_state.current_page += 1
                pagination_state.last_accessed = datetime.utcnow()

                # Calcular total solo la primera vez o si no se ha calculado
                if pagination_state.total_items == 0:
                    try:
                        total_count = await self._get_total_count(
                            query_function, query_params
                        )
                        pagination_state.total_items = total_count
                    except Exception as e:
                        print(f"⚠️ Error calculando total: {e}")
                        pagination_state.total_items = len(returned_items) + len(
                            results
                        )

                await db.commit()

                # Metadata
                total_returned = len(pagination_state.get_returned_items())
                has_more = len(results) == pagination_state.items_per_page

                metadata = {
                    "session_id": session_id,
                    "current_page": pagination_state.current_page,
                    "items_per_page": pagination_state.items_per_page,
                    "items_in_page": len(results),
                    "total_items_returned": total_returned,
                    "total_items_available": pagination_state.total_items,
                    "has_more_pages": has_more,
                    "progress_percentage": (
                        (total_returned / pagination_state.total_items * 100)
                        if pagination_state.total_items > 0
                        else 0
                    ),
                    "endpoint": endpoint,
                    "query_params": pagination_state.get_query_params(),
                }

                print(
                    f"📊 Página {pagination_state.current_page}: {len(results)} elementos"
                )
                return results, metadata

        except Exception as e:
            print(f"❌ Error en get_next_page: {e}")
            # Fallback: devolver resultados sin paginación inteligente
            async with async_session_factory() as db:
                try:
                    fallback_results = await query_function(
                        db=db, offset=0, limit=page_size or 20, **query_params
                    )
                    fallback_metadata = {
                        "session_id": session_id,
                        "current_page": 1,
                        "items_per_page": len(fallback_results),
                        "items_in_page": len(fallback_results),
                        "total_items_returned": len(fallback_results),
                        "total_items_available": len(fallback_results),
                        "has_more_pages": False,
                        "progress_percentage": 100,
                        "error": "Paginación inteligente falló, usando fallback",
                    }
                    return fallback_results, fallback_metadata
                except Exception as fallback_error:
                    print(f"❌ Error en fallback: {fallback_error}")
                    return [], {
                        "error": f"Error crítico: {str(e)}",
                        "session_id": session_id,
                        "current_page": 0,
                        "items_per_page": 0,
                        "items_in_page": 0,
                        "total_items_returned": 0,
                        "total_items_available": 0,
                        "has_more_pages": False,
                        "progress_percentage": 0,
                    }

    async def reset_session(self, session_id: str, endpoint: str = None) -> bool:
        """Reiniciar sesión de paginación"""
        async with async_session_factory() as db:
            try:
                if endpoint:
                    # Reiniciar sesión específica
                    result = await db.execute(
                        select(PaginationState).where(
                            PaginationState.session_id == session_id,
                            PaginationState.endpoint == endpoint,
                            PaginationState.is_active == True,
                        )
                    )
                    states = result.scalars().all()
                else:
                    # Reiniciar todas las sesiones del session_id
                    result = await db.execute(
                        select(PaginationState).where(
                            PaginationState.session_id == session_id,
                            PaginationState.is_active == True,
                        )
                    )
                    states = result.scalars().all()

                for state in states:
                    state.is_active = False
                    state.last_accessed = datetime.utcnow()

                await db.commit()
                print(f"🔄 {len(states)} sesiones reiniciadas para {session_id}")
                return len(states) > 0

            except Exception as e:
                await db.rollback()
                print(f"❌ Error reiniciando sesión: {e}")
                return False

    async def get_session_info(self, session_id: str) -> List[Dict[str, Any]]:
        """Obtener información de sesiones activas"""
        async with async_session_factory() as db:
            try:
                result = await db.execute(
                    select(PaginationState).where(
                        PaginationState.session_id == session_id,
                        PaginationState.is_active == True,
                    )
                )

                states = result.scalars().all()

                return [
                    {
                        "endpoint": state.endpoint,
                        "current_page": state.current_page,
                        "items_per_page": state.items_per_page,
                        "total_items": state.total_items,
                        "items_returned": len(state.get_returned_items()),
                        "last_accessed": state.last_accessed.isoformat(),
                        "expires_at": (
                            state.expires_at.isoformat() if state.expires_at else None
                        ),
                        "progress_percentage": (
                            (len(state.get_returned_items()) / state.total_items * 100)
                            if state.total_items > 0
                            else 0
                        ),
                        "query_hash": state.query_hash[:8],  # Solo primeros 8 chars
                        "is_expired": state.is_expired(),
                    }
                    for state in states
                ]
            except Exception as e:
                print(f"❌ Error obteniendo info de sesión: {e}")
                return []

    async def cleanup_expired_sessions(self) -> int:
        """Limpiar sesiones expiradas"""
        async with async_session_factory() as db:
            try:
                # Buscar sesiones expiradas
                result = await db.execute(
                    select(PaginationState).where(
                        PaginationState.expires_at < datetime.utcnow()
                    )
                )

                expired_states = result.scalars().all()
                count = len(expired_states)

                # Eliminar sesiones expiradas
                for state in expired_states:
                    await db.delete(state)

                await db.commit()

                if count > 0:
                    print(f"🧹 {count} sesiones de paginación expiradas eliminadas")

                return count

            except Exception as e:
                await db.rollback()
                print(f"❌ Error limpiando sesiones: {e}")
                raise e

    async def _get_total_count(
        self, query_function, query_params: Dict[str, Any]
    ) -> int:
        """Obtener conteo total de elementos"""
        try:
            async with async_session_factory() as db:
                # Intentar obtener muestra grande para estimar
                sample_results = await query_function(
                    db=db, offset=0, limit=1000, **query_params
                )

                sample_count = len(sample_results)

                if sample_count < 1000:
                    # Si obtenemos menos de 1000, probablemente ese es el total
                    return sample_count
                else:
                    # Si obtenemos exactamente 1000, hay más elementos
                    # Hacer una estimación conservadora
                    return sample_count + 100  # Estimación

        except Exception as e:
            print(f"⚠️ Error calculando total: {e}")
            return 100  # Valor por defecto


# Instancia global corregida
corrected_smart_paginator = CorrectedSmartPaginator()
