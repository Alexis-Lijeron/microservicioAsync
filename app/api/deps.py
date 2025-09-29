from typing import Generator, Optional
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config.database import get_db
from app.config.settings import settings
from app.core.security import verify_token

security = HTTPBearer(auto_error=False)  # auto_error=False permite requests sin token


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(security),
    db: AsyncSession = Depends(get_db),
):
    """
    Obtener usuario actual desde el token JWT
    Si disable_auth está activado, devuelve un usuario mock
    """
    from app.models.estudiante import Estudiante  # Importar aquí para evitar ciclos

    # Si la autenticación está deshabilitada globalmente
    if settings.disable_auth:
        # Crear un usuario mock para desarrollo
        mock_user = type('MockUser', (), {
            'registro': 'DEV001',
            'nombre': 'Developer',
            'apellido': 'User',
            'ci': '00000000',
            'carrera_codigo': 'INF187'
        })()
        return mock_user

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="No se pudo validar las credenciales",
        headers={"WWW-Authenticate": "Bearer"},
    )

    # Si no hay credenciales y la auth está habilitada
    if not credentials:
        raise credentials_exception

    # Verificar token
    registro = verify_token(credentials.credentials)
    if registro is None:
        raise credentials_exception

    # Buscar usuario en la base de datos
    result = await db.execute(select(Estudiante).where(Estudiante.registro == registro))
    user = result.scalar_one_or_none()

    if user is None:
        raise credentials_exception

    return user


async def get_current_active_user(current_user=Depends(get_current_user)):
    """
    Obtener usuario activo (se puede extender para verificar si está activo)
    """
    return current_user
