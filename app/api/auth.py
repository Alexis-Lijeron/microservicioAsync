from datetime import timedelta
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from app.config.database import get_db
from app.config.settings import settings
from app.core.security import verify_password, create_access_token
from app.models.estudiante import Estudiante
from app.schemas.auth import UserLogin, Token

router = APIRouter()


async def authenticate_user(db: AsyncSession, registro: str, password: str):
    """Autenticar usuario por registro y contraseña"""
    try:
        result = await db.execute(
            select(Estudiante).where(Estudiante.registro == registro)
        )
        user = result.scalar_one_or_none()

        if not user:
            return None
        if not verify_password(password, user.contraseña):
            return None
        return user
    except Exception as e:
        print(f"Error en autenticación: {e}")
        return None


@router.post("/login", response_model=Token)
async def login_for_access_token(
    user_data: UserLogin, db: AsyncSession = Depends(get_db)
):
    """
    Endpoint de login que devuelve un JWT token
    """
    try:
        user = await authenticate_user(db, user_data.registro, user_data.password)
        if not user:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Registro o contraseña incorrectos",
                headers={"WWW-Authenticate": "Bearer"},
            )

        access_token_expires = timedelta(minutes=settings.access_token_expire_minutes)
        access_token = create_access_token(
            subject=user.registro, expires_delta=access_token_expires
        )

        return {"access_token": access_token, "token_type": "bearer"}
    except HTTPException:
        raise
    except Exception as e:
        print(f"Error en login: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error interno del servidor",
        )


@router.get("/me")
async def get_current_user_info(db: AsyncSession = Depends(get_db)):
    """
    Obtener información del usuario actual
    """
    # Importar aquí para evitar circular imports
    from app.api.deps import get_current_active_user

    # Esta es una implementación temporal
    return {
        "message": "Endpoint para obtener info del usuario actual",
        "note": "Requiere token JWT en header Authorization",
    }
