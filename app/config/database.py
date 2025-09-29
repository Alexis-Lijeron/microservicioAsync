from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy import text
import asyncio
from .settings import settings

# Create async engine with better connection handling
engine = create_async_engine(
    settings.database_url,
    echo=settings.debug,
    future=True,
    pool_pre_ping=True,
    pool_recycle=1200,
    pool_size=1000,
    max_overflow=19500,
    pool_timeout=10,
    connect_args={
        "server_settings": {
            "application_name": "universidad_app",
        },
        "command_timeout": 60,
    }
)

# Create session factory
async_session_factory = async_sessionmaker(
    engine, 
    class_=AsyncSession, 
    expire_on_commit=False,
    autoflush=True,
    autocommit=False
)

Base = declarative_base()


async def get_db() -> AsyncSession:
    """Get database session with proper error handling"""
    session = None
    try:
        session = async_session_factory()
        yield session
    except Exception as e:
        if session:
            await session.rollback()
        raise e
    finally:
        if session:
            await session.close()


async def test_connection(max_retries: int = 5, delay: float = 2.0) -> bool:
    """Test database connection with retries"""
    for attempt in range(max_retries):
        try:
            async with async_session_factory() as session:
                result = await session.execute(text("SELECT 1"))
                await session.commit()
                print(f"Database connection successful on attempt {attempt + 1}")
                return True
        except Exception as e:
            print(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                print(f"Retrying in {delay} seconds...")
                await asyncio.sleep(delay)
            else:
                print("All database connection attempts failed")
                return False
    return False


async def wait_for_db(max_wait: int = 60) -> bool:
    """Wait for database to be ready"""
    print("Waiting for database to be ready...")
    start_time = asyncio.get_event_loop().time()
    
    while True:
        try:
            if await test_connection(max_retries=1):
                return True
        except Exception as e:
            print(f"Database not ready: {e}")
        
        elapsed = asyncio.get_event_loop().time() - start_time
        if elapsed > max_wait:
            print(f"Timeout waiting for database after {max_wait} seconds")
            return False
        
        await asyncio.sleep(2)


async def init_db():
    """Initialize database tables with connection verification"""
    try:
        # First, wait for database to be ready
        if not await wait_for_db():
            raise Exception("Database is not ready")
        
        # Create tables
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        
        print("Database tables initialized successfully")
        return True
        
    except Exception as e:
        print(f"Failed to initialize database: {e}")
        raise e


async def close_db():
    """Close database connections"""
    try:
        await engine.dispose()
        print("Database connections closed")
    except Exception as e:
        print(f"Error closing database connections: {e}")