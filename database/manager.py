#!/usr/bin/env python3
"""
VolGuard 20.0 - Database Manager (Singleton Edition)
- SINGLETON IMPLEMENTED: Solves AWS RDS pool exhaustion.
- INCREASED CAPACITY: Configured for production loads.
"""
import asyncio
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.exc import TimeoutError as SA_TimeoutError
from tenacity import retry, stop_after_attempt, wait_exponential
from core.config import settings
from utils.logger import setup_logger

logger = setup_logger("DBManager")

class HybridDatabaseManager:
    _instance = None  # The Singleton Instance

    def __new__(cls):
        """
        Singleton Pattern: Ensures only ONE instance exists per process.
        This is critical to prevent creating a new DB Pool for every API request.
        """
        if cls._instance is None:
            cls._instance = super(HybridDatabaseManager, cls).__new__(cls)
            cls._instance.engine = None
            cls._instance.async_session = None
            cls._instance._initialized = False
        return cls._instance

    async def init_db(self):
        """Initializes the connection pool if not already active."""
        # If already initialized, do nothing.
        if self._initialized and self.engine is not None:
            return

        logger.info(f"🔌 Connecting to Database: {settings.POSTGRES_SERVER}")
        
        try:
            self.engine = create_async_engine(
                settings.DATABASE_URL,
                echo=False,
                pool_pre_ping=True,  # Critical for AWS RDS timeouts
                pool_size=20,        # Base connections (Increased)
                max_overflow=40,     # Burst capacity
                pool_recycle=1800,   # Recycle connections every 30 mins
                pool_timeout=30,
            )
            
            self.async_session = async_sessionmaker(
                bind=self.engine,
                expire_on_commit=False,
                class_=AsyncSession
            )
            
            # Import models here to ensure they are registered with Base metadata
            from database.models import Base
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
            
            self._initialized = True
            logger.info("✅ Database initialized (Singleton Mode)")
            
        except Exception as e:
            logger.critical(f"🔥 Database Init Failed: {e}")
            raise

    @asynccontextmanager
    async def get_session(self):
        """
        Yields a session from the shared singleton pool.
        """
        if not self._initialized or self.async_session is None:
            await self.init_db()
            
        session: AsyncSession = self.async_session()
        try:
            yield session
        except SA_TimeoutError:
            logger.critical("❌ DB POOL EXHAUSTED! Check AWS RDS max_connections.")
            raise
        except Exception as e:
            await session.rollback()
            logger.error(f"DB Session error: {e}")
            raise
        finally:
            await session.close()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    async def safe_commit(self, session: AsyncSession):
        """Commits transaction with auto-retry logic."""
        try:
            await session.commit()
        except Exception as e:
            logger.error(f"Commit failed, retrying: {e}")
            await session.rollback()
            raise

    async def close(self):
        """Closes the shared connection pool."""
        if self.engine:
            await self.engine.dispose()
            self.engine = None
            self._initialized = False
            logger.info("🛑 Database pool closed.")
