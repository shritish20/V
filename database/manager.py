import time
import asyncio
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
from sqlalchemy.exc import TimeoutError as SA_TimeoutError
from contextlib import asynccontextmanager
from tenacity import retry, stop_after_attempt, wait_exponential
from core.config import settings
from utils.logger import setup_logger

logger = setup_logger("DBManager")
Base = declarative_base()

class HybridDatabaseManager:
    """
    FIXED: Added connection pool monitoring and acquisition timeout safety.
    Addresses Critical Issue #4: "Database Connection Pool Exhaustion Risk"
    """
    def __init__(self):
        self.engine = None
        self.async_session = None

    async def init_db(self):
        if self.engine is None:
            # FIX: High-Performance Pool Settings
            self.engine = create_async_engine(
                settings.DATABASE_URL,
                echo=False,
                pool_pre_ping=True,  # Check connection health before using
                pool_size=20,        # Base connections (Increased)
                max_overflow=40,     # Burst connections (Increased)
                pool_recycle=3600,   # Recycle every hour
                pool_timeout=30      # Fail if no connection available after 30s
            )
            
            self.async_session = async_sessionmaker(
                bind=self.engine, 
                expire_on_commit=False, 
                class_=AsyncSession
            )
            
            # Auto-create tables
            try:
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("✓ Database initialized (Pool: 20+40)")
            except Exception as e:
                logger.critical(f"Failed to initialize database tables: {e}")
                raise

    @asynccontextmanager
    async def get_session(self):
        """
        Yields a DB session with performance monitoring.
        """
        if self.async_session is None:
            await self.init_db()
        
        start_time = time.time()
        session: AsyncSession = self.async_session()
        
        try:
            # Monitor Pool Saturation (Diagnostic)
            # checkedout() is available on the underlying pool implementation
            if hasattr(self.engine.pool, 'checkedout'):
                in_use = self.engine.pool.checkedout()
                if in_use > 40: # Warning threshold (approx 70% of max 60)
                    logger.warning(f"⚠️ HIGH DB LOAD: {in_use}/60 connections in use")

            yield session
            
        except SA_TimeoutError:
            # Explicitly catch Pool Exhaustion
            logger.critical("❌ DB POOL EXHAUSTED: Could not acquire connection within timeout!")
            raise
        except Exception as e:
            await session.rollback()
            logger.error(f"DB Session error: {e}")
            raise
        finally:
            await session.close()
            
            # Performance Monitoring: Detect slow transactions
            duration = time.time() - start_time
            if duration > 2.0:
                logger.warning(f"🐢 Slow DB Transaction detected: {duration:.2f}s")

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    async def safe_commit(self, session: AsyncSession):
        """
        Commit with retry logic for transient DB errors.
        """
        try:
            start_commit = time.time()
            await session.commit()
            
            commit_time = time.time() - start_commit
            if commit_time > 1.0:
                 logger.warning(f"🐢 Slow Commit: {commit_time:.2f}s")
                 
        except Exception as e:
            logger.error(f"Commit failed, retrying: {e}")
            await session.rollback()
            raise

    async def close(self):
        """Cleanup method for Engine shutdown"""
        if self.engine:
            await self.engine.dispose()
            logger.info("✓ Database connections closed.")
