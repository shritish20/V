import time
import asyncio
import traceback
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
    def __init__(self):
        self.engine = None
        self.async_session = None
        self._active_sessions = {}
        self._session_counter = 0
        self._last_pool_warning = 0

    async def init_db(self):
        if self.engine is None:
            self.engine = create_async_engine(
                settings.DATABASE_URL,
                echo=False,
                pool_pre_ping=True,
                pool_size=20,
                max_overflow=40,
                pool_recycle=3600,
                pool_timeout=30,
                pool_reset_on_return='rollback',
            )
            self.async_session = async_sessionmaker(
                bind=self.engine,
                expire_on_commit=False,
                class_=AsyncSession
            )
            try:
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("Database initialized (Pool: 20 base + 40 overflow)")
            except Exception as e:
                logger.critical(f"Failed to initialize database: {e}")
                raise

    @asynccontextmanager
    async def get_session(self):
        if self.async_session is None:
            await self.init_db()
        
        session_id = self._session_counter
        self._session_counter += 1
        start_time = time.time()
        caller_stack = ''.join(traceback.format_stack()[-4:-1])
        
        session: AsyncSession = self.async_session()
        self._active_sessions[session_id] = {'start': start_time, 'stack': caller_stack}

        try:
            self._check_pool_health()
            yield session
        except SA_TimeoutError:
            logger.critical("DATABASE POOL EXHAUSTED!")
            raise
        except Exception as e:
            await session.rollback()
            logger.error(f"DB Session error: {e}")
            raise
        finally:
            await session.close()
            self._active_sessions.pop(session_id, None)
            
            duration = time.time() - start_time
            if duration > 5.0:
                logger.error(f"CRITICAL: Session {session_id} held for {duration:.1f}s")

    def _check_pool_health(self):
        if not hasattr(self.engine.pool, 'checkedout'): return
        try:
            in_use = self.engine.pool.checkedout()
            pool_size = self.engine.pool.size()
            overflow = self.engine.pool.overflow()
            max_capacity = pool_size + overflow
            utilization = in_use / max_capacity if max_capacity > 0 else 0
            
            now = time.time()
            if utilization > 0.70 and (now - self._last_pool_warning) > 30:
                logger.warning(f"HIGH DB LOAD: {in_use}/{max_capacity} ({utilization*100:.0f}%)")
                self._last_pool_warning = now
        except Exception: pass

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    async def safe_commit(self, session: AsyncSession):
        try:
            await session.commit()
        except Exception as e:
            logger.error(f"Commit failed, retrying: {e}")
            await session.rollback()
            raise

    async def close(self):
        if self.engine:
            await self.engine.dispose()
