#!/usr/bin/env python3
"""
VolGuard 20.0 – Database Manager (Hardened)
- Optimized Connection Pool for 4-Process Architecture
- Session Health Tracking & Auto-Recovery
"""
import time
import asyncio
import traceback
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.exc import TimeoutError as SA_TimeoutError
from tenacity import retry, stop_after_attempt, wait_exponential
from core.config import settings
from utils.logger import setup_logger

logger = setup_logger("DBManager")

class HybridDatabaseManager:
    """
    Manages the Async Database connection pool.
    Optimized for multiple concurrent processes (Engine, Sheriff, API, Analyst).
    """
    def __init__(self):
        self.engine = None
        self.async_session = None
        self._active_sessions = {}
        self._session_counter = 0
        self._last_pool_warning = 0

    async def init_db(self):
        """Initializes the connection pool and creates tables if missing."""
        if self.engine is None:
            self.engine = create_async_engine(
                settings.DATABASE_URL,
                echo=False,
                pool_pre_ping=True,  # Critical: Checks connection health before use
                # CLAUDE FIX: Reduced pool size to prevent exhaustion across 4 processes
                pool_size=5,         
                max_overflow=10,     
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
                # Import Base here to ensure we pick up the models defined in database/models.py
                from database.models import Base
                async with self.engine.begin() as conn:
                    await conn.run_sync(Base.metadata.create_all)
                logger.info("✅ Database initialized (Pool: 5 base + 10 overflow)")
            except Exception as e:
                logger.critical(f"🔥 Failed to initialize database: {e}")
                raise

    @asynccontextmanager
    async def get_session(self):
        """
        Yields a DB session with performance tracking and error handling.
        """
        if self.async_session is None:
            await self.init_db()
        
        session_id = self._session_counter
        self._session_counter += 1
        start_time = time.time()
        # Debug helper: Capture where this session was requested from
        caller_stack = ''.join(traceback.format_stack()[-4:-1])
        
        session: AsyncSession = self.async_session()
        self._active_sessions[session_id] = {'start': start_time, 'stack': caller_stack}

        try:
            self._check_pool_health()
            yield session
        except SA_TimeoutError:
            logger.critical("❌ DATABASE POOL EXHAUSTED! Increase pool_size or check for stuck sessions.")
            raise
        except Exception as e:
            await session.rollback()
            logger.error(f"DB Session error: {e}")
            raise
        finally:
            await session.close()
            self._active_sessions.pop(session_id, None)
            
            # Warn if a query held the DB for too long (>5s)
            duration = time.time() - start_time
            if duration > 5.0:
                logger.error(f"⚠️ SLOW SESSION: Session {session_id} held for {duration:.1f}s")

    def _check_pool_health(self):
        """Monitors pool utilization and logs warnings if high."""
        if not hasattr(self.engine.pool, 'checkedout'): return
        try:
            in_use = self.engine.pool.checkedout()
            pool_size = self.engine.pool.size()
            overflow = self.engine.pool.overflow()
            max_capacity = pool_size + overflow
            utilization = in_use / max_capacity if max_capacity > 0 else 0
            
            now = time.time()
            # Warn if > 70% of connections are in use, but limit log spam (once per 30s)
            if utilization > 0.70 and (now - self._last_pool_warning) > 30:
                logger.warning(f"⚠️ HIGH DB LOAD: {in_use}/{max_capacity} connections ({utilization*100:.0f}%)")
                self._last_pool_warning = now
        except Exception: pass

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
        """Closes the connection pool."""
        if self.engine:
            await self.engine.dispose()
