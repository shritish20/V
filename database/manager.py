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
    """
    PRODUCTION-READY: Session Leak Detection + Pool Monitoring
    """
    def __init__(self):
        self.engine = None
        self.async_session = None
        
        # CRITICAL FIX: Session Leak Tracking
        self._active_sessions = {}
        self._session_counter = 0
        self._last_pool_warning = 0

    async def init_db(self):
        if self.engine is None:
            # PRODUCTION FIX: Optimized pool settings
            self.engine = create_async_engine(
                settings.DATABASE_URL,
                echo=False,
                pool_pre_ping=True,
                pool_size=20,
                max_overflow=40,
                pool_recycle=3600,
                pool_timeout=30,
                # ADDED: More aggressive connection validation
                pool_reset_on_return='rollback',
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
                logger.info("✅ Database initialized (Pool: 20 base + 40 overflow)")
            except Exception as e:
                logger.critical(f"❌ Failed to initialize database: {e}")
                raise

    @asynccontextmanager
    async def get_session(self):
        """
        PRODUCTION FIX: Session with leak detection and performance monitoring.
        """
        if self.async_session is None:
            await self.init_db()
        
        # Assign unique ID
        session_id = self._session_counter
        self._session_counter += 1
        start_time = time.time()
        
        # Capture caller stack for debugging
        caller_stack = ''.join(traceback.format_stack()[-4:-1])
        
        session: AsyncSession = self.async_session()
        self._active_sessions[session_id] = {
            'start': start_time,
            'stack': caller_stack
        }
        
        try:
            # PRODUCTION FIX: Monitor pool saturation BEFORE yielding
            self._check_pool_health()
            
            yield session
            
        except SA_TimeoutError:
            # Pool exhaustion - emergency diagnostics
            logger.critical("❌ DATABASE POOL EXHAUSTED!")
            self._emergency_pool_diagnostic()
            raise
            
        except Exception as e:
            await session.rollback()
            logger.error(f"DB Session error: {e}")
            raise
            
        finally:
            await session.close()
            self._active_sessions.pop(session_id, None)
            
            # Performance monitoring
            duration = time.time() - start_time
            if duration > 5.0:
                logger.error(
                    f"🐢 CRITICAL: Session {session_id} held for {duration:.1f}s\n"
                    f"Caller:\n{caller_stack}"
                )
            elif duration > 2.0:
                logger.warning(f"🐢 Slow Session {session_id}: {duration:.1f}s")

    def _check_pool_health(self):
        """Monitor connection pool usage"""
        if not hasattr(self.engine.pool, 'checkedout'):
            return
        
        try:
            in_use = self.engine.pool.checkedout()
            pool_size = self.engine.pool.size()
            overflow = self.engine.pool.overflow()
            max_capacity = pool_size + overflow
            
            utilization = in_use / max_capacity if max_capacity > 0 else 0
            
            # Throttle warnings (max 1 per 30 seconds)
            now = time.time()
            if utilization > 0.70 and (now - self._last_pool_warning) > 30:
                logger.warning(
                    f"⚠️ HIGH DB LOAD: {in_use}/{max_capacity} connections in use "
                    f"({utilization*100:.0f}%)"
                )
                self._last_pool_warning = now
            
            # Emergency threshold
            if utilization > 0.90:
                logger.error(
                    f"🚨 CRITICAL DB LOAD: {in_use}/{max_capacity} "
                    f"({utilization*100:.0f}%). Pool exhaustion imminent!"
                )
                self._log_long_running_sessions()
                
        except Exception as e:
            logger.debug(f"Pool health check failed: {e}")

    def _log_long_running_sessions(self):
        """Debug helper: Show oldest sessions"""
        if not self._active_sessions:
            return
        
        now = time.time()
        long_sessions = [
            (sid, now - info['start'], info['stack'])
            for sid, info in self._active_sessions.items()
            if (now - info['start']) > 10.0
        ]
        
        if long_sessions:
            long_sessions.sort(key=lambda x: x[1], reverse=True)
            logger.warning(f"🔍 {len(long_sessions)} sessions open >10s:")
            for sid, age, stack in long_sessions[:5]:
                logger.warning(f"  Session {sid}: {age:.1f}s\n{stack}")

    def _emergency_pool_diagnostic(self):
        """Called when pool exhaustion occurs"""
        logger.critical("=" * 60)
        logger.critical("DATABASE POOL EXHAUSTION DIAGNOSTIC")
        logger.critical("=" * 60)
        
        try:
            if hasattr(self.engine.pool, 'checkedout'):
                in_use = self.engine.pool.checkedout()
                logger.critical(f"Active Connections: {in_use}")
            
            logger.critical(f"Tracked Sessions: {len(self._active_sessions)}")
            
            if self._active_sessions:
                logger.critical("Top 10 Longest Sessions:")
                now = time.time()
                sorted_sessions = sorted(
                    self._active_sessions.items(),
                    key=lambda x: now - x[1]['start'],
                    reverse=True
                )
                
                for sid, info in sorted_sessions[:10]:
                    age = now - info['start']
                    logger.critical(
                        f"\nSession {sid}: {age:.1f}s old\n"
                        f"{info['stack']}"
                    )
        except Exception as e:
            logger.error(f"Diagnostic failed: {e}")
        
        logger.critical("=" * 60)

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
            logger.info("Closing database connections...")
            await self.engine.dispose()
            logger.info("✅ Database connections closed.")
