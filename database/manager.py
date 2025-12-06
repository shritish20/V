from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import declarative_base
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

    async def init_db(self):
        if self.engine is None:
            self.engine = create_async_engine(
                settings.DATABASE_URL,
                echo=False,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
            )
            self.async_session = async_sessionmaker(
                bind=self.engine, expire_on_commit=False, class_=AsyncSession
            )
            logger.info("✅ Database engine initialized.")

    @asynccontextmanager
    async def get_session(self):
        if self.async_session is None:
            await self.init_db()
        session: AsyncSession = self.async_session()
        try:
            yield session
        except Exception as e:
            await session.rollback()
            logger.error(f"DB Session error: {e}")
            raise
        finally:
            await session.close()

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=5))
    async def safe_commit(self, session: AsyncSession):
        try:
            await session.commit()
        except Exception as e:
            logger.error(f"Commit failed, retrying: {e}")
            raise
