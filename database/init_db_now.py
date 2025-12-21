# init_db_now.py
import asyncio
from core.config import settings
from database.manager import HybridDatabaseManager
from database.models import Base

async def init():
    print(f"⚡ Connecting to: {settings.DATABASE_URL}")
    db = HybridDatabaseManager()
    
    # Create the engine specifically for metadata creation
    from sqlalchemy.ext.asyncio import create_async_engine
    engine = create_async_engine(settings.DATABASE_URL, echo=True)
    
    async with engine.begin() as conn:
        print("⚡ Creating Tables...")
        await conn.run_sync(Base.metadata.create_all)
        print("✅ Tables Created Successfully!")
        
    await engine.dispose()

if __name__ == "__main__":
    asyncio.run(init())
