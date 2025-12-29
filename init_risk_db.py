import asyncio
import sys, os
sys.path.append(os.getcwd())
from database.manager import HybridDatabaseManager
from database.models_risk import Base

async def init():
    db = HybridDatabaseManager()
    await db.init_db()
    async with db.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    print("Risk Tables Created.")

if __name__ == "__main__": asyncio.run(init())
