import asyncpg
import asyncio

async def test():
    try:
        conn = await asyncpg.connect("postgresql://admin:ITEX2024@192.168.0.244:5432/prefect")
        print("Connected!")
        await conn.close()
    except Exception as e: 
        print(e)

asyncio.run(test())