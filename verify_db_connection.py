import asyncio
import os
import uuid
import dotenv
from db import init_db, close_db, create_search_run, get_executed_queries, _require_pool

dotenv.load_dotenv()

async def main():
    print("Initializing DB...")
    try:
        await init_db()
        print("DB Initialized.")
    except Exception as e:
        print(f"Failed to initialize DB: {e}")
        return

    try:
        print("Testing create_search_run...")
        run_id = await create_search_run("test_query", "test_mode")
        print(f"Created search run: {run_id}")

        print("Testing get_executed_queries...")
        queries = await get_executed_queries()
        if "test_query" in queries:
            print("Query found in DB.")
        else:
            print("Query NOT found in DB.")

        print("Verification successful.")
    except Exception as e:
        print(f"Verification failed: {e}")
    finally:
        await close_db()

if __name__ == "__main__":
    asyncio.run(main())
