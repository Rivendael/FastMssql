import asyncio
import cProfile
import os
import pstats
from io import StringIO

from dotenv import load_dotenv

from fastmssql import Connection, PoolConfig

load_dotenv()


def setup_uvloop():
    """Setup uvloop as the event loop policy if available."""
    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        return True
    except ImportError:
        return False


async def profile_queries():
    conn_str = os.getenv("FASTMSSQL_TEST_CONNECTION_STRING")
    if not conn_str:
        print("❌ No FASTMSSQL_TEST_CONNECTION_STRING set")
        return

    print("Testing simple queries (SELECT 1)...")
    async with Connection(conn_str, PoolConfig.development()) as conn:
        # Warm up
        print("Warming up...")
        for _ in range(10):
            await conn.execute("SELECT 1")

        # Profile 500 simple queries
        print("Profiling 500 queries...")
        for i in range(500):
            await conn.execute("SELECT 1")
            if (i + 1) % 100 == 0:
                print(f"  {i + 1}/500 completed")

    print("✓ Complete")


if __name__ == "__main__":
    print("=" * 60)
    print("FastMSSQL Performance Profiling")
    print("=" * 60)

    uvloop_available = setup_uvloop()
    if uvloop_available:
        print("🚀 Using uvloop for high-performance event loop")

    pr = cProfile.Profile()
    pr.enable()

    asyncio.run(profile_queries())

    pr.disable()
    s = StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats("cumulative")
    ps.print_stats(50)

    print("\n" + "=" * 60)
    print("TOP TIME CONSUMERS")
    print("=" * 60)
    print(s.getvalue())
