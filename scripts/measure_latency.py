import asyncio
import os
import re
import statistics
import time
from urllib.parse import urlparse

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DSN = os.environ.get("NEON_APP_DATABASE_URL") or os.environ.get("NEON_DATABASE_URL")
OWNER_DSN = os.environ.get("NEON_DATABASE_URL")


async def run_benchmark():
    print("=" * 70)
    print(" MeetMind AI - CO-8 Latency and DB Statement Benchmark")
    print("=" * 70)

    # 1. Neon Region from hostname
    parsed = urlparse(DSN)
    hostname = parsed.hostname or ""
    print(f"Neon Hostname:      {hostname}")
    match = re.search(r"([a-z]{2}-[a-z]+-\d+)", hostname)
    neon_region = match.group(1) if match else "unknown"
    print(f"Neon Region:        {neon_region} (AWS Singapore)")
    print("-" * 70)

    # 2. SELECT 1 RTT over 20 runs
    print("Measuring SELECT 1 RTT over 20 consecutive runs on an established connection...")
    conn = await asyncpg.connect(DSN, statement_cache_size=0)
    try:
        rtts = []
        for _ in range(20):
            t0 = time.perf_counter()
            await conn.fetchval("SELECT 1")
            t1 = time.perf_counter()
            rtts.append((t1 - t0) * 1000)

        rtts_sorted = sorted(rtts)
        p50 = statistics.median(rtts)
        p95 = rtts_sorted[int(len(rtts_sorted) * 0.95)]
        mean = statistics.mean(rtts)
        print("  Runs: 20")
        print(f"  Min:  {min(rtts):.2f} ms")
        print(f"  Mean: {mean:.2f} ms")
        print(f"  p50:  {p50:.2f} ms")
        print(f"  p95:  {p95:.2f} ms")
        print(f"  Max:  {max(rtts):.2f} ms")
    finally:
        await conn.close()

    print("-" * 70)
    print("Measuring Per-DB-Statement Breakdown & Ack Latency (Cold vs Warmed)...")

    # Set up test schema and table using owner connection
    test_schema = "tenant_latency_bench"
    owner_conn = await asyncpg.connect(OWNER_DSN, statement_cache_size=0)
    try:
        from meetmind_security.migrations import apply_migrations_conn

        await owner_conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{test_schema}"')
        await apply_migrations_conn(owner_conn, test_schema)
    finally:
        await owner_conn.close()

    # Measure COLD start (creating pool and first acquire)
    t_cold_start = time.perf_counter()
    cold_pool = await asyncpg.create_pool(DSN, min_size=1, max_size=5, statement_cache_size=0)
    t_cold_created = time.perf_counter()
    cold_connect_ms = (t_cold_created - t_cold_start) * 1000

    # 3. Per-statement measurements
    async with cold_pool.acquire() as c:
        await c.execute(f'SET search_path = "{test_schema}"')
        t0 = time.perf_counter()
        await c.execute(
            """
            INSERT INTO audit_events (tenant_id, user_id, event_type, payload)
            VALUES ($1, $2, $3, $4::jsonb)
            """,
            "latency_bench",
            "bench_user",
            "meeting_started",
            '{"meeting_id": "mtg_bench_1", "adapter_type": "desktop_app"}',
        )
        t_mtg_start_cold = (time.perf_counter() - t0) * 1000

    await cold_pool.close()

    # WARM POOL: min_size connections pre-allocated (Lifespan pattern)
    warm_pool = await asyncpg.create_pool(DSN, min_size=2, max_size=10, statement_cache_size=0)
    warmed = [await warm_pool.acquire() for _ in range(warm_pool.get_min_size())]
    for c in warmed:
        await warm_pool.release(c)

    stmt_mtg_start_times = []
    stmt_consent_times = []
    stmt_mtg_end_times = []

    for i in range(5):
        async with warm_pool.acquire() as c:
            await c.execute(f'SET search_path = "{test_schema}"')

            # meeting_start audit write
            t0 = time.perf_counter()
            await c.execute(
                """
                INSERT INTO audit_events (tenant_id, user_id, event_type, payload)
                VALUES ($1, $2, $3, $4::jsonb)
                """,
                "latency_bench",
                f"bench_user_{i}",
                "meeting_started",
                '{"meeting_id": "mtg_bench", "adapter_type": "desktop_app"}',
            )
            stmt_mtg_start_times.append((time.perf_counter() - t0) * 1000)

            # consent_confirmed write
            t0 = time.perf_counter()
            await c.execute(
                """
                INSERT INTO consent_events (tenant_id, user_id, meeting_id, consent_type, action)
                VALUES ($1, $2, $3, $4, $5)
                """,
                "latency_bench",
                f"bench_user_{i}",
                f"mtg_bench_{i}",
                "audio_capture",
                "granted",
            )
            stmt_consent_times.append((time.perf_counter() - t0) * 1000)

            # meeting_end audit write
            t0 = time.perf_counter()
            await c.execute(
                """
                INSERT INTO audit_events (tenant_id, user_id, event_type, payload)
                VALUES ($1, $2, $3, $4::jsonb)
                """,
                "latency_bench",
                f"bench_user_{i}",
                "meeting_ended",
                '{"meeting_id": "mtg_bench", "total_chunks": 100}',
            )
            stmt_mtg_end_times.append((time.perf_counter() - t0) * 1000)

    # Teardown test schema
    owner_conn = await asyncpg.connect(OWNER_DSN, statement_cache_size=0)
    try:
        await owner_conn.execute(f'DROP SCHEMA IF EXISTS "{test_schema}" CASCADE')
    finally:
        await owner_conn.close()

    await warm_pool.close()

    print("Latency Results Summary:")
    print(f"  Cold Pool Handshake + SSL Connect: {cold_connect_ms:.2f} ms")
    print(f"  Cold meeting_start DB Statement:   {t_mtg_start_cold:.2f} ms")
    print()
    print("  Warmed Pool Per-Statement Latency (5-run mean):")
    m_st = statistics.mean(stmt_mtg_start_times)
    min_st, max_st = min(stmt_mtg_start_times), max(stmt_mtg_start_times)
    print(f"    - meeting_start     (INSERT): {m_st:.2f} ms (min: {min_st:.2f}, max: {max_st:.2f})")

    m_co = statistics.mean(stmt_consent_times)
    min_co, max_co = min(stmt_consent_times), max(stmt_consent_times)
    print(f"    - consent_confirmed (INSERT): {m_co:.2f} ms (min: {min_co:.2f}, max: {max_co:.2f})")

    m_en = statistics.mean(stmt_mtg_end_times)
    min_en, max_en = min(stmt_mtg_end_times), max(stmt_mtg_end_times)
    print(f"    - meeting_end       (INSERT): {m_en:.2f} ms (min: {min_en:.2f}, max: {max_en:.2f})")
    print("=" * 70)


asyncio.run(run_benchmark())
