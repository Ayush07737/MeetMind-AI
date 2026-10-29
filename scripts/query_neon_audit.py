import asyncio
import ssl

import asyncpg
from dotenv import load_dotenv
from meetmind_security import db

load_dotenv()


async def main():
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    conn = await asyncpg.connect(db.get_app_dsn(), ssl=ctx)
    schemas = await conn.fetch(
        "SELECT schema_name FROM information_schema.schemata "
        "WHERE schema_name LIKE 'tenant_bench_%'"
    )
    print(f"Checking {len(schemas)} tenant_bench_* schemas for mtg_bench_a6a72332 or 2026-10-23...")
    for s in schemas:
        sname = s["schema_name"]
        try:
            rows = await conn.fetch(f"""
                SELECT event_type, created_at, payload->>'meeting_id' as mid, payload
                FROM "{sname}".audit_events 
                WHERE created_at >= '2026-10-23 00:00:00+00'
                ORDER BY created_at ASC
            """)
            if rows:
                print(f"=== {sname} ({len(rows)} events) ===")
                for r in rows:
                    print(f"  {r['created_at']} | {r['event_type']} | mid={r['mid']}")
        except Exception:
            pass
    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
