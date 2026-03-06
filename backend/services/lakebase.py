"""Lakebase (PostgreSQL) persistence for Genie Space scan results."""

import asyncio
import logging
import os
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# In-memory fallback store (used when Lakebase is unavailable)
_memory_store: dict = {
    "scans": {},      # space_id -> latest ScanResult dict
    "history": {},    # space_id -> list of ScanResult dicts (ordered by timestamp)
    "stars": set(),   # set of starred space_ids
    "seen": set(),    # set of seen space_ids
}

_pool = None
_lakebase_available = False


def _generate_lakebase_credential() -> tuple[str, str] | None:
    """Generate Lakebase OAuth credentials using the Databricks SDK.

    Uses whatever auth the SDK resolves (service principal in prod, CLI
    profile locally). Returns (user_email, oauth_token) or None.
    """
    instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME")
    if not instance_name:
        return None

    try:
        from backend.services.auth import get_workspace_client
        client = get_workspace_client()

        resp = client.api_client.do(
            method="POST",
            path="/api/2.0/database/credentials",
            body={
                "request_id": "lakebase-pool",
                "instance_names": [instance_name],
            },
        )
        token = resp.get("token")
        if not token:
            logger.warning("Lakebase credential response missing token")
            return None

        # Resolve username: SP identity or human user
        user = os.environ.get("LAKEBASE_USER")
        if not user:
            try:
                me = client.current_user.me()
                user = me.user_name
            except Exception:
                user = "databricks"
        logger.info(f"Generated Lakebase credential via SDK (user={user})")
        return user, token
    except Exception as e:
        logger.warning(f"Lakebase credential generation failed: {e}")
        return None


async def init_pool():
    """Initialize asyncpg connection pool. Falls back gracefully if unavailable."""
    global _pool, _lakebase_available

    host = os.environ.get("LAKEBASE_HOST")
    if not host:
        logger.info("LAKEBASE_HOST not set - using in-memory fallback")
        return

    password = os.environ.get("LAKEBASE_PASSWORD")
    user = os.environ.get("LAKEBASE_USER", "postgres")

    if not password:
        cred = _generate_lakebase_credential()
        if cred:
            user, password = cred
        else:
            logger.warning("No LAKEBASE_PASSWORD and credential generation failed - using in-memory fallback")
            return

    try:
        import asyncpg
        _pool = await asyncpg.create_pool(
            host=host,
            port=int(os.environ.get("LAKEBASE_PORT", "5432")),
            database=os.environ.get("LAKEBASE_DATABASE", "databricks_postgres"),
            user=user,
            password=password,
            min_size=2,
            max_size=10,
            command_timeout=30,
            ssl="require",
        )
        _lakebase_available = True
        logger.info("Lakebase connection pool initialized")
    except Exception as e:
        logger.warning(f"Lakebase unavailable: {e}. Using in-memory fallback.")
        _lakebase_available = False


async def close_pool():
    """Close the connection pool."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def save_scan_result(space_id: str, scan_result: dict) -> None:
    """Save a scan result to Lakebase (or in-memory fallback)."""
    scan_result["scanned_at"] = scan_result.get("scanned_at", datetime.utcnow().isoformat())

    if not _lakebase_available or _pool is None:
        _memory_store["scans"][space_id] = scan_result
        history = _memory_store["history"].setdefault(space_id, [])
        history.append(scan_result)
        # Keep last 30 entries
        _memory_store["history"][space_id] = history[-30:]
        _memory_store["seen"].add(space_id)
        return

    import json
    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO scan_results (space_id, score, maturity, breakdown, findings, next_steps, scanned_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (space_id, scanned_at) DO UPDATE SET
                score = EXCLUDED.score,
                maturity = EXCLUDED.maturity,
                breakdown = EXCLUDED.breakdown,
                findings = EXCLUDED.findings,
                next_steps = EXCLUDED.next_steps
        """,
            space_id,
            scan_result["score"],
            scan_result["maturity"],
            json.dumps(scan_result.get("breakdown", {})),
            json.dumps(scan_result.get("findings", [])),
            json.dumps(scan_result.get("next_steps", [])),
            datetime.fromisoformat(scan_result["scanned_at"]),
        )
        await conn.execute(
            "INSERT INTO seen_spaces (space_id) VALUES ($1) ON CONFLICT DO NOTHING",
            space_id,
        )


async def get_latest_score(space_id: str) -> Optional[dict]:
    """Get the latest scan result for a space."""
    if not _lakebase_available or _pool is None:
        return _memory_store["scans"].get(space_id)

    import json
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT score, maturity, breakdown, findings, next_steps, scanned_at
            FROM scan_results
            WHERE space_id = $1
            ORDER BY scanned_at DESC
            LIMIT 1
        """, space_id)
        if not row:
            return None
        return {
            "score": row["score"],
            "maturity": row["maturity"],
            "breakdown": json.loads(row["breakdown"]),
            "findings": json.loads(row["findings"]),
            "next_steps": json.loads(row["next_steps"]),
            "scanned_at": row["scanned_at"].isoformat(),
        }


async def get_score_history(space_id: str, days: int = 30) -> list[dict]:
    """Get score history for a space over the last N days."""
    if not _lakebase_available or _pool is None:
        return _memory_store["history"].get(space_id, [])

    import json
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT score, maturity, scanned_at
            FROM scan_results
            WHERE space_id = $1
              AND scanned_at >= NOW() - INTERVAL '$2 days'
            ORDER BY scanned_at ASC
        """, space_id, days)
        return [
            {
                "score": r["score"],
                "maturity": r["maturity"],
                "scanned_at": r["scanned_at"].isoformat(),
            }
            for r in rows
        ]


async def star_space(space_id: str, starred: bool) -> None:
    """Star or unstar a space."""
    if not _lakebase_available or _pool is None:
        if starred:
            _memory_store["stars"].add(space_id)
        else:
            _memory_store["stars"].discard(space_id)
        return

    async with _pool.acquire() as conn:
        if starred:
            await conn.execute(
                "INSERT INTO starred_spaces (space_id) VALUES ($1) ON CONFLICT DO NOTHING",
                space_id,
            )
        else:
            await conn.execute(
                "DELETE FROM starred_spaces WHERE space_id = $1",
                space_id,
            )


async def get_starred_spaces() -> list[str]:
    """Get all starred space IDs."""
    if not _lakebase_available or _pool is None:
        return list(_memory_store["stars"])

    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT space_id FROM starred_spaces")
        return [r["space_id"] for r in rows]


async def record_space_seen(space_id: str) -> None:
    """Record that a space has been seen."""
    if not _lakebase_available or _pool is None:
        _memory_store["seen"].add(space_id)
        return

    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO seen_spaces (space_id) VALUES ($1) ON CONFLICT DO NOTHING",
            space_id,
        )


async def get_all_scan_summaries() -> list[dict]:
    """Get latest scan summary for all scanned spaces."""
    if not _lakebase_available or _pool is None:
        return list(_memory_store["scans"].values())

    import json
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT ON (space_id)
                space_id, score, maturity, findings, scanned_at
            FROM scan_results
            ORDER BY space_id, scanned_at DESC
        """)
        return [
            {
                "space_id": r["space_id"],
                "score": r["score"],
                "maturity": r["maturity"],
                "findings": json.loads(r["findings"]),
                "scanned_at": r["scanned_at"].isoformat(),
            }
            for r in rows
        ]
