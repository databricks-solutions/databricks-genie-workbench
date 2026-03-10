"""Shared Lakebase (PostgreSQL) connection pool management.

Each agent initializes its own pool from its own app.yaml env vars
(LAKEBASE_HOST, LAKEBASE_INSTANCE_NAME, etc.). Schema migrations are
idempotent (IF NOT EXISTS) so agents can boot in any order.

Domain-specific query functions (save_scan_result, get_score_history, etc.)
stay in each agent's own module — this shared client only manages the pool
lifecycle, credential generation, and DDL.

Source: backend/services/lakebase.py (269 lines)
"""

from __future__ import annotations

import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)

_pool = None
_lakebase_available = False

# In-memory fallback (same pattern as backend/services/lakebase.py:12-17)
_memory_store: dict = {
    "scans": {},
    "history": {},
    "stars": set(),
    "seen": set(),
    "sessions": {},
}


# ── DDL statements per agent (all use IF NOT EXISTS) ──────────────────────────

SCORER_DDL = [
    """CREATE TABLE IF NOT EXISTS scan_results (
        space_id TEXT NOT NULL,
        score INTEGER NOT NULL,
        maturity TEXT,
        breakdown JSONB,
        findings JSONB,
        next_steps JSONB,
        scanned_at TIMESTAMPTZ NOT NULL,
        UNIQUE (space_id, scanned_at)
    )""",
    "CREATE TABLE IF NOT EXISTS starred_spaces (space_id TEXT PRIMARY KEY)",
    "CREATE TABLE IF NOT EXISTS seen_spaces (space_id TEXT PRIMARY KEY)",
]

CREATOR_DDL = [
    """CREATE TABLE IF NOT EXISTS agent_sessions (
        session_id TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        updated_at TIMESTAMPTZ DEFAULT NOW()
    )""",
]


# ── Credential generation (mirrors backend/services/lakebase.py:23-59) ────────

def _generate_lakebase_credential() -> tuple[str, str] | None:
    """Generate Lakebase OAuth credentials using the Databricks SDK."""
    instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME")
    if not instance_name:
        return None

    try:
        from agents._shared.auth_bridge import get_service_principal_client

        client = get_service_principal_client()
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

        user = os.environ.get("LAKEBASE_USER")
        if not user:
            try:
                me = client.current_user.me()
                user = me.user_name
            except Exception:
                user = "databricks"

        logger.info("Generated Lakebase credential via SDK (user=%s)", user)
        return user, token
    except Exception as e:
        logger.warning("Lakebase credential generation failed: %s", e)
        return None


# ── Pool lifecycle ────────────────────────────────────────────────────────────

async def init_pool(ddl_statements: Optional[list[str]] = None):
    """Initialize asyncpg pool and run idempotent DDL.

    Call this at agent startup (e.g., in a FastAPI lifespan handler).

    Args:
        ddl_statements: SQL DDL to execute after connecting.
            Use SCORER_DDL, CREATOR_DDL, or combine them.
    """
    global _pool, _lakebase_available

    host = os.environ.get("LAKEBASE_HOST")
    if not host:
        logger.info("LAKEBASE_HOST not set — using in-memory fallback")
        return

    password = os.environ.get("LAKEBASE_PASSWORD")
    user = os.environ.get("LAKEBASE_USER", "postgres")

    if not password:
        cred = _generate_lakebase_credential()
        if cred:
            user, password = cred
        else:
            logger.warning(
                "No LAKEBASE_PASSWORD and credential generation failed "
                "— using in-memory fallback"
            )
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

        # Run idempotent DDL
        if ddl_statements and _pool:
            async with _pool.acquire() as conn:
                for ddl in ddl_statements:
                    await conn.execute(ddl)
            logger.info("Executed %d DDL statements", len(ddl_statements))

    except Exception as e:
        logger.warning("Lakebase unavailable: %s. Using in-memory fallback.", e)
        _lakebase_available = False


async def close_pool():
    """Close the connection pool. Call at agent shutdown."""
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


async def get_pool():
    """Get the connection pool (or None if using in-memory fallback)."""
    return _pool


def is_available() -> bool:
    """Check if Lakebase is connected."""
    return _lakebase_available


def get_memory_store() -> dict:
    """Get the in-memory fallback store (for when Lakebase is unavailable)."""
    return _memory_store
