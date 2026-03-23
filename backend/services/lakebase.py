"""Lakebase (PostgreSQL) persistence for Genie Space scan results."""

import asyncio
import json
import logging
import os
import time
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)

# In-memory fallback store (used when Lakebase is unavailable)
_memory_store: dict = {
    "scans": {},      # space_id -> latest ScanResult dict
    "history": {},    # space_id -> list of ScanResult dicts (ordered by timestamp)
    "stars": set(),   # set of starred space_ids
    "seen": set(),    # set of seen space_ids
    "optimization_runs": {},  # space_id -> latest optimization run dict
}

_pool = None
_lakebase_available = False
_schema_retry_after: float = 0  # timestamp after which we retry schema creation


def _generate_credential() -> tuple[str, str] | None:
    """Generate Lakebase credentials via Databricks SDK.

    The SP has an OAuth role in Lakebase. Its OAuth access token works as
    the postgres password (same pattern as the Lakebase UI's OAuth connection).
    The username is the SP's application_id (client_id).
    """
    try:
        from backend.services.auth import get_service_principal_client
        client = get_service_principal_client()

        # The SP's OAuth token works as the postgres password.
        # Generate an OAuth token directly using client_credentials grant.
        token = None
        client_id = client.config.client_id or os.environ.get("DATABRICKS_CLIENT_ID", "")
        client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")  # gitleaks:allow
        host = (client.config.host or os.environ.get("DATABRICKS_HOST", "")).rstrip("/")

        if client_id and client_secret and host:
            import urllib.request
            import urllib.parse
            token_url = f"{host}/oidc/v1/token"
            data = urllib.parse.urlencode({
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,  # gitleaks:allow
                "scope": "iam.current-user:read iam.groups:read iam.service-principals:read iam.users:read",
            }).encode()
            try:
                req = urllib.request.Request(token_url, data=data,
                    headers={"Content-Type": "application/x-www-form-urlencoded"})
                with urllib.request.urlopen(req, timeout=10) as resp:
                    token_resp = json.loads(resp.read())
                    token = token_resp.get("access_token")
                if token:
                    logger.info(f"Generated OAuth token for Lakebase (client_id={client_id[:8]}...)")
            except Exception as e:
                logger.debug(f"OAuth token generation failed: {e}")

        if not token:
            logger.info("Could not obtain SP OAuth token for Lakebase")
            return None

        # Username is the SP's client_id (application_id)
        user = client.config.client_id or os.environ.get("DATABRICKS_CLIENT_ID", "")
        if not user:
            try:
                me = client.current_user.me()
                user = me.user_name or ""
            except Exception:
                pass

        if not user:
            logger.info("Could not determine SP username for Lakebase")
            return None

        logger.info(f"Using SP OAuth token for Lakebase (user={user[:8]}...)")
        return user, token
    except Exception as e:
        logger.warning(f"Lakebase credential generation failed: {e}")
        return None


async def _ensure_schema():
    """Idempotently create all Lakebase tables and indexes.

    Safe to call on every startup — uses CREATE TABLE IF NOT EXISTS.
    On failure, marks Lakebase unavailable and schedules a retry so the
    app self-heals once Lakebase permissions are fixed (e.g. resource attached).
    """
    global _lakebase_available, _schema_retry_after
    if _pool is None:
        return
    try:
        async with _pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS scan_results (
                    id          SERIAL PRIMARY KEY,
                    space_id    VARCHAR(64) NOT NULL,
                    score       INTEGER     NOT NULL CHECK (score >= 0 AND score <= 100),
                    maturity    VARCHAR(32) NOT NULL,
                    breakdown   JSONB       NOT NULL DEFAULT '{}',
                    findings    JSONB       NOT NULL DEFAULT '[]',
                    next_steps  JSONB       NOT NULL DEFAULT '[]',
                    scanned_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                    UNIQUE (space_id, scanned_at)
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scan_results_space_id ON scan_results(space_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scan_results_scanned_at ON scan_results(scanned_at DESC)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scan_results_score ON scan_results(score)"
            )
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS starred_spaces (
                    space_id   VARCHAR(64) PRIMARY KEY,
                    starred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS seen_spaces (
                    space_id   VARCHAR(64) PRIMARY KEY,
                    first_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """)
        _lakebase_available = True
        logger.info("Lakebase schema ready (3 tables)")
    except Exception as e:
        logger.warning(f"Failed to ensure Lakebase schema: {e}. Falling back to in-memory storage.")
        _lakebase_available = False
        _schema_retry_after = time.monotonic() + 30  # retry after 30 seconds


async def _maybe_retry_schema():
    """If pool exists but schema failed, retry periodically (e.g. after Lakebase resource is attached)."""
    global _schema_retry_after
    if _lakebase_available or _pool is None:
        return
    if time.monotonic() < _schema_retry_after:
        return
    _schema_retry_after = time.monotonic() + 30  # prevent thundering herd
    logger.info("Retrying Lakebase schema creation...")
    await _ensure_schema()


async def init_pool():
    """Initialize asyncpg connection pool. Falls back gracefully if unavailable.

    When Lakebase is connected via the Databricks Apps UI, the platform injects
    LAKEBASE_HOST and LAKEBASE_PASSWORD as environment variables. Without these,
    the app uses in-memory storage (ephemeral per deployment).
    """
    global _pool, _lakebase_available

    host = os.environ.get("LAKEBASE_HOST")
    if not host:
        logger.info("LAKEBASE_HOST not set - using in-memory fallback. "
                     "Connect Lakebase via the Databricks Apps UI for persistent storage.")
        return

    # valueFrom: postgres injects the endpoint resource path, not the hostname.
    # Resolve the actual hostname from the Lakebase API.
    if host.startswith("projects/"):
        logger.info(f"LAKEBASE_HOST is a resource path ({host}), resolving hostname...")
        try:
            from backend.services.auth import get_service_principal_client
            sp = get_service_principal_client()
            # Extract the endpoint path up to /endpoints/...
            # Format: projects/{project}/branches/{branch}/endpoints/{endpoint}
            parts = host.split("/")
            # Find the endpoints section and construct the parent path
            ep_idx = parts.index("endpoints") if "endpoints" in parts else -1
            if ep_idx >= 0:
                endpoints_path = "/".join(parts[:ep_idx + 2])  # up to endpoints/{name}
                parent_path = "/".join(parts[:ep_idx])  # branches path
                resp = sp.api_client.do("GET", f"/api/2.0/postgres/{parent_path}/endpoints")
                endpoints = resp.get("endpoints", [])
                for ep in endpoints:
                    if ep.get("name", "").startswith(endpoints_path) or ep.get("name") == host:
                        hosts = ep.get("status", {}).get("hosts", {})
                        # Use the non-pooled host — the pooled host doesn't support OAuth auth
                        resolved = hosts.get("host", "")
                        if resolved:
                            logger.info(f"Resolved Lakebase host: {resolved}")
                            host = resolved
                            break
        except Exception as e:
            logger.warning(f"Could not resolve Lakebase host from resource path: {e}")
            return

    password = os.environ.get("LAKEBASE_PASSWORD")
    user = os.environ.get("LAKEBASE_USER", "postgres")

    if not password:
        # Try generating credentials via Databricks SDK
        cred = _generate_credential()
        if cred:
            user, password = cred
        else:
            logger.warning("LAKEBASE_HOST is set but no password available - using in-memory fallback. "
                           "Ensure the Lakebase postgres resource is properly connected in the Apps UI.")
            return

    logger.info(f"Connecting to Lakebase: host={host}, user={user[:12]}..., port={os.environ.get('LAKEBASE_PORT', '5432')}, db={os.environ.get('LAKEBASE_DATABASE', 'databricks_postgres')}, password_len={len(password) if password else 0}")
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
        await _ensure_schema()
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
            json.dumps({"optimization_accuracy": scan_result.get("optimization_accuracy")}),
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
        extra = json.loads(row["breakdown"])
        return {
            "score": row["score"],
            "total": 12,
            "maturity": row["maturity"],
            "optimization_accuracy": extra.get("optimization_accuracy"),
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
    await _maybe_retry_schema()
    if not _lakebase_available or _pool is None:
        return list(_memory_store["stars"])

    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT space_id FROM starred_spaces")
        return [r["space_id"] for r in rows]


async def is_space_starred(space_id: str) -> bool:
    """Check if a single space is starred (O(1) vs fetching all)."""
    if not _lakebase_available or _pool is None:
        return space_id in _memory_store["stars"]

    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM starred_spaces WHERE space_id = $1", space_id
        )
        return row is not None


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
    await _maybe_retry_schema()
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


async def save_optimization_run(space_id: str, benchmark_total: int, benchmark_correct: int) -> None:
    """Save an optimization run result.

    Called when the user completes the optimization workflow (labeling + suggestions).
    """
    accuracy = benchmark_correct / benchmark_total if benchmark_total > 0 else 0.0
    run = {
        "space_id": space_id,
        "benchmark_total": benchmark_total,
        "benchmark_correct": benchmark_correct,
        "accuracy": accuracy,
        "created_at": datetime.utcnow().isoformat(),
    }

    if not _lakebase_available or _pool is None:
        _memory_store["optimization_runs"][space_id] = run
        return

    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO optimization_runs (space_id, benchmark_total, benchmark_correct, accuracy)
            VALUES ($1, $2, $3, $4)
        """, space_id, benchmark_total, benchmark_correct, accuracy)


async def get_latest_optimization_run(space_id: str) -> Optional[dict]:
    """Get the latest optimization run for a space.

    Returns dict with ``accuracy`` (float 0-1) and ``created_at``, or None.
    """
    if not _lakebase_available or _pool is None:
        return _memory_store["optimization_runs"].get(space_id)

    async with _pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT accuracy, created_at
            FROM optimization_runs
            WHERE space_id = $1
            ORDER BY created_at DESC
            LIMIT 1
        """, space_id)
        if not row:
            return None
        return {
            "accuracy": float(row["accuracy"]),
            "created_at": row["created_at"].isoformat(),
        }
