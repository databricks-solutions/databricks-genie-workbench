"""Lakebase (PostgreSQL) persistence for Genie Space scan results."""

import asyncio
import json
import logging
import os
import time
import uuid
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
_token_refresh_task: asyncio.Task | None = None
_current_token: str | None = None
_lakebase_instance_name: str | None = None
_lakebase_autoscaling_endpoint: str | None = None  # autoscaling endpoint path for credential generation
_conn_params: dict | None = None  # stored at init for pool recreation on token refresh


def _generate_credential() -> tuple[str, str] | None:
    """Generate Lakebase credentials via the Databricks database credential API.

    Supports both provisioned Lakebase (database.generate_database_credential)
    and autoscaling Lakebase (postgres.generate_database_credential).
    Tokens expire after 1 hour.
    """
    global _current_token
    try:
        from backend.services.auth import get_service_principal_client
        client = get_service_principal_client()

        # Autoscaling Lakebase: use postgres API with endpoint path
        if _lakebase_autoscaling_endpoint:
            cred = client.postgres.generate_database_credential(
                endpoint=_lakebase_autoscaling_endpoint,
            )
            label = f"autoscaling endpoint '{_lakebase_autoscaling_endpoint.split('/')[1]}'"
        else:
            # Provisioned Lakebase: use database API with instance name
            instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME", "")
            if not instance_name:
                logger.info("LAKEBASE_INSTANCE_NAME not set — cannot generate database credential")
                return None
            cred = client.database.generate_database_credential(
                request_id=str(uuid.uuid4()),
                instance_names=[instance_name],
            )
            label = f"instance '{instance_name}'"

        token = cred.token
        if not token:
            logger.info("Database credential API returned no token")
            return None

        _current_token = token
        logger.info(f"Generated database credential for Lakebase {label}")

        # Username is the SP's application_id
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

        logger.info(f"Using database credential for Lakebase (user={user[:8]}...)")
        return user, token
    except Exception as e:
        logger.warning(f"Lakebase credential generation failed: {e}")
        return None


async def _token_refresh_loop():
    """Background task to refresh the Lakebase token every 50 minutes (before 1-hour expiry).

    asyncpg pools store the password at creation time with no way to update it.
    We must recreate the pool with fresh credentials so new connections authenticate.
    """
    global _pool, _current_token
    while True:
        await asyncio.sleep(50 * 60)  # 50 minutes
        try:
            cred = _generate_credential()
            if not cred:
                logger.warning("Failed to refresh Lakebase token")
                continue
            user, token = cred
            _current_token = token
            if _conn_params is None or _pool is None:
                continue
            # Recreate pool with fresh credentials
            import asyncpg
            new_pool = await asyncpg.create_pool(
                host=_conn_params["host"],
                port=_conn_params["port"],
                database=_conn_params["database"],
                user=user,
                password=token,
                min_size=2,
                max_size=10,
                command_timeout=30,
                ssl="require",
            )
            old_pool = _pool
            _pool = new_pool
            if old_pool:
                await old_pool.close()
            logger.info("Lakebase token refreshed and pool recreated")
        except Exception as e:
            logger.warning(f"Lakebase token refresh error: {e}")


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
            # Lakebase restricts the default 'public' schema — use a dedicated schema
            await conn.execute("CREATE SCHEMA IF NOT EXISTS genie")
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.scan_results (
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
                "CREATE INDEX IF NOT EXISTS idx_scan_results_space_id ON genie.scan_results(space_id)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scan_results_scanned_at ON genie.scan_results(scanned_at DESC)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_scan_results_score ON genie.scan_results(score)"
            )
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.starred_spaces (
                    space_id   VARCHAR(64) PRIMARY KEY,
                    starred_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.seen_spaces (
                    space_id   VARCHAR(64) PRIMARY KEY,
                    first_seen TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.optimization_runs (
                    id              SERIAL PRIMARY KEY,
                    space_id        VARCHAR(64) NOT NULL,
                    benchmark_total INTEGER NOT NULL,
                    benchmark_correct INTEGER NOT NULL,
                    accuracy        REAL NOT NULL,
                    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_optimization_runs_space_id ON genie.optimization_runs(space_id)"
            )
        _lakebase_available = True
        logger.info("Lakebase schema ready (4 tables)")
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
    global _pool, _lakebase_available, _token_refresh_task, _lakebase_instance_name, _lakebase_autoscaling_endpoint, _conn_params

    host = os.environ.get("LAKEBASE_HOST")
    if not host:
        logger.info("LAKEBASE_HOST not set - using in-memory fallback. "
                     "Connect Lakebase via the Databricks Apps UI for persistent storage.")
        return

    # Resolve hostname when LAKEBASE_HOST is a resource path (not a DNS name).
    # The Apps platform injects either:
    #   - Autoscaling: "projects/<name>/branches/<branch>/endpoints/<endpoint>"
    #   - Provisioned: a resource reference that doesn't end in ".com"
    if host.startswith("projects/") or not host.endswith(".com"):
        from backend.services.auth import get_service_principal_client
        client = get_service_principal_client()

        # Try Lakebase Autoscaling first (projects/... path format)
        if host.startswith("projects/"):
            logger.info(f"LAKEBASE_HOST is '{host}', resolving via Lakebase Autoscaling API...")
            _lakebase_autoscaling_endpoint = host  # store for credential generation
            try:
                endpoint = client.postgres.get_endpoint(name=host)
                resolved = endpoint.status and endpoint.status.hosts and endpoint.status.hosts.host
                if resolved:
                    logger.info(f"Resolved Lakebase Autoscaling host: {resolved}")
                    host = resolved
                else:
                    logger.warning("Autoscaling endpoint has no host — is it stopped?")
                    return
            except Exception as e:
                logger.warning(f"Could not resolve Lakebase Autoscaling endpoint: {e}")
                return
        else:
            # Provisioned Lakebase: resolve from instance name
            instance_name = os.environ.get("LAKEBASE_INSTANCE_NAME", "")
            if not instance_name:
                logger.warning("LAKEBASE_HOST requires resolution but LAKEBASE_INSTANCE_NAME is not set")
                return
            logger.info(f"LAKEBASE_HOST is '{host}', resolving from instance '{instance_name}'...")
            try:
                instance = client.database.get_database_instance(name=instance_name)
                resolved = instance.read_write_dns
                if resolved:
                    logger.info(f"Resolved Lakebase host: {resolved}")
                    host = resolved
                else:
                    logger.warning("Instance has no read_write_dns — is it stopped?")
                    return
            except Exception as e:
                logger.warning(f"Could not resolve Lakebase host from instance: {e}")
                return

    password = os.environ.get("LAKEBASE_PASSWORD")
    user = os.environ.get("LAKEBASE_USER", "postgres")

    if not password:
        # Generate credentials via Databricks database credential API
        cred = _generate_credential()
        if cred:
            user, password = cred
        else:
            logger.warning("LAKEBASE_HOST is set but no password available - using in-memory fallback. "
                           "Ensure the Lakebase postgres resource is properly connected in the Apps UI.")
            return

    port = int(os.environ.get("LAKEBASE_PORT", "5432"))
    database = os.environ.get("LAKEBASE_DATABASE", "databricks_postgres")
    _conn_params = {"host": host, "port": port, "database": database}

    logger.info(f"Connecting to Lakebase: host={host}, user={user[:12]}..., port={port}, db={database}")
    try:
        import asyncpg
        _pool = await asyncpg.create_pool(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            min_size=2,
            max_size=25,
            command_timeout=30,
            timeout=10,
            ssl="require",
        )
        _lakebase_available = True
        logger.info("Lakebase connection pool initialized")
        await _ensure_schema()

        # Start background token refresh (tokens expire after 1 hour)
        _token_refresh_task = asyncio.create_task(_token_refresh_loop())
    except Exception as e:
        logger.warning(f"Lakebase unavailable: {e}. Using in-memory fallback.")
        _lakebase_available = False


async def close_pool():
    """Close the connection pool and stop token refresh."""
    global _pool, _token_refresh_task
    if _token_refresh_task:
        _token_refresh_task.cancel()
        _token_refresh_task = None
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
            INSERT INTO genie.scan_results (space_id, score, maturity, breakdown, findings, next_steps, scanned_at)
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
            json.dumps({
                "optimization_accuracy": scan_result.get("optimization_accuracy"),
                "checks": scan_result.get("checks", []),
                "warnings": scan_result.get("warnings", []),
                "warning_next_steps": scan_result.get("warning_next_steps", []),
            }),
            json.dumps(scan_result.get("findings", [])),
            json.dumps(scan_result.get("next_steps", [])),
            datetime.fromisoformat(scan_result["scanned_at"]),
        )
        await conn.execute(
            "INSERT INTO genie.seen_spaces (space_id) VALUES ($1) ON CONFLICT DO NOTHING",
            space_id,
        )


def _build_score_dict(row) -> dict:
    """Build a score dict from a scan_results DB row."""
    import json
    extra = json.loads(row["breakdown"])
    return {
        "score": row["score"],
        "total": 12,
        "maturity": row["maturity"],
        "optimization_accuracy": extra.get("optimization_accuracy"),
        "checks": extra.get("checks", []),
        "findings": json.loads(row["findings"]),
        "next_steps": json.loads(row["next_steps"]),
        "warnings": extra.get("warnings", []),
        "warning_next_steps": extra.get("warning_next_steps", []),
        "scanned_at": row["scanned_at"].isoformat(),
    }


async def get_latest_score(space_id: str) -> Optional[dict]:
    """Get the latest scan result for a space."""
    if not _lakebase_available or _pool is None:
        return _memory_store["scans"].get(space_id)

    async with _pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT score, maturity, breakdown, findings, next_steps, scanned_at
            FROM genie.scan_results
            WHERE space_id = $1
            ORDER BY scanned_at DESC
            LIMIT 1
        """, space_id)
        if not row:
            return None
        return _build_score_dict(row)


async def get_latest_scores_batch(space_ids: list[str]) -> dict[str, dict]:
    """Get the latest scan result for multiple spaces in a single query."""
    if not space_ids:
        return {}

    if not _lakebase_available or _pool is None:
        return {
            sid: _memory_store["scans"][sid]
            for sid in space_ids
            if sid in _memory_store["scans"]
        }

    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT ON (space_id)
                space_id, score, maturity, breakdown, findings, next_steps, scanned_at
            FROM genie.scan_results
            WHERE space_id = ANY($1)
            ORDER BY space_id, scanned_at DESC
        """, space_ids)
        return {row["space_id"]: _build_score_dict(row) for row in rows}


async def get_score_history(space_id: str, days: int = 30) -> list[dict]:
    """Get score history for a space over the last N days."""
    if not _lakebase_available or _pool is None:
        return _memory_store["history"].get(space_id, [])

    import json
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT score, maturity, breakdown, scanned_at
            FROM genie.scan_results
            WHERE space_id = $1
              AND scanned_at >= NOW() - $2 * INTERVAL '1 day'
            ORDER BY scanned_at ASC
        """, space_id, days)
        results = []
        for r in rows:
            extra = json.loads(r["breakdown"]) if r["breakdown"] else {}
            results.append({
                "score": r["score"],
                "maturity": r["maturity"],
                "optimization_accuracy": extra.get("optimization_accuracy"),
                "scanned_at": r["scanned_at"].isoformat(),
            })
        return results


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
                "INSERT INTO genie.starred_spaces (space_id) VALUES ($1) ON CONFLICT DO NOTHING",
                space_id,
            )
        else:
            await conn.execute(
                "DELETE FROM genie.starred_spaces WHERE space_id = $1",
                space_id,
            )


async def get_starred_spaces() -> list[str]:
    """Get all starred space IDs."""
    await _maybe_retry_schema()
    if not _lakebase_available or _pool is None:
        return list(_memory_store["stars"])

    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT space_id FROM genie.starred_spaces")
        return [r["space_id"] for r in rows]


async def is_space_starred(space_id: str) -> bool:
    """Check if a single space is starred (O(1) vs fetching all)."""
    if not _lakebase_available or _pool is None:
        return space_id in _memory_store["stars"]

    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM genie.starred_spaces WHERE space_id = $1", space_id
        )
        return row is not None


async def record_space_seen(space_id: str) -> None:
    """Record that a space has been seen."""
    if not _lakebase_available or _pool is None:
        _memory_store["seen"].add(space_id)
        return

    async with _pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO genie.seen_spaces (space_id) VALUES ($1) ON CONFLICT DO NOTHING",
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
            FROM genie.scan_results
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
            INSERT INTO genie.optimization_runs (space_id, benchmark_total, benchmark_correct, accuracy)
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
            FROM genie.optimization_runs
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
