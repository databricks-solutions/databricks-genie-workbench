"""Lakebase (PostgreSQL) persistence for Genie Workbench.

Holds both the workbench's scan/star/optimization tables and the GenieWatch
observability caches. Single asyncpg pool, single `genie.*` schema, single
in-memory fallback. Watch tables are prefixed `watch_` to keep ownership clear.
"""

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
    "join_advice": {},  # space_id -> {seeds, updated_at, seeded_by} (Join Advisor advice)
    # ── GenieWatch caches (read-only observability surface) ──
    "watch_space_cache": {},        # space_id -> dict
    "watch_conversation_cache": {}, # (space_id, conversation_id) -> dict
    "watch_message_cache": {},      # (space_id, conversation_id, message_id) -> dict
    "watch_sync_watermark": {},     # resource -> dict
    "watch_daily_rollup": {},       # (space_id, day) -> dict
    # ── Ontology (read-only estate surface) — durable settings only ──
    "ont_settings": {},             # workspace_id -> {company_name, catalog_allowlist, updated_at}
}

_pool = None
_lakebase_available = False
_schema_retry_after: float = 0  # timestamp after which we retry schema creation
_token_refresh_task: asyncio.Task | None = None
_current_token: str | None = None
_lakebase_instance_name: str | None = None
_lakebase_autoscaling_endpoint: str | None = None  # autoscaling endpoint path for credential generation
_lakebase_project_name: str | None = None  # extracted once from endpoint path for logging
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
            label = f"autoscaling endpoint '{_lakebase_project_name}'"
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

    On Lakebase Autoscaling, the SP must have a Postgres role created via
    the SDK (setup_lakebase.py) with CONNECT + CREATE ON DATABASE grants.
    """
    global _lakebase_available, _schema_retry_after
    if _pool is None:
        return
    try:
        async with _pool.acquire() as conn:
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
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.hidden_optimization_runs (
                    run_id      VARCHAR(36) PRIMARY KEY,
                    space_id    VARCHAR(128) NOT NULL,
                    hidden_by   TEXT NOT NULL,
                    hidden_at   TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_hidden_optimization_runs_space_id "
                "ON genie.hidden_optimization_runs(space_id)"
            )
            # Join Advisor advice (Semantic Blueprint v4 §7). One row per space:
            # the pending set of operator-seeded candidate joins carried into the
            # next Auto-Optimize run as ADVICE (never a declared join_spec — the
            # Workbench makes no ad-hoc Genie Agent config edits). seeds_json is a
            # JSON array of JoinCandidate dicts.
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.join_advice (
                    space_id    VARCHAR(128) PRIMARY KEY,
                    seeds_json  TEXT NOT NULL,
                    seeded_by   TEXT,
                    updated_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                )
            """)

            # ── GenieWatch tables (read-only observability) ──
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.watch_space_cache (
                    space_id     VARCHAR(64) PRIMARY KEY,
                    title        TEXT,
                    owner_email  TEXT,
                    description  TEXT,
                    permissions  JSONB,
                    last_seen_at TIMESTAMPTZ NOT NULL,
                    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_watch_space_cache_owner ON genie.watch_space_cache(owner_email)"
            )
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.watch_conversation_cache (
                    space_id        VARCHAR(64) NOT NULL,
                    conversation_id VARCHAR(64) NOT NULL,
                    user_email      TEXT,
                    created_at      TIMESTAMPTZ,
                    message_count   INT DEFAULT 0,
                    last_message_at TIMESTAMPTZ,
                    PRIMARY KEY (space_id, conversation_id)
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_watch_conv_last ON genie.watch_conversation_cache(last_message_at DESC)"
            )
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.watch_message_cache (
                    space_id        VARCHAR(64) NOT NULL,
                    conversation_id VARCHAR(64) NOT NULL,
                    message_id      VARCHAR(64) NOT NULL,
                    user_email      TEXT,
                    created_at      TIMESTAMPTZ,
                    status          TEXT,
                    has_sql         BOOLEAN,
                    feedback_rating TEXT,
                    PRIMARY KEY (space_id, conversation_id, message_id)
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.watch_sync_watermark (
                    resource       VARCHAR(128) PRIMARY KEY,
                    last_synced_at TIMESTAMPTZ NOT NULL,
                    status         TEXT,
                    error          TEXT
                )
            """)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.watch_daily_usage_rollup (
                    space_id     VARCHAR(64) NOT NULL,
                    day          DATE NOT NULL,
                    queries      INT NOT NULL,
                    approx_dbus  DOUBLE PRECISION,
                    approx_usd   DOUBLE PRECISION,
                    feedback_pos INT NOT NULL DEFAULT 0,
                    feedback_neg INT NOT NULL DEFAULT 0,
                    PRIMARY KEY (space_id, day)
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_watch_rollup_day ON genie.watch_daily_usage_rollup(day DESC)"
            )

            # ── Ontology (Phase 1) — the ONE durable table: settings ──
            # company name + catalog allowlist (MV-D42), one row per workspace/
            # app instance. Everything else the Ontology page reads (governed-tag
            # graph, taxonomy, tags lens) is served live + TTL-cached, never
            # persisted, in Phase 1. The genie_ont_* proposal/mirror/audit tables
            # from architecture §7 are intentionally NOT created here.
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS genie.genie_ont_settings (
                    workspace_id      TEXT PRIMARY KEY,
                    company_name      TEXT,
                    catalog_allowlist JSONB NOT NULL DEFAULT '[]',
                    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            # read_identity (MV-D50): additive, defaulted — old rows read as "obo"
            # (the viewing admin). IF NOT EXISTS keeps this idempotent on restart,
            # matching the CREATE TABLE IF NOT EXISTS discipline above.
            await conn.execute(
                "ALTER TABLE genie.genie_ont_settings "
                "ADD COLUMN IF NOT EXISTS read_identity TEXT NOT NULL DEFAULT 'obo'"
            )
            # Stage 3 curation policy (MV-D57): additive + defaulted, one ADD COLUMN IF
            # NOT EXISTS each (the MV-D50 pattern), so an old row reads the shipped
            # moderate defaults. industry_alignment is STORED + DORMANT (MV-D58, §9 is
            # Phase 4). Idempotent on restart.
            await conn.execute(
                "ALTER TABLE genie.genie_ont_settings "
                "ADD COLUMN IF NOT EXISTS domain_facet_denylist JSONB NOT NULL DEFAULT '[]'"
            )
            await conn.execute(
                "ALTER TABLE genie.genie_ont_settings "
                "ADD COLUMN IF NOT EXISTS domain_min_tables INT NOT NULL DEFAULT 3"
            )
            await conn.execute(
                "ALTER TABLE genie.genie_ont_settings "
                "ADD COLUMN IF NOT EXISTS domain_min_schemas INT NOT NULL DEFAULT 2"
            )
            await conn.execute(
                "ALTER TABLE genie.genie_ont_settings "
                "ADD COLUMN IF NOT EXISTS domain_require_connection BOOLEAN NOT NULL DEFAULT TRUE"
            )
            await conn.execute(
                "ALTER TABLE genie.genie_ont_settings "
                "ADD COLUMN IF NOT EXISTS industry_alignment JSONB NOT NULL "
                "DEFAULT '{\"enabled\": false, \"reference_model\": null}'"
            )
        _lakebase_available = True
        logger.info("Lakebase schema ready (5 workbench tables + 5 watch tables + 1 ontology table)")
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
    global _pool, _lakebase_available, _token_refresh_task, _lakebase_instance_name, _lakebase_autoscaling_endpoint, _lakebase_project_name, _conn_params

    host = os.environ.get("LAKEBASE_HOST")
    if not host:
        logger.info("LAKEBASE_HOST not set - using in-memory fallback. "
                     "Connect Lakebase via the Databricks Apps UI for persistent storage.")
        return

    # Resolve hostname when LAKEBASE_HOST is a resource path (not a DNS name).
    # The Apps platform injects either:
    #   - Autoscaling: "projects/<name>/branches/<branch>/endpoints/<endpoint>"
    #   - Provisioned: a resource reference that doesn't end in ".com"
    if host.startswith("projects/") or "." not in host:
        from backend.services.auth import get_service_principal_client
        client = get_service_principal_client()

        # Try Lakebase Autoscaling first (projects/... path format)
        if host.startswith("projects/"):
            logger.info(f"LAKEBASE_HOST is '{host}', resolving via Lakebase Autoscaling API...")
            _lakebase_autoscaling_endpoint = host  # store for credential generation
            _lakebase_project_name = host.split("/")[1]  # extract once for logging
            try:
                endpoint = client.postgres.get_endpoint(name=host)
                hosts = endpoint.status and endpoint.status.hosts
                resolved = hosts.host if hosts else None
                if resolved:
                    logger.info(f"Resolved Lakebase Autoscaling host: {resolved}")
                    host = resolved
                else:
                    logger.warning("Autoscaling endpoint has no host — endpoint may be stopped or DNS not yet propagated")
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


async def hide_optimization_run_from_history(
    run_id: str,
    space_id: str,
    hidden_by: str,
) -> None:
    """Persist a Workbench-only tombstone for a GSO optimization run.

    The durable GSO Delta audit rows are intentionally left untouched. Unlike
    low-risk read paths, this mutation fails closed when Lakebase is
    unavailable so the API never reports a removal that would be lost on app
    restart.
    """
    await _maybe_retry_schema()
    if not is_available():
        raise RuntimeError("Lakebase is unavailable; history removal was not persisted.")

    assert _pool is not None
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO genie.hidden_optimization_runs
                (run_id, space_id, hidden_by, hidden_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (run_id) DO UPDATE SET
                space_id = EXCLUDED.space_id,
                hidden_by = EXCLUDED.hidden_by,
                hidden_at = NOW()
            """,
            run_id,
            space_id,
            hidden_by or "unknown",
        )


async def get_hidden_optimization_run_ids(space_id: str) -> set[str]:
    """Return run IDs hidden from Workbench history for one Genie Agent.

    Reads fail open so a transient Lakebase issue does not make the entire
    optimization history endpoint unavailable. The write path above remains
    fail closed and never promises ephemeral removal.
    """
    await _maybe_retry_schema()
    if not is_available():
        return set()

    assert _pool is not None
    try:
        async with _pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT run_id
                FROM genie.hidden_optimization_runs
                WHERE space_id = $1
                """,
                space_id,
            )
        return {str(row["run_id"]) for row in rows}
    except Exception:
        logger.warning(
            "Failed to read hidden optimization runs for space %s",
            space_id,
            exc_info=True,
        )
        return set()


# ─────────────────────────────────────────────────────────────────────────
# Join Advisor advice (Semantic Blueprint v4 §7)
# ─────────────────────────────────────────────────────────────────────────


async def save_join_advice(
    space_id: str, seeds: list[dict], seeded_by: str | None = None
) -> dict:
    """Persist the pending Join Advisor advice for a space (upsert; empty clears).

    ``seeds`` is a list of JoinCandidate dicts. This is ADVICE the next
    Auto-Optimize run validates and adds itself — the Workbench never writes it
    into ``serialized_space``. Returns the stored record. Mirrors the write posture
    of ``save_optimization_run`` (best-effort; falls back to the in-memory store).
    """
    updated_at = datetime.utcnow().isoformat()
    record = {"seeds": seeds, "seeded_by": seeded_by, "updated_at": updated_at}

    if not _lakebase_available or _pool is None:
        if seeds:
            _memory_store["join_advice"][space_id] = record
        else:
            _memory_store["join_advice"].pop(space_id, None)
        return record

    async with _pool.acquire() as conn:
        if not seeds:
            await conn.execute("DELETE FROM genie.join_advice WHERE space_id = $1", space_id)
            return record
        await conn.execute(
            """
            INSERT INTO genie.join_advice (space_id, seeds_json, seeded_by, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (space_id) DO UPDATE SET
                seeds_json = EXCLUDED.seeds_json,
                seeded_by = EXCLUDED.seeded_by,
                updated_at = NOW()
            """,
            space_id,
            json.dumps(seeds),
            seeded_by,
        )
    return record


async def get_join_advice(space_id: str) -> Optional[dict]:
    """Read the pending Join Advisor advice for a space, or ``None``.

    Returns ``{"seeds": [...], "seeded_by": str|None, "updated_at": str|None}``.
    Reads fail open (a transient Lakebase issue yields ``None``, i.e. no advice)
    so the semantic-model tab and the trigger never break on it."""
    if not _lakebase_available or _pool is None:
        return _memory_store["join_advice"].get(space_id)

    try:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT seeds_json, seeded_by, updated_at FROM genie.join_advice WHERE space_id = $1",
                space_id,
            )
        if not row:
            return None
        try:
            seeds = json.loads(row["seeds_json"]) or []
        except (ValueError, TypeError):
            seeds = []
        return {
            "seeds": seeds,
            "seeded_by": row["seeded_by"],
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        }
    except Exception:
        logger.warning("Failed to read join advice for space %s", space_id, exc_info=True)
        return None


# ─────────────────────────────────────────────────────────────────────────
# GenieWatch accessors — observability caches & user-set mappings.
# Tables live in the same `genie` schema (prefixed `watch_`) so there's a
# single Lakebase pool + single schema bootstrap.
# ─────────────────────────────────────────────────────────────────────────


def is_available() -> bool:
    """Whether the Lakebase pool is currently usable."""
    return _lakebase_available and _pool is not None


async def watch_upsert_space(space: dict) -> None:
    space_id = space["space_id"]
    if not is_available():
        _memory_store["watch_space_cache"][space_id] = {
            **space,
            "updated_at": datetime.utcnow().isoformat(),
        }
        return
    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO genie.watch_space_cache
                (space_id, title, owner_email, description, permissions, last_seen_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, NOW(), NOW())
            ON CONFLICT (space_id) DO UPDATE SET
                title        = EXCLUDED.title,
                owner_email  = EXCLUDED.owner_email,
                description  = EXCLUDED.description,
                permissions  = EXCLUDED.permissions,
                last_seen_at = NOW(),
                updated_at   = NOW()
        """,
            space_id,
            space.get("title"),
            space.get("owner_email"),
            space.get("description"),
            json.dumps(space.get("permissions") or []),
        )


async def watch_list_cached_spaces() -> list[dict]:
    await _maybe_retry_schema()
    if not is_available():
        return [
            {**s, "permissions": s.get("permissions") or []}
            for s in _memory_store["watch_space_cache"].values()
        ]
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT space_id, title, owner_email, description, permissions,
                   last_seen_at, updated_at
            FROM genie.watch_space_cache
            ORDER BY last_seen_at DESC
        """)
        return [
            {
                "space_id": r["space_id"],
                "title": r["title"],
                "owner_email": r["owner_email"],
                "description": r["description"],
                "permissions": json.loads(r["permissions"]) if r["permissions"] else [],
                "last_seen_at": r["last_seen_at"].isoformat() if r["last_seen_at"] else None,
                "updated_at": r["updated_at"].isoformat() if r["updated_at"] else None,
            }
            for r in rows
        ]


async def watch_upsert_conversation(conv: dict) -> None:
    if not is_available():
        key = (conv["space_id"], conv["conversation_id"])
        _memory_store["watch_conversation_cache"][key] = conv
        return
    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO genie.watch_conversation_cache
                (space_id, conversation_id, user_email, created_at, message_count, last_message_at)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (space_id, conversation_id) DO UPDATE SET
                user_email      = EXCLUDED.user_email,
                message_count   = EXCLUDED.message_count,
                last_message_at = EXCLUDED.last_message_at
        """,
            conv["space_id"], conv["conversation_id"],
            conv.get("user_email"),
            conv.get("created_at"),
            conv.get("message_count") or 0,
            conv.get("last_message_at"),
        )


async def watch_list_conversations(space_id: str, limit: int = 100) -> list[dict]:
    if not is_available():
        out = [
            c for k, c in _memory_store["watch_conversation_cache"].items()
            if k[0] == space_id
        ]
        return sorted(out, key=lambda c: c.get("last_message_at") or "", reverse=True)[:limit]
    async with _pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT conversation_id, user_email, created_at, message_count, last_message_at
            FROM genie.watch_conversation_cache
            WHERE space_id = $1
            ORDER BY last_message_at DESC NULLS LAST
            LIMIT $2
        """, space_id, limit)
        return [
            {
                "conversation_id": r["conversation_id"],
                "user_email": r["user_email"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "message_count": r["message_count"],
                "last_message_at": r["last_message_at"].isoformat() if r["last_message_at"] else None,
            }
            for r in rows
        ]


async def watch_upsert_message(msg: dict) -> None:
    if not is_available():
        key = (msg["space_id"], msg["conversation_id"], msg["message_id"])
        _memory_store["watch_message_cache"][key] = msg
        return
    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO genie.watch_message_cache
                (space_id, conversation_id, message_id, user_email, created_at, status, has_sql, feedback_rating)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (space_id, conversation_id, message_id) DO UPDATE SET
                status          = EXCLUDED.status,
                has_sql         = EXCLUDED.has_sql,
                feedback_rating = EXCLUDED.feedback_rating
        """,
            msg["space_id"], msg["conversation_id"], msg["message_id"],
            msg.get("user_email"),
            msg.get("created_at"),
            msg.get("status"),
            msg.get("has_sql"),
            msg.get("feedback_rating"),
        )


async def watch_get_watermark(resource: str) -> Optional[dict]:
    if not is_available():
        return _memory_store["watch_sync_watermark"].get(resource)
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT resource, last_synced_at, status, error FROM genie.watch_sync_watermark WHERE resource = $1",
            resource,
        )
        if not row:
            return None
        return {
            "resource": row["resource"],
            "last_synced_at": row["last_synced_at"].isoformat() if row["last_synced_at"] else None,
            "status": row["status"],
            "error": row["error"],
        }


async def watch_set_watermark(resource: str, status: str, error: str | None = None) -> None:
    if not is_available():
        _memory_store["watch_sync_watermark"][resource] = {
            "resource": resource,
            "last_synced_at": datetime.utcnow().isoformat(),
            "status": status,
            "error": error,
        }
        return
    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO genie.watch_sync_watermark (resource, last_synced_at, status, error)
            VALUES ($1, NOW(), $2, $3)
            ON CONFLICT (resource) DO UPDATE SET
                last_synced_at = NOW(), status = EXCLUDED.status, error = EXCLUDED.error
        """, resource, status, error)


async def watch_upsert_daily_rollup(row: dict) -> None:
    if not is_available():
        _memory_store["watch_daily_rollup"][(row["space_id"], row["day"])] = row
        return
    async with _pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO genie.watch_daily_usage_rollup
                (space_id, day, queries, approx_dbus, approx_usd, feedback_pos, feedback_neg)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (space_id, day) DO UPDATE SET
                queries      = EXCLUDED.queries,
                approx_dbus  = EXCLUDED.approx_dbus,
                approx_usd   = EXCLUDED.approx_usd,
                feedback_pos = EXCLUDED.feedback_pos,
                feedback_neg = EXCLUDED.feedback_neg
        """,
            row["space_id"], row["day"],
            row.get("queries") or 0,
            row.get("approx_dbus"),
            row.get("approx_usd"),
            row.get("feedback_pos") or 0,
            row.get("feedback_neg") or 0,
        )


# ─────────────────────────────────────────────────────────────────────────
# Ontology settings — company name + catalog allowlist (MV-D42).
# The only durable Ontology state in Phase 1. Reads fail open (a transient
# Lakebase issue yields defaults so the page still renders); the write path
# fails closed so the UI never reports a save that would be lost on restart.
# ─────────────────────────────────────────────────────────────────────────


async def ont_get_settings(workspace_id: str) -> Optional[dict]:
    """Read the Ontology settings row for a workspace, or ``None``."""
    await _maybe_retry_schema()
    if not is_available():
        return _memory_store["ont_settings"].get(workspace_id)

    try:
        async with _pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT company_name, catalog_allowlist, read_identity, "
                "domain_facet_denylist, domain_min_tables, domain_min_schemas, "
                "domain_require_connection, industry_alignment, updated_at "
                "FROM genie.genie_ont_settings WHERE workspace_id = $1",
                workspace_id,
            )
        if not row:
            return None
        try:
            allowlist = json.loads(row["catalog_allowlist"]) or []
        except (ValueError, TypeError):
            allowlist = []
        # Stage 3 config is additive: an old row predates these columns' ALTER — but
        # since the columns carry NOT NULL DEFAULTs, a read returns the defaults, not
        # NULL. Parse defensively so a hand-edited NULL still degrades to the default.
        def _json(val, default):
            if val is None:
                return default
            if isinstance(val, (dict, list)):
                return val
            try:
                return json.loads(val)
            except (ValueError, TypeError):
                return default
        return {
            "company_name": row["company_name"],
            "catalog_allowlist": allowlist,
            "read_identity": row["read_identity"] or "obo",
            "domain_facet_denylist": _json(row["domain_facet_denylist"], None),
            "domain_min_tables": row["domain_min_tables"],
            "domain_min_schemas": row["domain_min_schemas"],
            "domain_require_connection": row["domain_require_connection"],
            "industry_alignment": _json(row["industry_alignment"], None),
            "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
        }
    except Exception:
        logger.warning("Failed to read ontology settings for %s", workspace_id, exc_info=True)
        return None


async def ont_upsert_settings(
    workspace_id: str,
    company_name: str | None,
    catalog_allowlist: list[str],
    read_identity: str = "obo",
    *,
    domain_facet_denylist: list[str] | None = None,
    domain_min_tables: int = 3,
    domain_min_schemas: int = 2,
    domain_require_connection: bool = True,
    industry_alignment: dict | None = None,
) -> dict:
    """Upsert the Ontology settings row for a workspace. Fails closed. The Stage-3
    curation-policy fields are keyword-only + defaulted, so an older caller (positional
    company/allowlist/read_identity only) still writes a valid row."""
    await _maybe_retry_schema()
    denylist = list(domain_facet_denylist or [])
    industry = industry_alignment if isinstance(industry_alignment, dict) else {
        "enabled": False, "reference_model": None
    }
    record = {
        "company_name": company_name,
        "catalog_allowlist": catalog_allowlist,
        "read_identity": read_identity or "obo",
        "domain_facet_denylist": denylist,
        "domain_min_tables": int(domain_min_tables),
        "domain_min_schemas": int(domain_min_schemas),
        "domain_require_connection": bool(domain_require_connection),
        "industry_alignment": industry,
        "updated_at": datetime.utcnow().isoformat(),
    }

    if not is_available():
        _memory_store["ont_settings"][workspace_id] = record
        return record

    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO genie.genie_ont_settings
                (workspace_id, company_name, catalog_allowlist, read_identity,
                 domain_facet_denylist, domain_min_tables, domain_min_schemas,
                 domain_require_connection, industry_alignment, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW())
            ON CONFLICT (workspace_id) DO UPDATE SET
                company_name             = EXCLUDED.company_name,
                catalog_allowlist        = EXCLUDED.catalog_allowlist,
                read_identity            = EXCLUDED.read_identity,
                domain_facet_denylist    = EXCLUDED.domain_facet_denylist,
                domain_min_tables        = EXCLUDED.domain_min_tables,
                domain_min_schemas       = EXCLUDED.domain_min_schemas,
                domain_require_connection = EXCLUDED.domain_require_connection,
                industry_alignment       = EXCLUDED.industry_alignment,
                updated_at               = NOW()
            """,
            workspace_id,
            company_name,
            json.dumps(catalog_allowlist),
            record["read_identity"],
            json.dumps(denylist),
            record["domain_min_tables"],
            record["domain_min_schemas"],
            record["domain_require_connection"],
            json.dumps(industry),
        )
    return record
