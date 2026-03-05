"""Session management for the Create Genie agent.

Maintains conversation history and accumulated state per session.
Persists to Lakebase (PostgreSQL) when available, falls back to in-memory.
"""

import json
import time
import logging
import secrets
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

SESSION_TTL_SECONDS = 3600  # 1 hour
MAX_SESSIONS = 50


@dataclass
class AgentSession:
    session_id: str
    history: list[dict] = field(default_factory=list)
    created_at: float = field(default_factory=time.time)
    last_active: float = field(default_factory=time.time)
    space_config: dict | None = None
    space_id: str | None = None
    space_url: str | None = None

    def add_message(self, role: str, content: str) -> None:
        self.history.append({"role": role, "content": content})
        self.last_active = time.time()

    def add_tool_call(self, tool_call_id: str, name: str, arguments: str) -> None:
        self.history.append({
            "role": "assistant",
            "content": "",
            "tool_calls": [{
                "id": tool_call_id,
                "type": "function",
                "function": {"name": name, "arguments": arguments},
            }],
        })
        self.last_active = time.time()

    def add_tool_result(self, tool_call_id: str, content: str) -> None:
        self.history.append({
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": content,
        })
        self.last_active = time.time()

    def is_expired(self) -> bool:
        return (time.time() - self.last_active) > SESSION_TTL_SECONDS


# ---------------------------------------------------------------------------
# In-memory cache (always used as L1; Lakebase is the durable L2)
# ---------------------------------------------------------------------------
_sessions: dict[str, AgentSession] = {}


def _get_pool():
    """Return the shared Lakebase connection pool (if available)."""
    from backend.services.lakebase import _pool, _lakebase_available
    if _lakebase_available and _pool is not None:
        return _pool
    return None


async def _ensure_table() -> None:
    """Idempotently create the agent_sessions table in Lakebase."""
    pool = _get_pool()
    if pool is None:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS agent_sessions (
                    session_id   TEXT PRIMARY KEY,
                    history      JSONB NOT NULL DEFAULT '[]'::jsonb,
                    space_config JSONB,
                    space_id     TEXT,
                    space_url    TEXT,
                    created_at   DOUBLE PRECISION NOT NULL,
                    last_active  DOUBLE PRECISION NOT NULL
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_agent_sessions_last_active
                ON agent_sessions (last_active)
            """)
        logger.info("agent_sessions table ready")
    except Exception as e:
        logger.warning(f"Failed to ensure agent_sessions table: {e}")


async def _persist(session: AgentSession) -> None:
    """Write session state to Lakebase (fire-and-forget safe)."""
    pool = _get_pool()
    if pool is None:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO agent_sessions
                    (session_id, history, space_config, space_id, space_url, created_at, last_active)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT (session_id) DO UPDATE SET
                    history      = EXCLUDED.history,
                    space_config = EXCLUDED.space_config,
                    space_id     = EXCLUDED.space_id,
                    space_url    = EXCLUDED.space_url,
                    last_active  = EXCLUDED.last_active
            """,
                session.session_id,
                json.dumps(session.history),
                json.dumps(session.space_config) if session.space_config else None,
                session.space_id,
                session.space_url,
                session.created_at,
                session.last_active,
            )
    except Exception as e:
        logger.warning(f"Failed to persist session {session.session_id}: {e}")


async def _load(session_id: str) -> AgentSession | None:
    """Try to load a session from Lakebase."""
    pool = _get_pool()
    if pool is None:
        return None
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM agent_sessions WHERE session_id = $1",
                session_id,
            )
        if row is None:
            return None
        session = AgentSession(
            session_id=row["session_id"],
            history=json.loads(row["history"]),
            created_at=row["created_at"],
            last_active=row["last_active"],
            space_config=json.loads(row["space_config"]) if row["space_config"] else None,
            space_id=row["space_id"],
            space_url=row["space_url"],
        )
        if session.is_expired():
            return None
        return session
    except Exception as e:
        logger.warning(f"Failed to load session {session_id} from Lakebase: {e}")
        return None


# ---------------------------------------------------------------------------
# Public API (sync wrappers — the router can await the async variants)
# ---------------------------------------------------------------------------

def create_session() -> AgentSession:
    """Create a new agent session (in-memory). Call persist_session() after."""
    _evict_expired()
    session_id = secrets.token_hex(16)
    session = AgentSession(session_id=session_id)
    _sessions[session_id] = session
    logger.info(f"Created agent session {session_id}")
    return session


def get_session(session_id: str) -> AgentSession | None:
    """Get session from in-memory cache."""
    session = _sessions.get(session_id)
    if session is None:
        return None
    if session.is_expired():
        _sessions.pop(session_id, None)
        return None
    session.last_active = time.time()
    return session


async def get_session_async(session_id: str) -> AgentSession | None:
    """Get session: tries in-memory first, falls back to Lakebase."""
    session = get_session(session_id)
    if session is not None:
        return session
    # L2: Lakebase
    session = await _load(session_id)
    if session is not None:
        session.last_active = time.time()
        _sessions[session_id] = session  # promote to L1
        logger.info(f"Restored session {session_id} from Lakebase")
    return session


async def persist_session(session: AgentSession) -> None:
    """Persist session to Lakebase (non-blocking best-effort)."""
    await _persist(session)


def delete_session(session_id: str) -> bool:
    """Delete a session from in-memory cache. Returns True if it existed."""
    return _sessions.pop(session_id, None) is not None


async def delete_session_async(session_id: str) -> bool:
    """Delete from both in-memory and Lakebase."""
    existed = _sessions.pop(session_id, None) is not None
    pool = _get_pool()
    if pool:
        try:
            async with pool.acquire() as conn:
                result = await conn.execute(
                    "DELETE FROM agent_sessions WHERE session_id = $1",
                    session_id,
                )
                if "DELETE 1" in result:
                    existed = True
        except Exception as e:
            logger.warning(f"Failed to delete session {session_id} from Lakebase: {e}")
    return existed


def _evict_expired() -> None:
    """Remove expired sessions from in-memory cache and enforce max count."""
    expired = [sid for sid, s in _sessions.items() if s.is_expired()]
    for sid in expired:
        _sessions.pop(sid, None)

    if len(_sessions) >= MAX_SESSIONS:
        oldest = sorted(_sessions.items(), key=lambda x: x[1].last_active)
        for sid, _ in oldest[:len(_sessions) - MAX_SESSIONS + 1]:
            _sessions.pop(sid, None)
