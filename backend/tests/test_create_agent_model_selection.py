import asyncio
import json
import time

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import create
from backend.services import create_agent_session
from backend.services.create_agent_session import AgentSession


def test_agent_chat_request_model_is_saved_on_session(monkeypatch):
    captured: dict[str, object] = {}

    class FakeAgent:
        async def chat(self, session, user_message, selections=None, vc_capture=None):
            captured["session"] = session
            captured["model"] = session.llm_model
            captured["message"] = user_message
            yield {"event": "done", "data": {"needs_continuation": False}}

    monkeypatch.setattr("backend.services.create_agent.get_create_agent", lambda: FakeAgent())
    monkeypatch.setattr("backend.services.model_catalog.validate_chat_model", lambda model, client=None: model)
    monkeypatch.setattr("backend.services.auth.get_workspace_client", lambda: object())
    monkeypatch.setattr(create, "persist_session", lambda session: None, raising=False)

    app = FastAPI()
    app.include_router(create.router)

    with TestClient(app) as client:
        resp = client.post(
            "/api/create/agent/chat",
            json={"message": "build a space", "model": "custom-chat"},
        )

    assert resp.status_code == 200, resp.text
    assert "event: done" in resp.text
    assert captured["model"] == "custom-chat"
    assert isinstance(captured["session"], AgentSession)


def test_agent_session_persistence_round_trips_llm_model(monkeypatch):
    class FakeConn:
        def __init__(self):
            self.execute_calls = []

        async def execute(self, sql, *args):
            self.execute_calls.append((sql, args))

        async def fetchrow(self, sql, session_id):
            now = time.time()
            return {
                "session_id": session_id,
                "history": json.dumps([]),
                "space_config": None,
                "space_id": None,
                "space_url": None,
                "selected_catalogs": json.dumps([]),
                "selected_schemas": json.dumps([]),
                "selected_tables": json.dumps([]),
                "feasibility_confirmed": False,
                "llm_model": "persisted-chat",
                "created_at": now,
                "last_active": now,
            }

    class Acquire:
        def __init__(self, conn):
            self.conn = conn

        async def __aenter__(self):
            return self.conn

        async def __aexit__(self, *args):
            return False

    class FakePool:
        def __init__(self, conn):
            self.conn = conn

        def acquire(self):
            return Acquire(self.conn)

    conn = FakeConn()
    monkeypatch.setattr(create_agent_session, "_get_pool", lambda: FakePool(conn))

    async def run():
        await create_agent_session._ensure_table()
        session = AgentSession(session_id="s1", llm_model="persisted-chat")
        await create_agent_session._persist(session)
        return await create_agent_session._load("s1")

    loaded = asyncio.run(run())

    assert any("llm_model" in sql for sql, _ in conn.execute_calls)
    assert any(args and "persisted-chat" in args for _, args in conn.execute_calls)
    assert loaded is not None
    assert loaded.llm_model == "persisted-chat"
