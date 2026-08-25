"""On-demand metric-view advice (MV-D23): the suggest route and service.

Tested at the seam, not end to end (no Databricks). Three things matter:

- **Ordering (item 1, reuse-don't-rebuild):** the shared warehouse bootstrapper
  runs before the first advice INSERT, and the sentinel run is written before
  any candidate persists. This is the one-migration-applicator / one-run-writer
  contract; a regression that inserted before ensuring columns would fail here.
- **Embedding degrades, never stalls (MV-D15):** past its hard timeout the
  embedding client returns empty vectors — the endpoint-unreachable signal —
  rather than blocking the interactive request.
- **The route returns one shape:** the advisor outcome plus proposals in the
  same ``MvProposal`` shape the space-scoped list returns, so the panel mounts
  from this source with no component change.
"""

from __future__ import annotations

import time
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.routers import auto_optimize
from backend.services import mv_suggest
from genie_space_optimizer.common import warehouse
from genie_space_optimizer.optimization import mv_advisor


# ── The hard embedding timeout (MV-D15) ────────────────────────────────────


def test_timeout_embedding_client_passes_through_on_success():
    inner = SimpleNamespace(embed=lambda texts: [[0.1, 0.2] for _ in texts])
    client = mv_suggest._TimeoutEmbeddingClient(inner, timeout_s=5.0)
    assert client.embed(["a", "b"]) == [[0.1, 0.2], [0.1, 0.2]]


def test_timeout_embedding_client_degrades_to_empty_vectors_on_timeout():
    def _slow(texts):
        time.sleep(5.0)
        return [[1.0] for _ in texts]

    client = mv_suggest._TimeoutEmbeddingClient(
        SimpleNamespace(embed=_slow), timeout_s=0.05
    )
    started = time.monotonic()
    out = client.embed(["a", "b"])
    # One empty vector per input — the same shape a live endpoint returns, but
    # empty, which the advisor reads as S-unavailable. And it returned fast.
    assert out == [[], []]
    assert time.monotonic() - started < 2.0


def test_timeout_embedding_client_degrades_on_endpoint_error():
    def _boom(texts):
        raise RuntimeError("endpoint down")

    client = mv_suggest._TimeoutEmbeddingClient(
        SimpleNamespace(embed=_boom), timeout_s=5.0
    )
    assert client.embed(["a"]) == [[]]


# ── The service ordering contract (item 1) ─────────────────────────────────


def test_service_ensures_tables_before_creating_the_advice_run(monkeypatch):
    calls: list[str] = []

    monkeypatch.setattr(
        warehouse, "wh_ensure_optimization_tables",
        lambda *a, **k: calls.append("ensure"),
    )
    monkeypatch.setattr(
        warehouse, "wh_create_advice_run",
        lambda *a, **k: calls.append("advice_run"),
    )
    # The stage rows (STARTED / terminal) are written but not part of the
    # ensure→run→advise ordering assertion, so stub them out of `calls`.
    monkeypatch.setattr(warehouse, "wh_write_stage", lambda *a, **k: None)
    monkeypatch.setattr(
        mv_advisor, "advise_from_corpus",
        lambda **k: calls.append("advise") or SimpleNamespace(
            status="SKIPPED", skip_reason="no_candidates", error=None,
            detail=lambda: {"status": "SKIPPED", "skip_reason": "no_candidates"},
        ),
    )
    monkeypatch.setattr(mv_advisor, "space_corpus_entries", lambda cfg: ())
    monkeypatch.setattr(
        mv_advisor, "estate_metric_view_yamls", lambda *a, **k: {}
    )

    outcome, run_id = mv_suggest.suggest_for_space(
        sp_ws=MagicMock(),
        catalog="main",
        schema="gso",
        warehouse_id="wh1",
        llm_model="m",
        space_id="space-1",
        applied_config={"instructions": {}},
        triggered_by="analyst@example.com",
    )

    # Bootstrap (which applies the run_kind / yaml_text migrations) must precede
    # the advice INSERT, which must precede the advisor's persistence.
    assert calls == ["ensure", "advice_run", "advise"]
    assert outcome.status == "SKIPPED"
    assert run_id


def test_service_injects_the_suppression_reader(monkeypatch):
    """MV-D30 as-implemented (Prompt 15.3): the backend (IQ Scan) caller must
    inject ``read_suppressed_fingerprints`` — the warehouse twin of the ledger
    the reject route writes — or a re-scan would resurface a measure the user
    just rejected. This is the backend half of the both-callers invariant; the
    Spark job half is pinned in test_mv_advisor. Both surfaces must agree about
    what "rejected" means."""
    captured: dict = {}

    monkeypatch.setattr(warehouse, "wh_ensure_optimization_tables", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_create_advice_run", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_write_stage", lambda *a, **k: None)
    monkeypatch.setattr(mv_advisor, "space_corpus_entries", lambda cfg: ())
    monkeypatch.setattr(mv_advisor, "estate_metric_view_yamls", lambda *a, **k: {})

    def _capture(**k):
        captured.update(k)
        return SimpleNamespace(
            status="SKIPPED", skip_reason="no_candidates", error=None,
            detail=lambda: {"status": "SKIPPED", "skip_reason": "no_candidates"},
        )

    monkeypatch.setattr(mv_advisor, "advise_from_corpus", _capture)

    mv_suggest.suggest_for_space(
        sp_ws=MagicMock(),
        catalog="main",
        schema="gso",
        warehouse_id="wh1",
        llm_model="m",
        space_id="space-1",
        applied_config={"instructions": {}},
        triggered_by="analyst@example.com",
    )

    reader = captured.get("read_suppressed_fingerprints")
    assert callable(reader), "backend caller must inject the suppression reader"


def test_service_writes_a_started_then_terminal_stage_row(monkeypatch):
    """MV-D31 hydration source: every advice run persists ONE ``genie_opt_stages``
    row (a STARTED then a terminal), the terminal carrying
    ``AdvisorOutcome.detail()`` — so a later mount reads "last scanned + N
    proposals" without re-running. A COMPLETE outcome writes a COMPLETE row; a
    clean SKIP writes a SKIPPED row (the empty/skip state hydrates too)."""
    stages: list[dict] = []

    monkeypatch.setattr(warehouse, "wh_ensure_optimization_tables", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_create_advice_run", lambda *a, **k: None)
    monkeypatch.setattr(mv_advisor, "space_corpus_entries", lambda cfg: ())
    monkeypatch.setattr(mv_advisor, "estate_metric_view_yamls", lambda *a, **k: {})
    monkeypatch.setattr(
        warehouse, "wh_write_stage",
        lambda ws, wh, **k: stages.append(k),
    )
    monkeypatch.setattr(
        mv_advisor, "advise_from_corpus",
        lambda **k: SimpleNamespace(
            status="COMPLETE", skip_reason=None, error=None,
            detail=lambda: {"status": "COMPLETE", "measures_found": 3},
        ),
    )

    mv_suggest.suggest_for_space(
        sp_ws=MagicMock(),
        catalog="main",
        schema="gso",
        warehouse_id="wh1",
        llm_model="m",
        space_id="space-1",
        applied_config={"instructions": {}},
        triggered_by="analyst@example.com",
    )

    assert [s["status"] for s in stages] == ["STARTED", "COMPLETE"]
    assert all(s["stage"] == mv_advisor.MV_ADVISOR_PHASE_NAME for s in stages)
    # The terminal row carries the outcome detail — the hydration payload.
    assert stages[-1]["detail"] == {"status": "COMPLETE", "measures_found": 3}


def test_service_stage_row_marks_a_swallowed_failure_terminal(monkeypatch):
    """A phase exception must still leave a terminal (FAILED) stage row, or
    hydration would read a stuck STARTED forever. The exception re-raises after
    the row is stamped."""
    stages: list[dict] = []

    monkeypatch.setattr(warehouse, "wh_ensure_optimization_tables", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_create_advice_run", lambda *a, **k: None)
    monkeypatch.setattr(mv_advisor, "space_corpus_entries", lambda cfg: ())
    monkeypatch.setattr(mv_advisor, "estate_metric_view_yamls", lambda *a, **k: {})
    monkeypatch.setattr(
        warehouse, "wh_write_stage",
        lambda ws, wh, **k: stages.append(k),
    )

    def _boom(**k):
        raise RuntimeError("advisor exploded")

    monkeypatch.setattr(mv_advisor, "advise_from_corpus", _boom)

    with pytest.raises(RuntimeError):
        mv_suggest.suggest_for_space(
            sp_ws=MagicMock(),
            catalog="main",
            schema="gso",
            warehouse_id="wh1",
            llm_model="m",
            space_id="space-1",
            applied_config={"instructions": {}},
            triggered_by="analyst@example.com",
        )

    assert [s["status"] for s in stages] == ["STARTED", "FAILED"]
    assert stages[-1]["error_message"] == "RuntimeError"


# ── The route ──────────────────────────────────────────────────────────────


@pytest.fixture
def client(monkeypatch) -> TestClient:
    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: MagicMock())
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client", lambda: MagicMock())
    app = FastAPI()
    app.include_router(auto_optimize.router)
    return TestClient(app)


def _row(suggestion_id="sug1"):
    return {
        "suggestion_id": suggestion_id,
        "dedup_fingerprint": "fp1",
        "target_space_id": "space-1",
        "candidate_type": "metric_view",
        "confidence_score": 72.0,
    }


def test_suggest_returns_outcome_and_proposals(client, monkeypatch):
    from genie_space_optimizer.common import genie_client

    monkeypatch.setattr(
        genie_client, "fetch_space_config",
        lambda ws, space_id: {"_parsed_space": {"instructions": {}}},
    )
    monkeypatch.setattr(
        mv_suggest, "suggest_for_space",
        lambda **k: (
            SimpleNamespace(status="COMPLETE", skip_reason=None, error=None),
            "run-adv-1",
        ),
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates", lambda *a, **k: [_row("sug1"), _row("sug2")]
    )

    resp = client.post("/api/auto-optimize/spaces/space-1/mv/suggest")
    assert resp.status_code == 200
    body = resp.json()
    assert body["space_id"] == "space-1"
    assert body["run_id"] == "run-adv-1"
    assert body["status"] == "COMPLETE"
    assert body["skip_reason"] is None
    assert [p["suggestion_id"] for p in body["proposals"]] == ["sug1", "sug2"]


def test_suggest_reports_skip_reason_when_advisor_finds_nothing(client, monkeypatch):
    from genie_space_optimizer.common import genie_client

    monkeypatch.setattr(
        genie_client, "fetch_space_config",
        lambda ws, space_id: {"_parsed_space": {"instructions": {}}},
    )
    monkeypatch.setattr(
        mv_suggest, "suggest_for_space",
        lambda **k: (
            SimpleNamespace(
                status="SKIPPED", skip_reason="no_candidates", error=None, measures_found=0
            ),
            "run-adv-2",
        ),
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])

    resp = client.post("/api/auto-optimize/spaces/space-1/mv/suggest")
    assert resp.status_code == 200
    body = resp.json()
    # EMPTY is distinguishable from a failure: the panel renders "nothing to
    # suggest" from skip_reason, not from an inferred empty list.
    assert body["status"] == "SKIPPED"
    assert body["skip_reason"] == "no_candidates"
    assert body["proposals"] == []


def test_suggest_surfaces_measures_found_for_the_governance_ladder(client, monkeypatch):
    """Prompt 15.3: the panel needs measures_found to tell the two NO_CANDIDATES
    empties apart — 'nothing recurring' (0) vs 'already governed' (> 0). The route
    must pass the advisor's count through, not swallow it."""
    from genie_space_optimizer.common import genie_client

    monkeypatch.setattr(
        genie_client, "fetch_space_config",
        lambda ws, space_id: {"_parsed_space": {"instructions": {}}},
    )
    monkeypatch.setattr(
        mv_suggest, "suggest_for_space",
        lambda **k: (
            SimpleNamespace(
                status="SKIPPED", skip_reason="NO_CANDIDATES", error=None, measures_found=4
            ),
            "run-adv-3",
        ),
    )
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [])

    resp = client.post("/api/auto-optimize/spaces/space-1/mv/suggest")
    assert resp.status_code == 200
    body = resp.json()
    assert body["skip_reason"] == "NO_CANDIDATES"
    assert body["measures_found"] == 4
    assert body["proposals"] == []


# ── The staged-progress stream + OBO/SSE identity trap (MV-D31) ─────────────


def _parse_sse(text: str) -> list[tuple[str, str]]:
    """Split an SSE body into ``(event, data)`` frames (keepalives ignored)."""
    frames: list[tuple[str, str]] = []
    for block in text.split("\n\n"):
        event = data = None
        for line in block.splitlines():
            if line.startswith("event:"):
                event = line[len("event:"):].strip()
            elif line.startswith("data:"):
                data = line[len("data:"):].strip()
        if event is not None:
            frames.append((event, data or ""))
    return frames


def test_stream_sees_the_obo_identity_and_emits_stages_then_result(monkeypatch):
    """MANDATORY (the OBO/SSE trap). ``call_next`` returns before the streaming
    generator runs, so the OBO ContextVar does not propagate into it — the token
    must be re-set from ``request.state`` *inside* the generator (the create.py
    precedent). This drives the stream with the REAL auth functions and the real
    middleware: the identity-bound ``fetch_space_config`` must run under the
    USER's client (token ``user-token``), never the SP (``sp-token``). If the
    re-set were dropped, ``require_obo_workspace_client`` inside the generator
    would raise or the read would fall to the SP — the silent second failure mode
    this branch hunts. The test asserts identity, not merely that stages arrive."""
    import json as _json

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.main import OBOAuthMiddleware
    from backend.services import auth
    from genie_space_optimizer.common import genie_client

    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.cloud.databricks.com")

    # Build a LIGHTWEIGHT client from the config the real set_obo_user_token
    # assembles — the actual SDK Config / WorkspaceClient do offline credential
    # resolution that hangs with no workspace. Keeping the real
    # set_obo_user_token / require_obo_workspace_client / ContextVar path (only
    # the SDK classes are faked) is what makes this an honest OBO-identity test:
    # the token must round-trip through the re-set to reach fetch_space_config.
    monkeypatch.setattr(auth, "Config", lambda **k: SimpleNamespace(**k))
    monkeypatch.setattr(
        auth, "WorkspaceClient", lambda config=None, **k: SimpleNamespace(config=config)
    )

    # The SP client is a sentinel carrying a DISTINCT token, so a read that falls
    # to the SP is caught by identity, not just by "which mock".
    sp_client = SimpleNamespace(config=SimpleNamespace(token="sp-token"))
    monkeypatch.setattr(auth, "_get_default_client", lambda: sp_client)

    captured: dict = {}

    def _fetch(ws, space_id):
        captured["fetch_token"] = getattr(getattr(ws, "config", None), "token", None)
        return {"_parsed_space": {"instructions": {}}}

    monkeypatch.setattr(genie_client, "fetch_space_config", _fetch)

    def _suggest(**k):
        # Drive the on_stage seam from the worker thread, as the advisor does.
        on_stage = k["on_stage"]
        on_stage(mv_advisor.STAGE_READING)
        on_stage(mv_advisor.STAGE_SCANNING)
        on_stage(mv_advisor.STAGE_SCORING)
        on_stage(mv_advisor.STAGE_RENDERING)
        return (
            SimpleNamespace(status="COMPLETE", skip_reason=None, error=None, measures_found=3),
            "run-stream-1",
        )

    monkeypatch.setattr(mv_suggest, "suggest_for_space", _suggest)
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: [_row("sug1")])

    app = FastAPI()
    app.add_middleware(OBOAuthMiddleware)
    app.include_router(auto_optimize.router)
    stream_client = TestClient(app)

    resp = stream_client.post(
        "/api/auto-optimize/spaces/space-1/mv/suggest/stream",
        headers={"x-forwarded-access-token": "user-token"},
    )
    assert resp.status_code == 200
    frames = _parse_sse(resp.text)
    kinds = [e for e, _ in frames]

    # Identity: the config read ran under the USER's token, not the SP's.
    assert captured["fetch_token"] == "user-token"

    # The four honest stages arrived, on entry, in order.
    stages = [_json.loads(d)["stage"] for e, d in frames if e == "stage"]
    assert stages == [
        mv_advisor.STAGE_READING,
        mv_advisor.STAGE_SCANNING,
        mv_advisor.STAGE_SCORING,
        mv_advisor.STAGE_RENDERING,
    ]

    # A final result frame carries the same MvSuggestResponse shape.
    assert "result" in kinds
    result = _json.loads(next(d for e, d in frames if e == "result"))
    assert result["run_id"] == "run-stream-1"
    assert result["status"] == "COMPLETE"
    assert [p["suggestion_id"] for p in result["proposals"]] == ["sug1"]


def test_stream_without_a_user_token_is_a_clean_401(monkeypatch):
    """MV-D20: a suggest stream is bound to the signed-in user. With no OBO token
    the fail-fast check in the handler body (where the ContextVar is still valid)
    returns a clean 401 — not a mid-stream error the client must parse out of the
    event frames."""
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.main import OBOAuthMiddleware

    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")

    app = FastAPI()
    app.add_middleware(OBOAuthMiddleware)
    app.include_router(auto_optimize.router)
    stream_client = TestClient(app)

    resp = stream_client.post("/api/auto-optimize/spaces/space-1/mv/suggest/stream")
    assert resp.status_code == 401
