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
    monkeypatch.setattr(
        mv_advisor, "advise_from_corpus",
        lambda **k: calls.append("advise") or SimpleNamespace(
            status="SKIPPED", skip_reason="no_candidates", error=None
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
    monkeypatch.setattr(mv_advisor, "space_corpus_entries", lambda cfg: ())
    monkeypatch.setattr(mv_advisor, "estate_metric_view_yamls", lambda *a, **k: {})

    def _capture(**k):
        captured.update(k)
        return SimpleNamespace(status="SKIPPED", skip_reason="no_candidates", error=None)

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
