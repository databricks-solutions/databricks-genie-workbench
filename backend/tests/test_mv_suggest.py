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


_SPACE_TABLES_CONFIG = {
    "data_sources": {"tables": [{"identifier": "main.sales.orders"}]},
    "instructions": {},
}


def _capturing_advise(captured: list[dict]):
    def _capture(**k):
        captured.append(k)
        return mv_advisor.AdvisorOutcome(
            status=mv_advisor.STATUS_SKIPPED, skip_reason=mv_advisor.SKIP_NO_CANDIDATES,
        )

    return _capture


def test_both_callers_pass_the_space_tables(monkeypatch):
    """MV-D123 Ruling 4: the in-job phase and the IQ Scan service each give
    ``advise_from_corpus`` a resolver built from the space's ``applied_config``,
    and a rekey writer bound to their own store."""
    captured: list[dict] = []
    rekeyed: list[tuple[str, dict]] = []
    monkeypatch.setattr(mv_advisor, "advise_from_corpus", _capturing_advise(captured))

    monkeypatch.setattr(
        mv_advisor, "load_iteration_zero_corpus",
        lambda *a, **k: mv_advisor.CorpusLoad(
            entries=(("SELECT 1", "q1"),), rows_seen=1, rows_with_sql=1,
            applied_config=_SPACE_TABLES_CONFIG,
        ),
    )
    monkeypatch.setattr(mv_advisor, "curated_corpus_entries", lambda *a, **k: ())
    monkeypatch.setattr(mv_advisor, "write_stage", lambda *a, **k: None)
    monkeypatch.setattr(
        mv_advisor, "rekey_mv_suppressions",
        lambda spark, **k: rekeyed.append(("spark", k["rekeys"])) or [],
    )
    mv_advisor.run_mv_advisor_phase(
        MagicMock(), run_id="r1", space_id="space-1", catalog="main", schema="gso",
        enabled=True,
    )

    monkeypatch.setattr(warehouse, "wh_ensure_optimization_tables", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_create_advice_run", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_write_stage", lambda *a, **k: None)
    monkeypatch.setattr(mv_advisor, "space_corpus_entries", lambda cfg: ())
    monkeypatch.setattr(mv_advisor, "estate_metric_view_yamls", lambda *a, **k: {})
    monkeypatch.setattr(
        warehouse, "wh_rekey_mv_suppressions",
        lambda ws, wid, **k: rekeyed.append(("warehouse", k["rekeys"])) or [],
    )
    mv_suggest.suggest_for_space(
        sp_ws=MagicMock(), catalog="main", schema="gso", warehouse_id="wh1",
        llm_model="m", space_id="space-1", applied_config=_SPACE_TABLES_CONFIG,
        triggered_by="analyst@example.com",
    )

    assert len(captured) == 2
    for kwargs in captured:
        resolver = kwargs["resolver"]
        assert resolver.has_table_list
        assert resolver.resolve("orders") == "main.sales.orders"
        kwargs["rekey_suppressions"]({"1" * 64: "a" * 64})
    assert rekeyed == [("spark", {"1" * 64: "a" * 64}), ("warehouse", {"1" * 64: "a" * 64})]


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


_SUG = {k: f"sug_{k * 12}" for k in "abd"}


def test_suggest_injects_the_kept_names_reader(monkeypatch):
    """MV-D122: the IQ Scan caller keeps a decided or created row's view name the
    same way the in-job advisor does, through a strict ``wh_load_mv_candidates``
    read and ``wh_created_suggestion_ids`` over the undecided rows."""
    captured: dict = {}
    reads: list[dict] = []
    asked: list[dict] = []

    monkeypatch.setattr(warehouse, "wh_ensure_optimization_tables", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_create_advice_run", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_write_stage", lambda *a, **k: None)
    monkeypatch.setattr(mv_advisor, "space_corpus_entries", lambda cfg: ())
    monkeypatch.setattr(mv_advisor, "estate_metric_view_yamls", lambda *a, **k: {})

    def _load(ws, warehouse_id, catalog, schema, **kwargs):
        reads.append({"warehouse_id": warehouse_id, "catalog": catalog, "schema": schema, **kwargs})
        return [
            {"proposed_object": "main.sales.Kept_Metrics", "dedup_fingerprint": "fpA",
             "suggestion_id": _SUG["a"], "decision": "approved"},
            {"proposed_object": "main.sales.open_metrics", "dedup_fingerprint": "fpB",
             "suggestion_id": _SUG["b"], "decision": None},
            {"proposed_object": "main.sales.made_metrics", "dedup_fingerprint": "fpD",
             "suggestion_id": _SUG["d"], "decision": None},
        ]

    def _created(ws, warehouse_id, **kwargs):
        asked.append({"warehouse_id": warehouse_id, **kwargs})
        return {_SUG["d"]}

    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", _load)
    monkeypatch.setattr(warehouse, "wh_created_suggestion_ids", _created)

    def _capture(**k):
        captured.update(k)
        return SimpleNamespace(
            status="SKIPPED", skip_reason="no_candidates", error=None,
            detail=lambda: {"status": "SKIPPED", "skip_reason": "no_candidates"},
        )

    monkeypatch.setattr(mv_advisor, "advise_from_corpus", _capture)

    mv_suggest.suggest_for_space(
        sp_ws=MagicMock(), catalog="main", schema="gso", warehouse_id="wh1",
        llm_model="m", space_id="space-1", applied_config={"instructions": {}},
        triggered_by="analyst@example.com",
    )

    assert captured["read_kept_names"]() == {
        "fpA": "main.sales.Kept_Metrics", "fpD": "main.sales.made_metrics",
    }
    assert reads == [
        {"warehouse_id": "wh1", "catalog": "main", "schema": "gso", "target_space_id": "space-1",
         "include_superseded": True, "strict": True}
    ]
    assert [sorted(a.pop("suggestion_ids")) for a in asked] == [[_SUG["b"], _SUG["d"]]]
    assert asked == [{"warehouse_id": "wh1", "catalog": "main", "schema": "gso"}]


_ORDERS, _REFUNDS = "main.sales.orders", "main.sales.refunds"


@pytest.mark.parametrize("failing", ["candidates", "ledger"])
def test_a_failed_kept_names_read_persists_no_conflict_on_the_iq_scan(
    monkeypatch, caplog, failing,
):
    """F1/F5: the real advisor over two colliding CONFLICT halves; a kept-names read
    that fails persists no CONFLICT proposal, and the warning names the type only."""
    upserts: list[dict] = []
    stages: list[dict] = []
    corpus = [
        (f"SELECT SUM(amount) AS total, region FROM {t} GROUP BY region", f"{t}_{i}")
        for t in (_ORDERS, _REFUNDS) for i in range(8)
    ]
    config = {"instructions": {"example_question_sqls": [
        {"id": f"eq_{i}", "sql": f"SELECT SUM(amount * 2) AS total FROM {t}"}
        for i, t in enumerate((_ORDERS, _REFUNDS))
    ]}}

    def _boom(*_a, **_k):
        raise RuntimeError("secret_literal")

    def _load(ws, warehouse_id, catalog, schema, **kwargs):
        if failing == "candidates" and kwargs.get("strict"):
            _boom()
        return [{"proposed_object": "main.sales.measure_amount_metrics",
                 "dedup_fingerprint": "f" * 32, "suggestion_id": _SUG["a"], "decision": None}]

    from genie_space_optimizer.optimization import mv_scoring, mv_signals

    monkeypatch.setattr(warehouse, "wh_ensure_optimization_tables", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_create_advice_run", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_write_stage", lambda ws, wh, **k: stages.append(k))
    monkeypatch.setattr(warehouse, "wh_upsert_mv_candidate", lambda ws, wh, **k: upserts.append(k))
    monkeypatch.setattr(warehouse, "wh_load_mv_suppressed_fingerprints", lambda *a, **k: set())
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", _load)
    monkeypatch.setattr(
        warehouse, "wh_created_suggestion_ids",
        _boom if failing == "ledger" else (lambda *a, **k: set()),
    )
    monkeypatch.setattr(mv_advisor, "space_corpus_entries", lambda cfg: corpus)
    monkeypatch.setattr(mv_advisor, "estate_metric_view_yamls", lambda *a, **k: {})
    monkeypatch.setattr(
        mv_scoring, "FoundationModelEmbeddingClient",
        lambda ws: SimpleNamespace(embed=lambda texts: [[] for _ in texts]),
    )
    monkeypatch.setattr(mv_signals, "warehouse_reader", lambda *a, **k: None)

    caplog.set_level("DEBUG")
    outcome, _run_id = mv_suggest.suggest_for_space(
        sp_ws=MagicMock(), catalog="main", schema="gso", warehouse_id="wh1",
        llm_model="m", space_id="space-1", applied_config=config,
        triggered_by="analyst@example.com",
    )

    assert outcome.status == "COMPLETE"
    # The corpus holds only the two CONFLICT halves, so nothing may persist.
    assert upserts == []
    assert {f["verdict"] for f in stages[-1]["detail"]["render_failures"]} == {
        mv_advisor.CONFLICT_NAMES_UNREAD,
    }
    assert any(
        r.name.endswith("mv_advisor") and "RuntimeError" in r.getMessage()
        for r in caplog.records
    )
    assert all("secret_literal" not in r.getMessage() for r in caplog.records)
    assert all(not r.exc_info for r in caplog.records)


def test_persist_bundle_fans_out_supersession_to_legacy_members(monkeypatch):
    """MV-D30 as-implemented (Prompt 15.6): when the injected persist callback
    writes a view-grained bundle, it retires any legacy per-measure candidate the
    bundle covers so hydration surfaces the bundle alone — never both grains. The
    member fingerprints ride in ``evidence["measures"][].dedup_fingerprint``."""
    captured: dict = {}
    superseded: dict = {}

    monkeypatch.setattr(warehouse, "wh_ensure_optimization_tables", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_create_advice_run", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_write_stage", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_upsert_mv_candidate", lambda *a, **k: None)
    monkeypatch.setattr(mv_advisor, "space_corpus_entries", lambda cfg: ())
    monkeypatch.setattr(mv_advisor, "estate_metric_view_yamls", lambda *a, **k: {})
    monkeypatch.setattr(
        warehouse, "wh_supersede_legacy_mv_candidates",
        lambda ws, wh, **k: superseded.update(k),
    )

    def _capture(**k):
        captured.update(k)
        return SimpleNamespace(
            status="COMPLETE", skip_reason=None, error=None,
            detail=lambda: {"status": "COMPLETE"},
        )

    monkeypatch.setattr(mv_advisor, "advise_from_corpus", _capture)

    mv_suggest.suggest_for_space(
        sp_ws=MagicMock(), catalog="main", schema="gso", warehouse_id="wh1",
        llm_model="m", space_id="space-1", applied_config={"instructions": {}},
        triggered_by="analyst@example.com",
    )

    persist = captured["persist_proposal"]
    bundle = SimpleNamespace(
        target_space_id="space-1", suggestion_id="bundle", dedup_fingerprint="bfp",
        candidate_type="NEW_METRIC_VIEW", confidence_score=80.0, tier="HIGH",
        proposed_object="main.gso.v", components=SimpleNamespace(to_dict=lambda: {}),
        evidence={"bundle": True, "measures": [
            {"dedup_fingerprint": "fpA"}, {"dedup_fingerprint": "fpB"},
        ]},
        provenance={}, alternatives=[], conflicts=[],
    )
    assert persist(bundle, SimpleNamespace(ok=True, yaml_text="y")) is True
    assert superseded.get("target_space_id") == "space-1"
    assert superseded.get("superseded_by") == "bfp"
    assert sorted(superseded.get("member_fingerprints")) == ["fpA", "fpB"]


def test_persist_single_measure_does_not_supersede(monkeypatch):
    """A non-bundle proposal (legacy/CONFLICT grain) never fans out — supersession
    is a bundle-landing event only."""
    captured: dict = {}
    calls: list = []

    monkeypatch.setattr(warehouse, "wh_ensure_optimization_tables", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_create_advice_run", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_write_stage", lambda *a, **k: None)
    monkeypatch.setattr(warehouse, "wh_upsert_mv_candidate", lambda *a, **k: None)
    monkeypatch.setattr(mv_advisor, "space_corpus_entries", lambda cfg: ())
    monkeypatch.setattr(mv_advisor, "estate_metric_view_yamls", lambda *a, **k: {})
    monkeypatch.setattr(
        warehouse, "wh_supersede_legacy_mv_candidates",
        lambda *a, **k: calls.append(k),
    )
    monkeypatch.setattr(
        mv_advisor, "advise_from_corpus",
        lambda **k: captured.update(k) or SimpleNamespace(
            status="COMPLETE", skip_reason=None, error=None, detail=lambda: {},
        ),
    )

    mv_suggest.suggest_for_space(
        sp_ws=MagicMock(), catalog="main", schema="gso", warehouse_id="wh1",
        llm_model="m", space_id="space-1", applied_config={"instructions": {}},
        triggered_by="analyst@example.com",
    )
    persist = captured["persist_proposal"]
    single = SimpleNamespace(
        target_space_id="space-1", suggestion_id="s", dedup_fingerprint="fp1",
        candidate_type="CONFLICT", confidence_score=50.0, tier="LOW",
        proposed_object=None, components=SimpleNamespace(to_dict=lambda: {}),
        evidence={}, provenance={}, alternatives=[], conflicts=[],
    )
    persist(single, SimpleNamespace(ok=False, yaml_text=None))
    assert calls == []


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


def _stale_beside_its_successor() -> list[dict]:
    from genie_space_optimizer.common.config import MV_RENDER_VERSION

    return [
        {**_row("sug_stale"), "proposed_object": "`Finance`.`Sales`.`Revenue`", "evidence": {}},
        {
            **_row("sug_current"), "proposed_object": "finance.sales.revenue",
            "evidence": {"render_version": MV_RENDER_VERSION},
        },
    ]


def _stub_a_complete_scan(monkeypatch) -> None:
    from genie_space_optimizer.common import genie_client

    monkeypatch.setattr(
        genie_client, "fetch_space_config",
        lambda ws, space_id: {"_parsed_space": {"instructions": {}}},
    )
    monkeypatch.setattr(
        mv_suggest, "suggest_for_space",
        lambda **k: (
            SimpleNamespace(status="COMPLETE", skip_reason=None, error=None, measures_found=2),
            "run-adv-4",
        ),
    )
    monkeypatch.setattr(
        warehouse, "wh_load_mv_candidates", lambda *a, **k: _stale_beside_its_successor(),
    )


def test_the_suggest_reload_drops_a_stale_proposal_its_successor_replaces(client, monkeypatch):
    """MV-D117: the post-scan reload reads through the same sibling rule as the list."""
    _stub_a_complete_scan(monkeypatch)

    resp = client.post("/api/auto-optimize/spaces/space-1/mv/suggest")

    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == ["sug_current"]


def test_the_stream_reload_drops_a_stale_proposal_its_successor_replaces(client, monkeypatch):
    import json as _json

    _stub_a_complete_scan(monkeypatch)

    resp = client.post("/api/auto-optimize/spaces/space-1/mv/suggest/stream")

    result = _json.loads(next(d for e, d in _parse_sse(resp.text) if e == "result"))
    assert [p["suggestion_id"] for p in result["proposals"]] == ["sug_current"]


_SUG_OLD = "sug_" + "a" * 12
_SUG_NEW = "sug_" + "b" * 12


def _stub_a_reshaped_scan(monkeypatch) -> list:
    """MV-D122: a re-scan reshaped the bundle, so two undecided current rows name
    one view; returns the recorded created-ledger lookups."""
    from genie_space_optimizer.common.config import MV_RENDER_VERSION

    _stub_a_complete_scan(monkeypatch)
    current = {"proposed_object": "finance.sales.revenue_metrics",
               "evidence": {"render_version": MV_RENDER_VERSION}, "decision": None}
    rows = [
        {**_row(_SUG_OLD), **current, "dedup_fingerprint": "a" * 64,
         "updated_at": "2026-10-01T10:00:00"},
        {**_row(_SUG_NEW), **current, "dedup_fingerprint": "b" * 64,
         "updated_at": "2026-10-01T11:00:00"},
    ]
    monkeypatch.setattr(warehouse, "wh_load_mv_candidates", lambda *a, **k: rows)
    asked: list = []

    def _created(ws, warehouse_id, *, catalog, schema, suggestion_ids):
        asked.append(sorted(suggestion_ids))
        return set()

    monkeypatch.setattr(warehouse, "wh_created_suggestion_ids", _created)
    return asked


def test_the_suggest_reload_drops_the_older_undecided_sibling(client, monkeypatch):
    asked = _stub_a_reshaped_scan(monkeypatch)

    resp = client.post("/api/auto-optimize/spaces/space-1/mv/suggest")

    assert [p["suggestion_id"] for p in resp.json()["proposals"]] == [_SUG_NEW]
    assert asked == [[_SUG_OLD, _SUG_NEW]]


def test_the_stream_reload_drops_the_older_undecided_sibling(client, monkeypatch):
    import json as _json

    asked = _stub_a_reshaped_scan(monkeypatch)

    resp = client.post("/api/auto-optimize/spaces/space-1/mv/suggest/stream")

    result = _json.loads(next(d for e, d in _parse_sse(resp.text) if e == "result"))
    assert [p["suggestion_id"] for p in result["proposals"]] == [_SUG_NEW]
    assert asked == [[_SUG_OLD, _SUG_NEW]]


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


def test_suggest_scope_error_does_not_read_as_the_service_principal(client, monkeypatch):
    """M1c-D3: MissingSerializedSpaceError from the user's read is a 502 —
    never a second fetch_space_config call with the service principal."""
    from genie_space_optimizer.common import genie_client
    from genie_space_optimizer.common.genie_client import MissingSerializedSpaceError

    user_ws = MagicMock(name="user_ws")
    sp_ws = MagicMock(name="sp_ws")
    monkeypatch.setattr(auto_optimize, "require_obo_workspace_client", lambda: user_ws)
    monkeypatch.setattr(auto_optimize, "get_service_principal_client", lambda: sp_ws)

    seen: list[object] = []

    def _fetch(ws, space_id):
        seen.append(ws)
        raise MissingSerializedSpaceError("no serialized_space")

    monkeypatch.setattr(genie_client, "fetch_space_config", _fetch)

    resp = client.post("/api/auto-optimize/spaces/space-1/mv/suggest")
    assert resp.status_code == 502
    assert resp.json()["detail"] == "Could not read the Agent configuration."
    assert seen == [user_ws]


def test_suggest_stream_config_error_emits_error_event(monkeypatch):
    """M1c-D3 stream: a failed OBO config read ends with an error event, no SP retry."""
    import json as _json

    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from backend.main import OBOAuthMiddleware
    from backend.services import auth
    from genie_space_optimizer.common import genie_client
    from genie_space_optimizer.common.genie_client import MissingSerializedSpaceError

    monkeypatch.setenv("GSO_CATALOG", "main")
    monkeypatch.setenv("GSO_SCHEMA", "gso_test")
    monkeypatch.setenv("GSO_JOB_ID", "12345")
    monkeypatch.setenv("GSO_WAREHOUSE_ID", "wh-test")
    monkeypatch.setenv("DATABRICKS_HOST", "https://test.cloud.databricks.com")

    monkeypatch.setattr(auth, "Config", lambda **k: SimpleNamespace(**k))
    monkeypatch.setattr(
        auth, "WorkspaceClient", lambda config=None, **k: SimpleNamespace(config=config)
    )
    sp_client = SimpleNamespace(config=SimpleNamespace(token="sp-token"))
    monkeypatch.setattr(auth, "_get_default_client", lambda: sp_client)

    seen: list[object] = []

    def _fetch(ws, space_id):
        seen.append(getattr(getattr(ws, "config", None), "token", None))
        raise MissingSerializedSpaceError("no serialized_space")

    monkeypatch.setattr(genie_client, "fetch_space_config", _fetch)

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
    assert "error" in kinds
    assert "result" not in kinds
    assert seen == ["user-token"]
    err = _json.loads(next(d for e, d in frames if e == "error"))
    assert err["detail"] == "Could not read the Agent configuration."
