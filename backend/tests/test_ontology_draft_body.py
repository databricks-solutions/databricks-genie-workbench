"""Unit tests for ontology on-demand + bulk body drafting (Stage 4.1d Steps 3–4).

Tests verify that the draft_body service initializes correctly and the models
are properly defined. Full integration tests are done via deploy-verify.
"""

from __future__ import annotations

import pytest

from backend.ontology.models import (
    BulkDraftResult,
    BulkDraftStart,
    BulkDraftStatus,
    DraftBodyResponse,
)
from backend.ontology.services import draft_body


# ── Service import tests ──────────────────────────────────────────────────────


def test_draft_body_module_imports():
    """Verify that draft_body module imports without errors."""
    assert hasattr(draft_body, "draft_one")
    assert hasattr(draft_body, "draft_subdomain")
    assert hasattr(draft_body, "get_bulk_draft_status")
    assert callable(draft_body.draft_one)
    assert callable(draft_body.draft_subdomain)
    assert callable(draft_body.get_bulk_draft_status)


# ── Model validation tests ────────────────────────────────────────────────────


def test_draft_body_response_model():
    """Test DraftBodyResponse model creation and serialization."""
    response = DraftBodyResponse(
        ok=True,
        page_id="page-001",
        body="Test body",
        body_source="llm_ondemand",
        as_of="2024-01-01T00:00:00Z",
        reason=None,
    )
    assert response.ok is True
    assert response.page_id == "page-001"
    assert response.body_source == "llm_ondemand"

    # Test JSON serialization
    data = response.model_dump(mode="json")
    assert data["ok"] is True
    assert data["body_source"] == "llm_ondemand"


def test_draft_body_response_failure():
    """Test DraftBodyResponse with failure state."""
    response = DraftBodyResponse(
        ok=False,
        page_id="page-002",
        body="Original body",
        body_source="unknown",
        as_of="2024-01-01T00:00:00Z",
        reason="LLM call failed",
    )
    assert response.ok is False
    assert response.reason == "LLM call failed"


def test_bulk_draft_start_model():
    """Test BulkDraftStart model."""
    start = BulkDraftStart(task_id="task-abc123", total=5)
    assert start.task_id == "task-abc123"
    assert start.total == 5

    data = start.model_dump(mode="json")
    assert data["task_id"] == "task-abc123"
    assert data["total"] == 5


def test_bulk_draft_result_model():
    """Test BulkDraftResult model."""
    result = BulkDraftResult(page_id="page-001", ok=True, reason=None)
    assert result.page_id == "page-001"
    assert result.ok is True
    assert result.reason is None

    result_fail = BulkDraftResult(page_id="page-002", ok=False, reason="Gate failed")
    assert result_fail.ok is False
    assert result_fail.reason == "Gate failed"


def test_bulk_draft_status_model():
    """Test BulkDraftStatus model."""
    results = [
        BulkDraftResult(page_id="page-001", ok=True, reason=None),
        BulkDraftResult(page_id="page-002", ok=False, reason="Timeout"),
    ]
    status = BulkDraftStatus(done=2, total=3, running=False, results=results)
    assert status.done == 2
    assert status.total == 3
    assert status.running is False
    assert len(status.results) == 2

    data = status.model_dump(mode="json")
    assert data["done"] == 2
    assert len(data["results"]) == 2


# ── Task registry tests ───────────────────────────────────────────────────────


def test_task_registry_lifecycle():
    """Test the in-process task registry for bulk drafts."""
    import uuid

    # Simulate a task being registered
    task_id = str(uuid.uuid4())

    # Initialize a task in the registry
    from backend.ontology.services.draft_body import _task_registry, _registry_lock

    with _registry_lock:
        _task_registry[task_id] = {
            "done": 0,
            "total": 3,
            "running": True,
            "results": [],
        }

    # Retrieve the task
    status = draft_body.get_bulk_draft_status(task_id)
    assert status is not None
    assert status["total"] == 3
    assert status["running"] is True
    assert status["done"] == 0

    # Simulate task completion
    with _registry_lock:
        _task_registry[task_id]["running"] = False
        _task_registry[task_id]["done"] = 3
        _task_registry[task_id]["results"] = [
            {"page_id": "page-001", "ok": True, "reason": None},
            {"page_id": "page-002", "ok": True, "reason": None},
            {"page_id": "page-003", "ok": True, "reason": None},
        ]

    # Retrieve final status
    final_status = draft_body.get_bulk_draft_status(task_id)
    assert final_status is not None
    assert final_status["running"] is False
    assert final_status["done"] == 3
    assert len(final_status["results"]) == 3


def test_nonexistent_task_returns_none():
    """Test that getting a nonexistent task returns None."""
    status = draft_body.get_bulk_draft_status("nonexistent-task-id-12345")
    assert status is None


# ── Behavioral tests (draft_one / draft_subdomain / route degrade) ────────────
# These exercise the real draft path with stub drafter + captured warehouse writes,
# so they cover the 5 gaps the shallow model/registry tests above missed.

import json  # noqa: E402
import time  # noqa: E402
import types  # noqa: E402

from backend.ontology.services import mirror  # noqa: E402


_ROW_BODY_WITH_ID = (
    "Description: Existing description.\n"
    "\n"
    "Definition:\n"
    "  The governed value from `cat.sch.mv`.\n"
    "\n"
    "Rules:\n"
    "  - Answer from `cat.sch.mv`.\n"
)

# A well-formed LLM draft: Definition + every Rules bullet carry a backticked Source.
_GOOD_DRAFT = (
    "Description: Total spend roll-up for the period.\n"
    "Definition: Total is the governed roll-up from `cat.sch.mv`.\n"
    "Rules:\n"
    "- Answer total from `cat.sch.mv`; never re-aggregate its sources.\n"
)

# A draft that paraphrases the identifier AWAY from the Definition line — passes only
# because _canonical_body substitutes the deterministic definition (MV-D70).
_DRAFT_NO_ID_IN_DEFINITION = (
    "Description: Total spend roll-up for the period.\n"
    "Definition: Total is the governed spend roll-up for the period.\n"
    "Rules:\n"
    "- Answer total from `cat.sch.mv`; never re-aggregate its sources.\n"
)


def _page_row(page_id: str, *, domain_id: str = "d1", facts_hash: str | None = None) -> dict:
    evidence: dict = {"canonical_id": f"c_{page_id}", "corroboration": 3}
    if facts_hash is not None:
        evidence["facts_hash"] = facts_hash
    return {
        "page_id": page_id,
        "domain_id": domain_id,
        "archetype": "Routing",
        "title": f"Title {page_id}",
        "body": _ROW_BODY_WITH_ID,
        "synonyms": [],
        "source_fqns": ["cat.sch.mv"],
        "related_fqns": [],
        "certify": True,
        "confidence": 0.9,
        "evidence": evidence,
    }


def _patch_mirror(monkeypatch, rows: list[dict]) -> None:
    async def _fake_read_table(table, metastore_id):  # noqa: ANN001
        return list(rows)

    monkeypatch.setattr(mirror, "_read_table", _fake_read_table)


def _patch_drafter(monkeypatch, text: str, captured: dict | None = None):
    def _factory(model=None, w=None):  # noqa: ANN001
        def _drafter(facts):  # noqa: ANN001
            if captured is not None:
                captured["facts"] = facts
            return text

        return _drafter

    monkeypatch.setattr(draft_body, "default_page_drafter", _factory)


def _capture_writes(monkeypatch) -> list[dict]:
    writes: list[dict] = []

    def _fake_update(w, metastore_id, page_id, body, evidence_json):  # noqa: ANN001
        # The service must hand us VALID JSON (evidence is a JSON text column) — json.loads
        # here fails loudly if the write ever regresses to a Spark CAST display form.
        evidence = json.loads(evidence_json)
        writes.append(
            {
                "page_id": page_id,
                "body": body,
                "evidence": evidence,
                "body_source": evidence.get("body_source"),
                "facts_hash": evidence.get("facts_hash"),
            }
        )
        return True

    monkeypatch.setattr(draft_body, "_execute_update_via_warehouse", _fake_update)
    return writes


def test_draft_one_reads_the_targeted_page_not_the_whole_table(monkeypatch):
    """F1: draft_one must resolve THE requested page_id — not hand the whole row list to
    the drafter (which used to raise AttributeError → 500)."""
    _patch_mirror(monkeypatch, [_page_row("p1"), _page_row("p2"), _page_row("p3")])
    captured: dict = {}
    _patch_drafter(monkeypatch, _GOOD_DRAFT, captured)
    writes = _capture_writes(monkeypatch)

    result = draft_body.draft_one("p2", metastore_id="ms1", w=object())

    assert result["ok"] is True
    assert result["page_id"] == "p2"
    # The drafter saw p2's facts (a dict), never the row list.
    assert isinstance(captured["facts"], dict)
    assert captured["facts"]["concept"] == "Title p2"
    assert [w_["page_id"] for w_ in writes] == ["p2"]


def test_draft_one_missing_page_degrades(monkeypatch):
    """F1/F4: an unknown page_id degrades to ok=false, never raises."""
    _patch_mirror(monkeypatch, [_page_row("p1")])
    _patch_drafter(monkeypatch, _GOOD_DRAFT)
    _capture_writes(monkeypatch)

    result = draft_body.draft_one("missing", metastore_id="ms1", w=object())
    assert result["ok"] is False
    assert result["reason"] == "page not found"


def test_draft_one_preserves_batch_facts_hash(monkeypatch):
    """F2: the persisted facts_hash is the PRESERVED batch hash from evidence — not a
    fresh SHA256 of the body (which would falsely flag the body stale next re-materialize)."""
    _patch_mirror(monkeypatch, [_page_row("p1", facts_hash="BATCHHASH_DO_NOT_RECOMPUTE")])
    _patch_drafter(monkeypatch, _GOOD_DRAFT)
    writes = _capture_writes(monkeypatch)

    result = draft_body.draft_one("p1", metastore_id="ms1", w=object())
    assert result["ok"] is True
    ev = writes[0]["evidence"]
    assert ev["facts_hash"] == "BATCHHASH_DO_NOT_RECOMPUTE"
    assert ev["body_stale"] is False
    # The write is valid JSON that PRESERVES pre-existing evidence keys (not a blob wipe).
    assert ev["canonical_id"] == "c_p1"


def test_draft_one_canonicalizes_before_gates(monkeypatch):
    """F3: a draft whose Definition drops the identifier is SALVAGED via _canonical_body
    (deterministic definition fallback) instead of being rejected by specificity_gate."""
    _patch_mirror(monkeypatch, [_page_row("p1")])
    _patch_drafter(monkeypatch, _DRAFT_NO_ID_IN_DEFINITION)
    writes = _capture_writes(monkeypatch)

    result = draft_body.draft_one("p1", metastore_id="ms1", w=object())
    assert result["ok"] is True
    # The reassembled Definition carries the deterministic, in-universe identifier.
    assert "Definition:" in writes[0]["body"]
    assert "`cat.sch.mv`" in writes[0]["body"].split("Rules:")[0]


def test_draft_one_empty_draft_degrades(monkeypatch):
    """F4: an empty drafter output degrades to ok=false, unchanged body, no write."""
    _patch_mirror(monkeypatch, [_page_row("p1")])
    _patch_drafter(monkeypatch, "   ")
    writes = _capture_writes(monkeypatch)

    result = draft_body.draft_one("p1", metastore_id="ms1", w=object())
    assert result["ok"] is False
    assert result["body"] == _ROW_BODY_WITH_ID
    assert writes == []


def test_draft_subdomain_stamps_llm_bulk(monkeypatch):
    """F5: every page drafted by draft_subdomain is persisted with body_source='llm_bulk',
    not 'llm_ondemand'."""
    _patch_mirror(monkeypatch, [_page_row("p1", domain_id="d1"), _page_row("p2", domain_id="d1")])
    _patch_drafter(monkeypatch, _GOOD_DRAFT)
    writes = _capture_writes(monkeypatch)

    task_id, total, _ = draft_body.draft_subdomain("d1", metastore_id="ms1", w=object(), max_workers=2)
    assert total == 2

    for _ in range(100):  # wait for the fire-and-forget worker pool (≤10s)
        st = draft_body.get_bulk_draft_status(task_id)
        if st and not st["running"]:
            break
        time.sleep(0.1)

    st = draft_body.get_bulk_draft_status(task_id)
    assert st is not None and st["running"] is False and st["done"] == 2
    assert {w_["page_id"] for w_ in writes} == {"p1", "p2"}
    assert all(w_["body_source"] == "llm_bulk" for w_ in writes)


def test_update_page_body_sql_binds_json_evidence_no_phantom_column():
    """The UPDATE binds a whole JSON evidence param and touches only real columns —
    no `updated_at` (genie_ont_pages has none) and no Spark `CAST(... AS STRING)` that
    would emit the non-JSON `{k -> v}` display form into a JSON text column."""
    stmt, params = draft_body._update_page_body_sql(
        "ms1", "p1", "the body", '{"body_source":"llm_ondemand","facts_hash":"h"}'
    )
    assert "updated_at" not in stmt
    assert "CAST(" not in stmt.upper()
    assert "MERGE" not in stmt.upper()
    assert "evidence = :evidence" in stmt
    names = {p.name for p in params}
    assert names == {"metastore_id", "page_id", "body", "evidence"}


async def test_post_draft_body_route_degrades_never_500(monkeypatch):
    """F4: an unexpected exception inside the route degrades to a typed ok=false payload,
    never a 500."""
    from backend.ontology.routers import drafts as drafts_router

    monkeypatch.setattr(drafts_router.ont_settings, "_metastore_id", lambda: "ms1")
    monkeypatch.setattr(drafts_router, "get_workspace_client", lambda: object())

    def _boom(*a, **k):  # noqa: ANN001, ANN002, ANN003
        raise RuntimeError("kaboom")

    monkeypatch.setattr(drafts_router.draft_body, "draft_one", _boom)

    fake_request = types.SimpleNamespace(headers={})
    payload = await drafts_router.post_draft_body("p1", fake_request)  # type: ignore[arg-type]
    assert payload["ok"] is False
    assert payload["page_id"] == "p1"
    assert payload["body"] == ""
    assert payload["reason"] == "draft failed"
