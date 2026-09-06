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
