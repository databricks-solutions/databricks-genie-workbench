"""Phase 0.4 — candidate ledger (frozen dataclass + JSONL writer)."""
from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.candidate_ledger import (
    IterationCandidateLedgerEntry,
    LEDGER_SCHEMA_VERSION,
    LEDGER_REQUIRED_FIELDS,
    write_ledger_entry,
    read_ledger,
    LedgerSchemaError,
)


def _make_entry(**overrides) -> IterationCandidateLedgerEntry:
    defaults: dict = dict(
        iteration=1,
        ag_id="ag-1",
        cluster_ids=("c1",),
        target_qids=("gs_026",),
        root_cause="no_metric_view_for_gross_sales",
        requested_levers=(5, 6),
        rca_card_id_or_provisional="rca-1",
        proposal_attempts=1,
        selected_proposal_id="prop-1",
        terminal_reason="no_structural_candidate",
        terminal_outcome="info",
        best_of_n_size=1,
        patches_applied=0,
        subset_isolation_run=False,
        subset_isolation_kept=(),
        subset_isolation_dropped=(),
        protected_dependents=(),
        narrow_replacement_attempted=False,
        narrow_replacement_succeeded=False,
        accuracy_delta_pp=0.0,
        acceptance_tier="reject_loss",
        retire_signature="root=no_metric_view_for_gross_sales|levers=5,6|targets=gs_026",
    )
    defaults.update(overrides)
    return IterationCandidateLedgerEntry(**defaults)


def test_dataclass_is_frozen():
    e = _make_entry()
    try:
        e.iteration = 99  # type: ignore[misc]
    except Exception:
        return
    raise AssertionError("dataclass should be frozen")


def test_required_fields_match_constant():
    """Every field listed in the Phase 0.4 schema MUST appear in
    LEDGER_REQUIRED_FIELDS exactly once."""
    expected = {
        "iteration", "ag_id", "cluster_ids", "target_qids", "root_cause",
        "requested_levers", "rca_card_id_or_provisional",
        "proposal_attempts", "selected_proposal_id",
        "terminal_reason", "terminal_outcome", "best_of_n_size",
        "patches_applied", "subset_isolation_run",
        "subset_isolation_kept", "subset_isolation_dropped",
        "protected_dependents", "narrow_replacement_attempted",
        "narrow_replacement_succeeded", "accuracy_delta_pp",
        "acceptance_tier", "retire_signature",
    }
    assert set(LEDGER_REQUIRED_FIELDS) == expected
    assert len(LEDGER_REQUIRED_FIELDS) == 22


def test_schema_version_is_v1():
    assert LEDGER_SCHEMA_VERSION == "v1"


def test_write_then_read_round_trips(tmp_path: Path):
    entries = [_make_entry(iteration=i, ag_id=f"ag-{i}") for i in range(1, 4)]
    ledger_path = tmp_path / "ledger.jsonl"
    for e in entries:
        write_ledger_entry(e, path=str(ledger_path))
    parsed = read_ledger(str(ledger_path))
    assert len(parsed) == 3
    for i, e in enumerate(parsed, start=1):
        assert e.iteration == i
        assert e.ag_id == f"ag-{i}"


def test_write_jsonl_is_append_only(tmp_path: Path):
    ledger_path = tmp_path / "ledger.jsonl"
    write_ledger_entry(_make_entry(iteration=1), path=str(ledger_path))
    write_ledger_entry(_make_entry(iteration=2), path=str(ledger_path))
    lines = ledger_path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    for line in lines:
        payload = json.loads(line)
        assert payload["schema_version"] == "v1"


def test_read_ledger_raises_on_unknown_schema_version(tmp_path: Path):
    ledger_path = tmp_path / "ledger.jsonl"
    ledger_path.write_text(
        json.dumps({"schema_version": "v99", "iteration": 1}) + "\n",
        encoding="utf-8",
    )
    try:
        read_ledger(str(ledger_path))
    except LedgerSchemaError:
        return
    raise AssertionError("read_ledger should raise on unknown schema_version")


def test_read_ledger_raises_on_missing_required_field(tmp_path: Path):
    ledger_path = tmp_path / "ledger.jsonl"
    payload = {"schema_version": "v1", "iteration": 1}  # missing 21 fields
    ledger_path.write_text(json.dumps(payload) + "\n", encoding="utf-8")
    try:
        read_ledger(str(ledger_path))
    except LedgerSchemaError as exc:
        assert "missing" in str(exc).lower()
        return
    raise AssertionError("read_ledger should raise on missing required fields")


# Task 12 — stdout marker + parser tests


def test_emit_ledger_marker_prefix():
    from genie_space_optimizer.optimization.candidate_ledger import (
        emit_ledger_marker,
    )
    line = emit_ledger_marker(_make_entry(), optimization_run_id="opt-1")
    assert line.startswith("GSO_CANDIDATE_LEDGER_ENTRY_V1 ")


def test_extract_from_stdout_returns_entries_in_order():
    from genie_space_optimizer.optimization.candidate_ledger import (
        emit_ledger_marker,
    )
    from genie_space_optimizer.tools.marker_parser import (
        extract_candidate_ledger_from_stdout,
    )

    lines = [
        emit_ledger_marker(_make_entry(iteration=1), optimization_run_id="r"),
        "some unrelated log line",
        emit_ledger_marker(_make_entry(iteration=2), optimization_run_id="r"),
    ]
    stdout = "\n".join(lines)
    entries = extract_candidate_ledger_from_stdout(stdout)
    assert len(entries) == 2
    assert entries[0]["iteration"] == 1
    assert entries[1]["iteration"] == 2


def test_parse_candidate_ledger_entry_marker_rejects_other_markers():
    from genie_space_optimizer.tools.marker_parser import (
        parse_candidate_ledger_entry_marker,
    )

    try:
        parse_candidate_ledger_entry_marker(
            'GSO_FULL_EVAL_V1 {"optimization_run_id":"r","payload":{}}'
        )
    except ValueError:
        return
    raise AssertionError("should reject non-candidate-ledger markers")
