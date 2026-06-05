"""Trial 20 Workstream B + C — terminal taxonomy + pivot graph.

Pins:

* ``TerminalReason.KEPT_INSUFFICIENT`` exists and round-trips through
  the enum constructor (B1).
* ``"kept_insufficient"`` is in ``_TERMINATIONS_REQUIRING_PIVOT`` (B3).
* ``next_patch_family_for_cluster`` returns the cycle-next family on
  pivot (C1) and falls back to legacy default when the flag is OFF.
* When ``prior_patch_family`` is empty, the function infers it from
  the latest signature carrying ``patch_family`` / ``patch_type`` /
  ``insufficient_repair_signature.patch_type`` (C2).
"""
from __future__ import annotations

from dataclasses import dataclass

import pytest

from genie_space_optimizer.optimization.stages.action_groups import (
    _PIVOT_FROM_FAMILY_AFTER_FAILURE,
    _PIVOT_GRAPH,
    _TERMINATIONS_REQUIRING_PIVOT,
    next_patch_family_for_cluster,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason


def test_b1_terminal_reason_kept_insufficient_exists():
    assert TerminalReason("kept_insufficient") is TerminalReason.KEPT_INSUFFICIENT


def test_b3_kept_insufficient_in_pivot_set():
    assert "kept_insufficient" in _TERMINATIONS_REQUIRING_PIVOT


def test_b3_other_legacy_reasons_still_pivot():
    """Legacy pivot membership preserved."""
    for r in (
        "no_applied_patches",
        "structural_gate_dropped_instruction_only",
        "applyability_rejected",
    ):
        assert r in _TERMINATIONS_REQUIRING_PIVOT


@dataclass
class _Sig:
    terminal_reason: str = ""
    patch_family: str | None = None
    patch_type: str | None = None
    insufficient_repair_signature: object | None = None


@dataclass
class _InsSig:
    patch_type: str | None = None
    patch_family: str | None = None


@pytest.fixture
def trial20_on(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL20_ENFORCE", raising=False)
    monkeypatch.delenv("GSO_TRIAL20_FAMILY_PIVOT_GRAPH", raising=False)


@pytest.fixture
def trial20_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL20_ENFORCE", "0")


def test_c1_pivot_cycle_under_flag_on(trial20_on):
    sigs = [_Sig(terminal_reason="kept_insufficient")]
    assert next_patch_family_for_cluster(
        cluster_id="c1",
        prior_terminal_signatures=sigs,
        prior_patch_family="add_example_sql",
    ) == "add_sql_snippet_filter"

    assert next_patch_family_for_cluster(
        cluster_id="c1",
        prior_terminal_signatures=sigs,
        prior_patch_family="add_sql_snippet_filter",
    ) == "add_sql_snippet_expression"

    assert next_patch_family_for_cluster(
        cluster_id="c1",
        prior_terminal_signatures=sigs,
        prior_patch_family="add_column_description",
    ) == "add_instruction"


def test_c1_pivot_cycle_under_flag_off(trial20_off):
    """OFF — byte-stable: pivot returns the legacy single constant."""
    sigs = [_Sig(terminal_reason="kept_insufficient")]
    assert next_patch_family_for_cluster(
        cluster_id="c1",
        prior_terminal_signatures=sigs,
        prior_patch_family="add_example_sql",
    ) == _PIVOT_FROM_FAMILY_AFTER_FAILURE


def test_c1_no_pivot_when_no_survival_failure(trial20_on):
    """No pivot reason -> keep prior family."""
    sigs = [_Sig(terminal_reason="accepted")]
    assert next_patch_family_for_cluster(
        cluster_id="c1",
        prior_terminal_signatures=sigs,
        prior_patch_family="add_instruction",
    ) == "add_instruction"


def test_c2_infer_prior_family_from_signature_patch_family(trial20_on):
    sigs = [_Sig(
        terminal_reason="kept_insufficient",
        patch_family="add_example_sql",
    )]
    out = next_patch_family_for_cluster(
        cluster_id="c1",
        prior_terminal_signatures=sigs,
        prior_patch_family="",
    )
    assert out == "add_sql_snippet_filter"


def test_c2_infer_from_insufficient_repair_signature(trial20_on):
    sigs = [_Sig(
        terminal_reason="kept_insufficient",
        insufficient_repair_signature=_InsSig(patch_type="add_instruction"),
    )]
    out = next_patch_family_for_cluster(
        cluster_id="c1",
        prior_terminal_signatures=sigs,
        prior_patch_family="",
    )
    assert out == "add_example_sql"


def test_pivot_graph_is_closed_cycle():
    """Every value in the cycle is also a key."""
    keys = set(_PIVOT_GRAPH.keys())
    values = set(_PIVOT_GRAPH.values())
    assert keys == values
    assert len(_PIVOT_GRAPH) == 5
