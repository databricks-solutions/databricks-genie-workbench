"""Phase 1.3 — _compute_forbidden_ag_set uses terminal signatures."""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    EmittedPatchShape,
    build_terminal_signature,
)
from genie_space_optimizer.optimization.reflection_buffer_schema import (
    build_reflection_entry,
)


def _csig(cluster_id: str, qids) -> tuple:
    """Cluster grouping key (computed independently of TerminalSignature)."""
    return ((str(cluster_id), tuple(sorted(qids))),)


def _entry(*, iteration: int, ag_id: str,
           terminal_reason: "TerminalReason | None",
           cluster_id: str = "c1", target_qids=("gs_001",),
           root_cause: str = "r", levers=(5,),
           shape: EmittedPatchShape = EmittedPatchShape.METADATA,
           rollback_class: str = "no_action") -> dict:
    sig = None if terminal_reason is None else build_terminal_signature(
        root_cause=root_cause,
        blame_set=(),
        lever_set=list(levers),
        target_qids=list(target_qids),
        terminal_reason=terminal_reason,
    )
    return build_reflection_entry(
        iteration=iteration, ag_id=ag_id,
        rollback_class=rollback_class, accepted=False,
        terminal_signature=sig,
        cluster_signature=_csig(cluster_id, target_qids),
        emitted_patch_shape=shape,
        legacy_fields={"root_cause": root_cause,
                       "target_qids": list(target_qids),
                       "lever_set": list(levers)},
    )


def test_legacy_filter_keeps_only_content_regression(monkeypatch):
    """With the flag OFF, only CONTENT_REGRESSION rollback_class is
    admitted to the forbidden set (pre-Phase-1 behavior)."""
    monkeypatch.setenv("GSO_TERMINAL_SIGNATURE_RETIRE", "0")
    from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
        compute_retired_signatures,
    )
    reflection = [
        _entry(iteration=1, ag_id="ag-1",
               terminal_reason=TerminalReason.CONTENT_REGRESSION_ROLLBACK,
               rollback_class="content_regression"),
        _entry(iteration=2, ag_id="ag-2",
               terminal_reason=TerminalReason.NO_APPLIED_PATCHES,
               rollback_class="no_action"),
    ]
    retired = compute_retired_signatures(reflection_buffer=reflection)
    # legacy mode: only entry 1 (content_regression) is retired
    assert len(retired) == 1


def test_phase_1_3_retires_any_non_accepted_terminal(monkeypatch):
    """With the flag ON, ANY non-accepted terminal_reason on a
    repeated signature retires the AG."""
    monkeypatch.setenv("GSO_TERMINAL_SIGNATURE_RETIRE", "1")
    from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
        compute_retired_signatures,
    )
    reflection = [
        _entry(iteration=1, ag_id="ag-1",
               terminal_reason=TerminalReason.NO_APPLIED_PATCHES),
        _entry(iteration=2, ag_id="ag-2",
               terminal_reason=TerminalReason.STRUCTURAL_GATE_DROPPED_INSTRUCTION_ONLY),
    ]
    retired = compute_retired_signatures(reflection_buffer=reflection)
    assert len(retired) == 2


def test_accepted_iterations_never_retire():
    """An accepted iteration MUST never enter the retired set even
    if its terminal signature later repeats."""
    from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
        compute_retired_signatures,
    )
    # Accepted iterations have terminal_reason=None (AcceptanceTier
    # owns the accepted vocabulary per spec Section 5; the buffer entry
    # records accepted=True with no TerminalReason).
    reflection = [
        {**_entry(iteration=1, ag_id="ag-1",
                  terminal_reason=None),
         "accepted": True},
        _entry(iteration=2, ag_id="ag-2",
               terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY),
    ]
    retired = compute_retired_signatures(reflection_buffer=reflection)
    # Only the iter-2 entry's signature appears
    assert len(retired) == 1


def test_same_signature_repeated_only_listed_once():
    """One repeat of the same TerminalSignature → one retired
    entry, not two."""
    from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
        compute_retired_signatures,
    )
    reflection = [
        _entry(iteration=1, ag_id="ag-1",
               terminal_reason=TerminalReason.NO_APPLIED_PATCHES),
        # identical signature
        _entry(iteration=2, ag_id="ag-2",
               terminal_reason=TerminalReason.NO_APPLIED_PATCHES),
    ]
    retired = compute_retired_signatures(reflection_buffer=reflection)
    assert len(retired) == 1


def test_is_signature_retired_after_one_appearance():
    """One appearance of the signature in the reflection_buffer is
    enough to retire (the block rule: one repeat → retired)."""
    from genie_space_optimizer.optimization.forbidden_ag_set_v2 import (
        is_signature_retired,
        compute_retired_signatures,
    )
    reflection = [
        _entry(iteration=1, ag_id="ag-1",
               terminal_reason=TerminalReason.PROPOSAL_GENERATION_EMPTY),
    ]
    retired = compute_retired_signatures(reflection_buffer=reflection)
    sig_same = reflection[0]["terminal_signature"]
    assert is_signature_retired(
        candidate_signature=sig_same, retired=retired,
    ) is True
