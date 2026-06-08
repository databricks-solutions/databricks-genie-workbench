"""Trial 32 W32.2 — inert-loses-to-structural arbitration.

The 7now W31.5 live replay (task ``153959534308899``) advanced
``gs_013``/``gs_026`` to ``accepted`` but the surviving patch was an
inert ``add_column_description`` (``behavioral_diff=unchanged``) that
WON slate selection over a SQL-reshaping structural co-proposal. The
routing brain treats ``METADATA_DESCRIPTION`` as fully
structural-equivalent to ``SQL_SNIPPET`` for ``top_n_cardinality_collapse``
(fixing set ``{SQL_SNIPPET, METADATA_DESCRIPTION}``), so the weaker
metadata patch was never deprioritised.

W32.2 introduces a *structural-strength ranking*: when a
structural-mandate RCA's slate carries BOTH a SQL-reshaping mechanism
(``SQL_SNIPPET``/``ROUTING``) and a strictly-weaker metadata mechanism
targeting the same QID, the metadata proposal is dropped before applier
with ``DropReason.INERT_SUPERSEDED_BY_STRUCTURAL``.

Generality is proved on a **non-anchor synthetic fixture**
(``demo_synth_q99``) — no per-QID / per-anchor literals.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism
from genie_space_optimizer.optimization.rca_mechanism_routing import (
    structural_mechanism_strength,
    structurally_superseded_by_stronger,
)
from genie_space_optimizer.optimization.proposal_slate_compiler import (
    DropReason,
    SlateCompilerContext,
    _check_inert_superseded_by_structural,
    compile_slate,
    drop_reason_to_terminal_reason,
)
from genie_space_optimizer.optimization.repair_proposal_typed import (
    RepairProposal,
)
from genie_space_optimizer.optimization.repair_intent import (
    PatchType,
    RepairShape,
)


_SYNTH_QID = "demo_synth_q99"  # deliberately NOT an anchor QID


def _proposal(intent_id: str, patch_type: str, qid: str = _SYNTH_QID) -> RepairProposal:
    return RepairProposal(
        intent_id=intent_id,
        intent_name=f"intent {intent_id}",
        intent_description="",
        repair_shape=RepairShape.OTHER,
        patch_type=PatchType(patch_type),
        rationale="r",
        confidence="medium",
        patch_body={"x": 1},
        blame_set=(qid,),
        target_qids=(qid,),
        selected_lever="lever-6",
        selected_levers=("lever-6",),
        bundle_id="",
    )


# ── pure ranking predicate ────────────────────────────────────────────


def test_structural_strength_orders_sql_reshaping_above_metadata():
    # SQL-reshaping mechanisms strictly outrank metadata description,
    # which outranks prose / example-sql.
    assert structural_mechanism_strength(PatchMechanism.SQL_SNIPPET) > (
        structural_mechanism_strength(PatchMechanism.METADATA_DESCRIPTION)
    )
    assert structural_mechanism_strength(PatchMechanism.ROUTING) > (
        structural_mechanism_strength(PatchMechanism.METADATA_DESCRIPTION)
    )
    assert structural_mechanism_strength(PatchMechanism.METADATA_DESCRIPTION) > (
        structural_mechanism_strength(PatchMechanism.INSTRUCTION_TEXT)
    )
    assert structural_mechanism_strength(None) < 0


@pytest.mark.parametrize("rca", ["top_n_cardinality_collapse", "plural_top_n_collapse"])
def test_metadata_superseded_when_stronger_structural_present(rca, monkeypatch):
    # ``plural_top_n_collapse`` is an alias requiring the Trial 26 map.
    monkeypatch.setenv("GSO_TRIAL26_KIT_MAP_EXPANDED", "1")
    observed = [PatchMechanism.METADATA_DESCRIPTION, PatchMechanism.SQL_SNIPPET]
    assert (
        structurally_superseded_by_stronger(
            rca, PatchMechanism.METADATA_DESCRIPTION, observed
        )
        is True
    )
    # The stronger one is never superseded by itself.
    assert (
        structurally_superseded_by_stronger(
            rca, PatchMechanism.SQL_SNIPPET, observed
        )
        is False
    )


def test_no_supersede_without_a_stronger_companion():
    # Metadata alone (no SQL-reshaping companion) is NOT superseded —
    # keeping a sole structural survivor avoids flatlining the slate.
    assert (
        structurally_superseded_by_stronger(
            "top_n_cardinality_collapse",
            PatchMechanism.METADATA_DESCRIPTION,
            [PatchMechanism.METADATA_DESCRIPTION],
        )
        is False
    )


def test_no_supersede_for_unmapped_rca():
    # An RCA outside the structural-mandate map carries no ranking; the
    # predicate is a no-op (generalises by map membership, no literals).
    assert (
        structurally_superseded_by_stronger(
            "some_unmapped_rca",
            PatchMechanism.METADATA_DESCRIPTION,
            [PatchMechanism.METADATA_DESCRIPTION, PatchMechanism.SQL_SNIPPET],
        )
        is False
    )
    assert structurally_superseded_by_stronger(None, None, []) is False


# ── Phase-2 check helper ───────────────────────────────────────────────


def test_check_drops_inert_metadata_keeps_structural():
    meta = _proposal("p_meta", "add_column_description")
    snip = _proposal("p_snip", "add_sql_snippet_filter")
    ctx = SlateCompilerContext(
        rca_kind_label_by_qid={_SYNTH_QID: "top_n_cardinality_collapse"},
    )
    drops = _check_inert_superseded_by_structural([meta, snip], ctx)
    dropped_ids = {p.intent_id for p, _ in drops}
    assert dropped_ids == {"p_meta"}
    assert all(r is DropReason.INERT_SUPERSEDED_BY_STRUCTURAL for _, r in drops)


def test_check_noop_when_only_metadata_present():
    meta = _proposal("p_meta", "add_column_description")
    ctx = SlateCompilerContext(
        rca_kind_label_by_qid={_SYNTH_QID: "top_n_cardinality_collapse"},
    )
    assert _check_inert_superseded_by_structural([meta], ctx) == []


def test_check_noop_for_unmapped_rca():
    meta = _proposal("p_meta", "add_column_description")
    snip = _proposal("p_snip", "add_sql_snippet_filter")
    ctx = SlateCompilerContext(
        rca_kind_label_by_qid={_SYNTH_QID: "some_unmapped_rca"},
    )
    assert _check_inert_superseded_by_structural([meta, snip], ctx) == []


# ── compile_slate wiring (Phase-1 gates neutralised) ───────────────────


def _disable_phase1(monkeypatch):
    # Neutralise Phase-1 per-proposal gates so this test isolates the
    # Phase-2 W32.2 decision (snippet/asset validators would otherwise
    # drop the synthetic proposals before Phase 2 runs).
    monkeypatch.setenv("GSO_TRIAL22_ASSET_GATE", "0")
    import genie_space_optimizer.optimization.proposal_slate_compiler as psc

    monkeypatch.setattr(psc, "_check_snippet_validator", lambda p, c: None)
    monkeypatch.setattr(psc, "_check_metadata_target", lambda p, c: None)


def test_compile_slate_drops_inert_metadata_with_marker(monkeypatch):
    _disable_phase1(monkeypatch)
    meta = _proposal("p_meta", "add_column_description")
    snip = _proposal("p_snip", "add_sql_snippet_filter")
    ctx = SlateCompilerContext(
        optimization_run_id="run_synth",
        iteration=3,
        cluster_id="C_synth",
        rca_kind_label_by_qid={_SYNTH_QID: "top_n_cardinality_collapse"},
    )
    result = compile_slate([meta, snip], ctx)
    surviving_ids = {p.intent_id for p in result.surviving_proposals}
    assert surviving_ids == {"p_snip"}
    dropped = {
        p.intent_id: r for p, r in result.dropped_proposals
    }
    assert dropped.get("p_meta") is DropReason.INERT_SUPERSEDED_BY_STRUCTURAL
    assert any(
        m.get("marker") == "GSO_TRIAL32_INERT_LOSES_TO_STRUCTURAL_V1"
        for m in result.actuator_markers
    )


def test_compile_slate_byte_stable_when_flag_off(monkeypatch):
    _disable_phase1(monkeypatch)
    monkeypatch.setenv("GSO_TRIAL32_INERT_LOSES_TO_STRUCTURAL", "0")
    meta = _proposal("p_meta", "add_column_description")
    snip = _proposal("p_snip", "add_sql_snippet_filter")
    ctx = SlateCompilerContext(
        rca_kind_label_by_qid={_SYNTH_QID: "top_n_cardinality_collapse"},
    )
    result = compile_slate([meta, snip], ctx)
    surviving_ids = {p.intent_id for p in result.surviving_proposals}
    assert surviving_ids == {"p_meta", "p_snip"}  # both kept when OFF


def test_drop_reason_maps_to_terminal_reason():
    # Adding the DropReason must keep the DropReason→TerminalReason map total.
    assert (
        drop_reason_to_terminal_reason(DropReason.INERT_SUPERSEDED_BY_STRUCTURAL)
        is not None
    )
