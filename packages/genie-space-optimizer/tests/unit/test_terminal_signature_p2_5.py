"""Phase 2 P2.5 — kit-aware TerminalSignature + KIT_FOR_RCA pivot.

Pins:

  * ``TerminalSignature`` is now a frozen dataclass (was NamedTuple)
    with two extra optional fields. The 5 legacy positional fields
    keep their order, defaults preserve byte-stability.
  * ``build_terminal_signature`` accepts and normalizes the new
    fields; JSON round-trip preserves them.
  * ``next_companion_family_from_kit`` picks the first un-tried
    companion lever's representative patch_family — and returns
    "" when (a) RCA isn't in KIT_FOR_RCA, or (b) every companion
    has already been tried.
  * ``_infer_prior_family_from_signatures`` consults the new
    ``prior_patch_family`` field first when present.
  * ``next_patch_family_for_cluster`` prefers the KIT_FOR_RCA
    companion when the RCA has a typed contract; falls back to
    ``_PIVOT_GRAPH`` for diagnoses outside the map.
"""
from __future__ import annotations

import dataclasses

from genie_space_optimizer.optimization.stages.action_groups import (
    _infer_prior_family_from_signatures,
    next_companion_family_from_kit,
    next_patch_family_for_cluster,
)
from genie_space_optimizer.optimization.terminal_reason import TerminalReason
from genie_space_optimizer.optimization.terminal_signature import (
    TerminalSignature,
    build_terminal_signature,
    from_jsonable,
    to_jsonable,
)


# ── Dataclass shape ──────────────────────────────────────────────────


def test_terminal_signature_is_frozen_dataclass_with_slots() -> None:
    assert dataclasses.is_dataclass(TerminalSignature)
    assert TerminalSignature.__dataclass_params__.frozen is True  # type: ignore[attr-defined]
    assert "__slots__" in TerminalSignature.__dict__


def test_terminal_signature_field_order_preserved() -> None:
    # The 5 legacy fields keep their order at positions 0..4;
    # P2.5 extensions live at positions 5..6.
    names = [f.name for f in dataclasses.fields(TerminalSignature)]
    assert names[:5] == [
        "root_cause",
        "blame_set_norm",
        "lever_set",
        "target_qids",
        "terminal_reason",
    ]
    assert "prior_lever_set" in names[5:]
    assert "prior_patch_family" in names[5:]


def test_terminal_signature_is_hashable() -> None:
    sig = build_terminal_signature(
        root_cause="rc",
        blame_set=["a.b"],
        lever_set={5},
        target_qids={"q1"},
        terminal_reason=TerminalReason.KEPT_INSUFFICIENT,
    )
    # Round-trip through a set to confirm hashability.
    assert sig in {sig}


# ── Constructor / normalization ──────────────────────────────────────


def test_build_defaults_for_back_compat_callsites() -> None:
    sig = build_terminal_signature(
        root_cause="JOIN_SEMANTICS_WRONG",
        blame_set=("b.x", "b.x", ""),  # dedupe + drop blanks
        lever_set=[5, 6],
        target_qids={"q1"},
        terminal_reason=TerminalReason.KEPT_INSUFFICIENT,
    )
    assert sig.root_cause == "join_semantics_wrong"
    assert sig.blame_set_norm == ("b.x", "b.x")  # blanks dropped, sorted
    assert sig.lever_set == frozenset({5, 6})
    assert sig.target_qids == frozenset({"q1"})
    # P2.5 defaults
    assert sig.prior_lever_set == frozenset()
    assert sig.prior_patch_family == ""


def test_build_normalizes_prior_lever_set_and_family() -> None:
    sig = build_terminal_signature(
        root_cause="JOIN_SEMANTICS_WRONG",
        blame_set=(),
        lever_set=(),
        target_qids=(),
        terminal_reason=TerminalReason.KEPT_INSUFFICIENT,
        prior_lever_set=("lever-1", "lever-6", "", "lever-1"),
        prior_patch_family=" Add_Example_Sql  ",
    )
    assert sig.prior_lever_set == frozenset({"lever-1", "lever-6"})
    assert sig.prior_patch_family == "add_example_sql"


def test_json_round_trip_preserves_new_fields() -> None:
    sig = build_terminal_signature(
        root_cause="rc",
        blame_set=("b",),
        lever_set={6},
        target_qids={"q"},
        terminal_reason=TerminalReason.KEPT_INSUFFICIENT,
        prior_lever_set={"lever-5b", "lever-1"},
        prior_patch_family="add_example_sql",
    )
    d = to_jsonable(sig)
    assert d["prior_lever_set"] == ["lever-1", "lever-5b"]  # sorted
    assert d["prior_patch_family"] == "add_example_sql"
    revived = from_jsonable(d)
    assert revived == sig


# ── KIT_FOR_RCA companion picker ─────────────────────────────────────


def _sig(
    *,
    rca: str,
    prior_levers: tuple[str, ...] = (),
    prior_family: str = "",
) -> TerminalSignature:
    return build_terminal_signature(
        root_cause=rca,
        blame_set=(),
        lever_set=(),
        target_qids=(),
        terminal_reason=TerminalReason.KEPT_INSUFFICIENT,
        prior_lever_set=prior_levers,
        prior_patch_family=prior_family,
    )


def test_companion_picker_returns_empty_for_unmandated_rca() -> None:
    s = _sig(rca="plural_top_n_collapse")
    assert next_companion_family_from_kit("plural_top_n_collapse", [s]) == ""


def test_companion_picker_picks_first_untried_companion() -> None:
    # join_semantics_wrong companions: {lever-1, lever-6}.
    # Prior tried: {lever-1} ⇒ next should pick lever-6.
    s = _sig(rca="join_semantics_wrong", prior_levers=("lever-1",))
    family = next_companion_family_from_kit("join_semantics_wrong", [s])
    # lever-6 → add_sql_snippet_filter (representative family).
    assert family == "add_sql_snippet_filter"


def test_companion_picker_returns_empty_when_all_companions_tried() -> None:
    s = _sig(
        rca="join_semantics_wrong",
        prior_levers=("lever-1", "lever-6"),
    )
    assert next_companion_family_from_kit(
        "join_semantics_wrong", [s]
    ) == ""


def test_companion_picker_aggregates_tried_across_signatures() -> None:
    a = _sig(rca="join_semantics_wrong", prior_levers=("lever-1",))
    b = _sig(rca="join_semantics_wrong", prior_levers=("lever-6",))
    assert next_companion_family_from_kit(
        "join_semantics_wrong", [a, b]
    ) == ""


# ── _infer_prior_family_from_signatures uses new field ───────────────


def test_infer_prior_family_prefers_prior_patch_family() -> None:
    s = _sig(rca="rc", prior_family="add_sql_snippet_filter")
    assert _infer_prior_family_from_signatures([s]) == "add_sql_snippet_filter"


# ── next_patch_family_for_cluster prefers KIT_FOR_RCA ────────────────


def test_next_patch_family_prefers_kit_for_rca_when_available(
    monkeypatch,
) -> None:
    # Enable the trial 20 pivot graph (which gates the new branch).
    import genie_space_optimizer.optimization.trial20_flags as flags

    monkeypatch.setattr(
        flags, "trial20_family_pivot_graph_enabled", lambda: True
    )
    # join_semantics_wrong + lever-1 tried ⇒ pivot helper picks
    # lever-6's representative family rather than walking
    # _PIVOT_GRAPH from add_example_sql.
    s = build_terminal_signature(
        root_cause="join_semantics_wrong",
        blame_set=(),
        lever_set=(),
        target_qids=(),
        # KEPT_INSUFFICIENT is in _TERMINATIONS_REQUIRING_PIVOT so
        # the pivot helper takes the branch.
        terminal_reason=TerminalReason.KEPT_INSUFFICIENT,
        prior_lever_set=("lever-1",),
        prior_patch_family="add_column_description",
    )
    family = next_patch_family_for_cluster(
        cluster_id="c1",
        prior_terminal_signatures=[s],
        prior_patch_family="add_column_description",
    )
    assert family == "add_sql_snippet_filter"


def test_next_patch_family_falls_back_when_rca_outside_kit_map(
    monkeypatch,
) -> None:
    import genie_space_optimizer.optimization.trial20_flags as flags

    monkeypatch.setattr(
        flags, "trial20_family_pivot_graph_enabled", lambda: True
    )
    s = build_terminal_signature(
        root_cause="plural_top_n_collapse",
        blame_set=(),
        lever_set=(),
        target_qids=(),
        terminal_reason=TerminalReason.KEPT_INSUFFICIENT,
        prior_lever_set=("lever-5",),
        prior_patch_family="add_example_sql",
    )
    family = next_patch_family_for_cluster(
        cluster_id="c1",
        prior_terminal_signatures=[s],
        prior_patch_family="add_example_sql",
    )
    # _PIVOT_GRAPH["add_example_sql"] is the legacy fallback target.
    assert family == "add_sql_snippet_filter"
