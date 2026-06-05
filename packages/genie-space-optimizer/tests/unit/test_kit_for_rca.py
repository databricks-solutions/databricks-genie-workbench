"""Phase 2 P2.2 — KIT_FOR_RCA mandatory-companion validator.

Pins:

  * The five mandated RCA kinds are in the map with non-empty
    companion sets.
  * Single-lever proposals on a mandated RCA emit the
    ``singleton`` violation signature.
  * Multi-lever proposals missing the companion set emit the
    ``no_companion`` violation signature.
  * Multi-lever proposals intersecting the companion set return ``""``
    (admissible).
  * RCAs outside the map return ``""`` regardless of kit size — the
    map is a positive contract, not a global mandate.
  * Case/whitespace are normalized — the Stage 1 enum is the
    canonical key but legacy rows survive.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.stages.action_groups import (
    KIT_FOR_RCA,
    kit_for_rca_violation_reason,
    next_companion_family_from_kit,
)


_MANDATED_RCAS = (
    "value_mapping_missing",
    "join_semantics_wrong",
    "time_grain_wrong",
    "column_disambiguation",
    "table_routing_wrong",
)


def test_kit_for_rca_covers_the_five_mandated_kinds() -> None:
    for rca in _MANDATED_RCAS:
        assert rca in KIT_FOR_RCA, f"missing kit contract for {rca}"
        assert KIT_FOR_RCA[rca], f"empty companion set for {rca}"


def test_singleton_kit_on_mandated_rca_emits_singleton_violation() -> None:
    sig = kit_for_rca_violation_reason(
        "value_mapping_missing", ["lever-5a"]
    )
    assert sig == "kit_for_rca_violation:rca=value_mapping_missing:singleton"


def test_kit_missing_companion_emits_no_companion_violation() -> None:
    # join_semantics_wrong demands one of {lever-1, lever-6}; lever-3
    # + lever-4 satisfies len>=2 but misses the companion set entirely.
    sig = kit_for_rca_violation_reason(
        "join_semantics_wrong", ["lever-3", "lever-4"]
    )
    assert sig == "kit_for_rca_violation:rca=join_semantics_wrong:no_companion"


def test_kit_with_one_companion_is_admissible() -> None:
    sig = kit_for_rca_violation_reason(
        "column_disambiguation", ["lever-1", "lever-3"]
    )
    assert sig == ""


def test_kit_with_full_companion_set_is_admissible() -> None:
    sig = kit_for_rca_violation_reason(
        "join_semantics_wrong", ["lever-1", "lever-6"]
    )
    assert sig == ""


def test_unmandated_rca_passes_regardless_of_kit_size() -> None:
    # ``plural_top_n_collapse`` is not in KIT_FOR_RCA — single-lever
    # is admissible.
    assert kit_for_rca_violation_reason(
        "plural_top_n_collapse", ["lever-5"]
    ) == ""
    assert kit_for_rca_violation_reason(
        "plural_top_n_collapse", []
    ) == ""


def test_empty_rca_kind_passes() -> None:
    # Defensive: missing RCA means we cannot enforce the contract.
    assert kit_for_rca_violation_reason("", ["lever-1"]) == ""
    assert kit_for_rca_violation_reason(None, ["lever-1"]) == ""


def test_rca_kind_is_normalized_to_lowercase_and_stripped() -> None:
    sig = kit_for_rca_violation_reason(
        "  Value_Mapping_Missing  ", ["lever-5a"]
    )
    assert sig.endswith(":singleton")
    assert "rca=value_mapping_missing" in sig


def test_blank_levers_are_filtered_before_counting() -> None:
    # A list of [lever-1, ""] is still a 1-element kit, so the
    # singleton rule fires on a mandated RCA.
    sig = kit_for_rca_violation_reason(
        "time_grain_wrong", ["lever-1", ""]
    )
    assert sig.endswith(":singleton")


def test_companion_set_intersection_uses_membership_not_equality() -> None:
    # value_mapping_missing companions: {lever-5b, lever-6, lever-5a}.
    # Kit ['lever-1', 'lever-5b'] satisfies the contract because
    # lever-5b is in the companion set, even though lever-1 is not.
    sig = kit_for_rca_violation_reason(
        "value_mapping_missing", ["lever-1", "lever-5b"]
    )
    assert sig == ""


# ── Trial 24 — Kit at Source extension (flag-gated) ───────────────────

_TRIAL24_RCAS = ("extra_defensive_filter", "top_n_cardinality_collapse")


def test_trial24_rcas_not_in_base_map() -> None:
    # The base constant must remain byte-stable — Trial 24 RCAs are
    # carried by the flag-gated extension, never the base map.
    for rca in _TRIAL24_RCAS:
        assert rca not in KIT_FOR_RCA


def test_trial24_rca_singleton_passes_when_flag_off(monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "0")
    # Flag-off: the e943 RCA has no kit contract, so the lone lever is
    # admissible (this is exactly the legacy gap Trial 24 closes).
    assert kit_for_rca_violation_reason(
        "extra_defensive_filter", ["lever-5a"]
    ) == ""


def test_trial24_rca_singleton_rejected_when_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    # Follow-on B reclassifies extra_defensive_filter as an instruction
    # solo by default; opt OUT of that here to pin the original kit
    # contract (a lone lever IS a singleton violation).
    monkeypatch.setenv("GSO_TRIAL24_FILTER_REMOVAL_SOLO", "0")
    sig = kit_for_rca_violation_reason(
        "extra_defensive_filter", ["lever-5a"]
    )
    assert sig == (
        "kit_for_rca_violation:rca=extra_defensive_filter:singleton"
    )


def test_trial24_rca_full_kit_admissible_when_flag_on(monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    assert kit_for_rca_violation_reason(
        "extra_defensive_filter", ["lever-5a", "lever-6"]
    ) == ""
    assert kit_for_rca_violation_reason(
        "top_n_cardinality_collapse", ["lever-6", "lever-1"]
    ) == ""


def test_trial24_rca_missing_companion_rejected_when_flag_on(
    monkeypatch,
) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    # top_n_cardinality_collapse demands {lever-6, lever-1}; a 2-lever
    # kit that misses both companions is a no_companion violation.
    sig = kit_for_rca_violation_reason(
        "top_n_cardinality_collapse", ["lever-3", "lever-4"]
    )
    assert sig == (
        "kit_for_rca_violation:rca=top_n_cardinality_collapse:no_companion"
    )


def test_trial24_next_companion_family_off_returns_empty(monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "0")
    # No kit contract flag-off -> caller falls back to legacy pivot graph.
    assert next_companion_family_from_kit(
        "extra_defensive_filter", []
    ) == ""


def test_trial24_next_companion_family_on_returns_family(monkeypatch) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    # Pin the kit pivot path: opt out of Follow-on B's solo
    # reclassification so extra_defensive_filter keeps its companion set.
    monkeypatch.setenv("GSO_TRIAL24_FILTER_REMOVAL_SOLO", "0")
    # Companions {lever-5a, lever-6}; nothing tried -> first in sorted
    # order is lever-5a -> add_instruction.
    fam = next_companion_family_from_kit("extra_defensive_filter", [])
    assert fam == "add_instruction"


def test_followon_b_extra_defensive_filter_solo_default_on(monkeypatch) -> None:
    # Follow-on B default ON (master on, sub-flag unset): the
    # filter-removal RCA is reclassified as an instruction solo, so a
    # lone lever is NOT a kit_for_rca violation and there is no forced
    # companion to pivot to.
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    monkeypatch.delenv("GSO_TRIAL24_FILTER_REMOVAL_SOLO", raising=False)
    assert kit_for_rca_violation_reason(
        "extra_defensive_filter", ["lever-5a"]
    ) == ""
    assert next_companion_family_from_kit("extra_defensive_filter", []) == ""
    # top_n_cardinality_collapse stays a kit: a lone lever still violates.
    assert kit_for_rca_violation_reason(
        "top_n_cardinality_collapse", ["lever-6"]
    ) == (
        "kit_for_rca_violation:rca=top_n_cardinality_collapse:singleton"
    )


@pytest.mark.parametrize("rca", _MANDATED_RCAS)
def test_trial24_flag_on_does_not_alter_base_rcas(monkeypatch, rca) -> None:
    monkeypatch.setenv("GSO_TRIAL24_KIT_AT_SOURCE", "1")
    # Base RCAs keep their base companion set even with the flag on.
    sig = kit_for_rca_violation_reason(rca, ["lever-5a"])
    assert sig.endswith(":singleton")
