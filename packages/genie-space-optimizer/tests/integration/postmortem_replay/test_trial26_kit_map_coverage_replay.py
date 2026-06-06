"""Trial 26 W26.4 — kit-map coverage replay (bright-line merge gate).

Pins: for every RCA kind Trial 26 newly covered, the kit gate
*actually fires* when called with the labels the live optimizer
produces — both canonical-key form AND English-text form.

Concretely:

  * ``wrong_aggregation``  (W26.2) — canonical key form
  * ``wrong_column``       (W26.2) — canonical key form
  * ``plural_top_n_collapse`` (W26.2 alias) — collapses to
    ``top_n_cardinality_collapse`` and the Trial-24 kit fires
  * ``"Top-N cardinality collapse via spurious RANK()=1 filter"``
    (W26.1 keyword tier on the production English label) — the
    Trial-24 kit fires
  * ``"wrong_aggregation (quantity vs revenue, COUNT vs COUNT DISTINCT)"``
    (W26.1 keyword tier on a real fixture label) — the W26.2 kit
    fires

Each row also emits the structured marker the postmortem watchdog
keys on (``GSO_TRIAL26_KIT_MAP_EXPANDED_V1`` for W26.2 hits and
``GSO_TRIAL26_RCA_CANONICAL_V1`` for W26.1 hits), proving the
observability surface is live.

This test does NOT exercise the slate compiler or the synthesizer —
those are covered by the dedicated Trial 24 replay suites. Its
single contract is the kit-map reachability — "does the kit gate
return a non-None kit for these labels?" — because the Trial 26 plan
established that the only thing keeping the kit-at-source mechanism
out of production was the labels never reaching a kit lookup.
"""
from __future__ import annotations

import json
import re

import pytest


# Each row: (raw_label, expected_companion_levers, marker_substring_or_none)
_COVERAGE_ROWS: tuple[tuple[str, frozenset[str], str | None], ...] = (
    # ── W26.2 — canonical-key form ────────────────────────────────────
    (
        "wrong_aggregation",
        frozenset({"lever-6", "lever-1"}),
        "GSO_TRIAL26_KIT_MAP_EXPANDED_V1",
    ),
    (
        "wrong_column",
        frozenset({"lever-1", "lever-5a"}),
        "GSO_TRIAL26_KIT_MAP_EXPANDED_V1",
    ),
    # ── W26.2 — alias form (plural_top_n_collapse → top_n_cardinality_collapse,
    # Trial 24 kit fires, NOT W26.2 kit) ──────────────────────────────
    (
        "plural_top_n_collapse",
        frozenset({"lever-6", "lever-1"}),
        None,
    ),
    # ── W26.1 — keyword tier on the production English labels ────────
    (
        "Top-N cardinality collapse via spurious RANK()=1 filter",
        frozenset({"lever-6", "lever-1"}),
        "GSO_TRIAL26_RCA_CANONICAL_V1",
    ),
    (
        "wrong_aggregation (quantity vs revenue, COUNT vs COUNT DISTINCT)",
        frozenset({"lever-6", "lever-1"}),
        "GSO_TRIAL26_RCA_CANONICAL_V1",
    ),
)


@pytest.fixture(autouse=True)
def _flag_defaults(monkeypatch):
    """Trial 26 sub-flags default ON; Trial 24 kit-at-source default ON.
    No env overrides — the kit gate must work out of the box.
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_KIT_AT_SOURCE", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_FILTER_REMOVAL_SOLO", raising=False)
    from genie_space_optimizer.optimization import rca_kind_canonical

    rca_kind_canonical._reset_cache_for_tests()
    yield
    rca_kind_canonical._reset_cache_for_tests()


@pytest.mark.parametrize(
    "raw_label,expected_companions,expected_marker_substring",
    _COVERAGE_ROWS,
    ids=[row[0][:60] for row in _COVERAGE_ROWS],
)
def test_kit_gate_reachable_for_trial26_label(
    raw_label,
    expected_companions,
    expected_marker_substring,
    capsys,
):
    """Every Trial 26 label must reach a non-None kit AND emit the
    expected observability marker. Together these prove the
    long-broken ``Trial 24 kit can never land in production`` failure
    chain is closed end-to-end.
    """
    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    companions = _kit_for_rca_companions(raw_label)

    assert companions == expected_companions, (
        f"Trial 26 kit-gate coverage regression for {raw_label!r}: "
        f"expected {set(expected_companions)} got "
        f"{set(companions) if companions else None}"
    )

    if expected_marker_substring:
        out = capsys.readouterr().out
        assert expected_marker_substring in out, (
            f"Trial 26 observability regression for {raw_label!r}: "
            f"expected marker {expected_marker_substring!r} in stdout; "
            f"got:\n{out}"
        )


def test_w26_2_marker_payload_well_formed_for_expanded_key(capsys):
    """The W26.2 marker the postmortem watchdog parses must include
    structured fields (``rca_kind`` + ``companion_levers``) so the
    automation reading the marker can route on the canonical key.
    """
    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    _kit_for_rca_companions("wrong_aggregation")
    out = capsys.readouterr().out

    match = re.search(
        r"GSO_TRIAL26_KIT_MAP_EXPANDED_V1\s+(\{[^\n]*\})", out
    )
    assert match, f"no W26.2 marker payload found in:\n{out}"

    payload = json.loads(match.group(1))
    assert payload["rca_kind"] == "wrong_aggregation"
    assert payload["companion_levers"] == ["lever-1", "lever-6"]


def test_w26_1_marker_payload_well_formed_for_english_label(capsys):
    """The W26.1 marker emitted on canonicalisation must carry the
    original ``raw_label`` AND the resolved ``canonical_key`` so
    postmortems can audit whether the canonicaliser is silently
    misrouting.
    """
    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    canonicalise_rca_kind(
        "Top-N cardinality collapse via spurious RANK()=1 filter"
    )
    out = capsys.readouterr().out

    match = re.search(
        r"GSO_TRIAL26_RCA_CANONICAL_V1\s+(\{[^\n]*\})", out
    )
    assert match, f"no W26.1 marker payload found in:\n{out}"

    payload = json.loads(match.group(1))
    assert payload["canonical_key"] == "top_n_cardinality_collapse"
    assert (
        payload["raw_label"]
        == "Top-N cardinality collapse via spurious RANK()=1 filter"
    )
    assert payload["via"] == "keyword"
    assert payload["confidence"] > 0.7


def test_legacy_unmandated_label_still_returns_none(capsys):
    """The negative bar — a label genuinely outside Trial 26's
    coverage must still fall through to ``None`` so the
    P2.2 kit-violation gate does not hard-reject a justified
    single-lever proposal.
    """
    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    assert _kit_for_rca_companions("unmandated_demo_rca") is None
    # And no W26.2 marker should fire for this miss.
    out = capsys.readouterr().out
    assert "GSO_TRIAL26_KIT_MAP_EXPANDED_V1" not in out
