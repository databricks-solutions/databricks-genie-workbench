"""Trial 26 W26.1 — canonical RCA-kind normaliser.

Pins:

* Canonical set spans every kit map (base ``KIT_FOR_RCA``,
  ``_TRIAL24_KIT_FOR_RCA``, ``_TRIAL26_KIT_FOR_RCA``) plus the
  ``unknown_kind`` sentinel.
* Deterministic tier ladder: exact-canonical → alias → keyword regex
  → optional LLM (when ``w`` is provided AND the W26.1 sub-flag is ON)
  → ``unknown_kind``.
* Result tuple is always typed: ``RcaKindCanonical(canonical_key,
  confidence, via, raw_label)``.
* ``GSO_TRIAL26_RCA_CANONICAL_V1`` marker emitted on every call when
  the W26.1 sub-flag is ON; never emitted when the sub-flag is OFF.
* Sub-flag OFF returns ``via="disabled"`` and falls back to the legacy
  ``.strip().lower()`` behaviour byte-stably.
* Master flag OFF forces sub-flag OFF (single emergency rollback knob).
"""
from __future__ import annotations

import json
import re
from typing import Any
from unittest.mock import MagicMock

import pytest


@pytest.fixture(autouse=True)
def _clear_canonicaliser_cache():
    """The canonicaliser memoises results per process. Each test gets
    a clean cache so flag mutations are observable.
    """
    from genie_space_optimizer.optimization import rca_kind_canonical

    rca_kind_canonical._reset_cache_for_tests()
    yield
    rca_kind_canonical._reset_cache_for_tests()


# ---------------------------------------------------------------------------
# Canonical key set: spans every kit map + unknown_kind sentinel
# ---------------------------------------------------------------------------


def test_canonical_key_set_spans_every_kit_map():
    from genie_space_optimizer.optimization.rca_kind_canonical import (
        RCA_CANONICAL_KEY_SET,
    )
    from genie_space_optimizer.optimization.stages.action_groups import (
        KIT_FOR_RCA,
        _TRIAL24_KIT_FOR_RCA,
        _TRIAL26_KIT_FOR_RCA,
    )

    # Every key in every kit map must be canonical.
    for key in KIT_FOR_RCA:
        assert key in RCA_CANONICAL_KEY_SET, f"{key} (base) not canonical"
    for key in _TRIAL24_KIT_FOR_RCA:
        assert key in RCA_CANONICAL_KEY_SET, f"{key} (T24) not canonical"
    for key in _TRIAL26_KIT_FOR_RCA:
        assert key in RCA_CANONICAL_KEY_SET, f"{key} (T26) not canonical"
    # The sentinel is always part of the canonical set.
    assert "unknown_kind" in RCA_CANONICAL_KEY_SET


# ---------------------------------------------------------------------------
# Tier 1 — exact canonical-key passthrough
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_label",
    [
        "extra_defensive_filter",
        "top_n_cardinality_collapse",
        "wrong_aggregation",
        "wrong_column",
        "join_semantics_wrong",
        "EXTRA_DEFENSIVE_FILTER",  # case-insensitive
        " top_n_cardinality_collapse ",  # whitespace
    ],
)
def test_tier1_canonical_passthrough(raw_label, monkeypatch):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    result = canonicalise_rca_kind(raw_label)

    assert result.canonical_key == raw_label.strip().lower()
    assert result.via in {"deterministic", "alias"}
    assert result.confidence >= 0.9
    assert result.raw_label == raw_label


# ---------------------------------------------------------------------------
# Tier 2 — alias table
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_label, expected",
    [
        ("top_n_collapse", "top_n_cardinality_collapse"),
        ("plural_top_n_collapse", "top_n_cardinality_collapse"),
        ("defensive_filter", "extra_defensive_filter"),
        ("column_disambig", "column_disambiguation"),
    ],
)
def test_tier2_alias_resolution(monkeypatch, raw_label, expected):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    result = canonicalise_rca_kind(raw_label)
    assert result.canonical_key == expected
    assert result.via == "alias"
    assert result.confidence >= 0.9


# ---------------------------------------------------------------------------
# Tier 3 — keyword regex on English-text labels
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw_label, expected",
    [
        (
            # The exact 7now production label called out in the Trial 26 plan
            "Top-N cardinality collapse via spurious RANK()=1 filter",
            "top_n_cardinality_collapse",
        ),
        (
            "top-N collapsed to single row",
            "top_n_cardinality_collapse",
        ),
        (
            "Wrong aggregation — used COUNT instead of SUM",
            "wrong_aggregation",
        ),
        (
            "wrong column selected for revenue measure",
            "wrong_column",
        ),
        (
            "defensive filter dropped wrong rows",
            "extra_defensive_filter",
        ),
        (
            "time grain wrong — month instead of day",
            "time_grain_wrong",
        ),
        (
            "join semantics wrong between orders and customers",
            "join_semantics_wrong",
        ),
        (
            "column disambiguation needed for revenue",
            "column_disambiguation",
        ),
    ],
)
def test_tier3_keyword_regex_for_english_labels(
    monkeypatch, raw_label, expected
):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    result = canonicalise_rca_kind(raw_label)
    assert result.canonical_key == expected, (
        f"keyword tier failed: {raw_label!r} → {result.canonical_key!r} "
        f"(expected {expected!r})"
    )
    assert result.via == "keyword"
    assert 0.7 <= result.confidence < 1.0


# ---------------------------------------------------------------------------
# Tier 4 — LLM fallback (mocked) when deterministic returns unknown_kind
# ---------------------------------------------------------------------------


def test_tier4_llm_called_when_deterministic_fails(monkeypatch):
    """When no deterministic tier resolves AND ``w`` is available, the
    LLM tier fires; result carries ``via="llm"``.
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization import rca_kind_canonical

    def _fake_llm(raw_label: str, *, w: Any) -> tuple[str, float]:
        # Inject any deterministic-failing label here.
        return ("wrong_aggregation", 0.82)

    monkeypatch.setattr(
        rca_kind_canonical, "_invoke_llm_tier", _fake_llm
    )

    result = rca_kind_canonical.canonicalise_rca_kind(
        "Some bizarre never-seen-before label that no regex knows about",
        w=MagicMock(),
    )
    assert result.canonical_key == "wrong_aggregation"
    assert result.via == "llm"
    assert result.confidence == pytest.approx(0.82)


def test_tier4_llm_returning_off_canonical_is_clamped_to_unknown(
    monkeypatch,
):
    """If the LLM returns a value outside the canonical set, the
    normaliser clamps to ``unknown_kind`` instead of trusting the LLM
    (defense in depth — the canonical set is the source of truth).
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization import rca_kind_canonical

    monkeypatch.setattr(
        rca_kind_canonical,
        "_invoke_llm_tier",
        lambda raw, *, w: ("totally_made_up_key", 0.9),
    )

    result = rca_kind_canonical.canonicalise_rca_kind(
        "another bizarre never-before-seen label",
        w=MagicMock(),
    )
    assert result.canonical_key == "unknown_kind"
    assert result.via == "llm_invalid"
    assert result.confidence == 0.0


def test_no_llm_when_w_is_none(monkeypatch):
    """Without ``w``, the LLM tier is skipped; deterministic-fail
    cascades straight to ``unknown_kind``.
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization import rca_kind_canonical

    # If the LLM tier IS called without a real client we'll know — it
    # would either raise or return something we did not mock.
    sentinel = {"n_calls": 0}

    def _spy(*a, **kw):
        sentinel["n_calls"] += 1
        return ("wrong_aggregation", 0.9)

    monkeypatch.setattr(rca_kind_canonical, "_invoke_llm_tier", _spy)

    result = rca_kind_canonical.canonicalise_rca_kind(
        "no signal at all here", w=None
    )
    assert sentinel["n_calls"] == 0
    assert result.canonical_key == "unknown_kind"
    assert result.via == "unknown"


def test_llm_exception_is_swallowed_returning_unknown(monkeypatch):
    """A raising LLM tier must not bubble out — the kit map should
    keep working with ``unknown_kind`` so downstream gates can fall
    through to the existing behaviour.
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization import rca_kind_canonical

    def _raises(raw_label, *, w):
        raise RuntimeError("LLM backend down")

    monkeypatch.setattr(rca_kind_canonical, "_invoke_llm_tier", _raises)

    result = rca_kind_canonical.canonicalise_rca_kind(
        "no signal", w=MagicMock()
    )
    assert result.canonical_key == "unknown_kind"
    assert result.via == "llm_error"
    assert result.confidence == 0.0


# ---------------------------------------------------------------------------
# Caching — repeated calls hit the cache (LLM not re-invoked)
# ---------------------------------------------------------------------------


def test_repeated_canonicalisation_is_cached(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization import rca_kind_canonical

    call_count = {"n": 0}

    def _counted(raw_label, *, w):
        call_count["n"] += 1
        return ("wrong_aggregation", 0.85)

    monkeypatch.setattr(rca_kind_canonical, "_invoke_llm_tier", _counted)

    raw = "some unique never-canonical label"
    a = rca_kind_canonical.canonicalise_rca_kind(raw, w=MagicMock())
    b = rca_kind_canonical.canonicalise_rca_kind(raw, w=MagicMock())
    c = rca_kind_canonical.canonicalise_rca_kind(raw, w=MagicMock())

    assert call_count["n"] == 1, "LLM tier must be invoked only once per raw label"
    assert a == b == c


# ---------------------------------------------------------------------------
# Marker emission
# ---------------------------------------------------------------------------


def test_marker_emitted_when_flag_on(monkeypatch, capsys):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    canonicalise_rca_kind("top-N collapsed to single row")
    out = capsys.readouterr().out

    assert "GSO_TRIAL26_RCA_CANONICAL_V1" in out
    match = re.search(r"GSO_TRIAL26_RCA_CANONICAL_V1\s+(\{[^\n]*\})", out)
    assert match, f"no payload found in:\n{out}"
    payload = json.loads(match.group(1))

    assert payload["raw_label"] == "top-N collapsed to single row"
    assert payload["canonical_key"] == "top_n_cardinality_collapse"
    assert payload["via"] == "keyword"
    assert "confidence" in payload


def test_no_marker_when_flag_off(monkeypatch, capsys):
    monkeypatch.setenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", "0")

    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    canonicalise_rca_kind("top-N collapsed to single row")
    out = capsys.readouterr().out

    assert "GSO_TRIAL26_RCA_CANONICAL_V1" not in out


# ---------------------------------------------------------------------------
# Flag-gating — byte-stable rollback
# ---------------------------------------------------------------------------


def test_subflag_off_disables_canonicalisation(monkeypatch):
    """When the W26.1 sub-flag is OFF, the normaliser performs only
    ``.strip().lower()`` and returns ``via="disabled"``.
    """
    monkeypatch.setenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", "0")

    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    result = canonicalise_rca_kind("Top-N cardinality collapse via X")
    assert result.canonical_key == "top-n cardinality collapse via x"
    assert result.via == "disabled"
    assert result.confidence == 0.0


def test_master_off_forces_subflag_off(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL26_KIT_GATE_REACHABLE", "0")
    monkeypatch.setenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", "1")

    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    result = canonicalise_rca_kind("top-N collapsed to single row")
    assert result.via == "disabled"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_empty_raw_label(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    result = canonicalise_rca_kind("")
    assert result.canonical_key == "unknown_kind"
    assert result.via == "empty"


def test_none_raw_label(monkeypatch):
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization.rca_kind_canonical import (
        canonicalise_rca_kind,
    )

    result = canonicalise_rca_kind(None)  # type: ignore[arg-type]
    assert result.canonical_key == "unknown_kind"
    assert result.via == "empty"


# ---------------------------------------------------------------------------
# Integration — kit-map normaliser uses the canonicaliser (no LLM)
# ---------------------------------------------------------------------------


def test_kit_map_normaliser_recognises_canonicalised_english(monkeypatch):
    """The W26.1 fix: an English label like the Trial 26 plan's
    7now example reaches the kit map as
    ``top_n_cardinality_collapse`` so the kit gate finally fires.
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_KIT_MAP_EXPANDED", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_KIT_AT_SOURCE", raising=False)
    monkeypatch.delenv("GSO_TRIAL24_FILTER_REMOVAL_SOLO", raising=False)

    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    companions = _kit_for_rca_companions(
        "Top-N cardinality collapse via spurious RANK()=1 filter"
    )
    # top_n_cardinality_collapse has Trial 24 kit {lever-6, lever-1}.
    assert companions == frozenset({"lever-6", "lever-1"})


def test_kit_map_normaliser_unknown_passes_through_when_canonical_unknown(
    monkeypatch,
):
    """``unknown_kind`` produced by the canonicaliser must NOT
    accidentally become a kit (defense — a typo or unmapped label
    should still trigger the legacy "no kit, single-lever admissible"
    path).
    """
    monkeypatch.delenv("GSO_TRIAL26_KIT_GATE_REACHABLE", raising=False)
    monkeypatch.delenv("GSO_TRIAL26_RCA_KIND_CANONICAL_NORMALISE", raising=False)

    from genie_space_optimizer.optimization.stages.action_groups import (
        _kit_for_rca_companions,
    )

    assert _kit_for_rca_companions("totally novel never-seen string") is None
