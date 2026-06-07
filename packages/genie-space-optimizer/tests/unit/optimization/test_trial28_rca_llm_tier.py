"""Trial 28 W28.1 — RCA-canonicaliser tier-4 LLM wire-up.

The Trial 27 live replay confirmed the dominant kit-gate blocker: free-text
Stage-2 routing narratives left ~70% of `GSO_TRIAL26_RCA_CANONICAL_V1`
labels at `unknown_kind`, so `_kit_for_rca_companions` returned `None` and
`GSO_TRIAL24_KIT_FORCED_V1` never fired. W28.1 wires the owed tier-4 LLM
call so the canonicaliser can categorise the narrative against the closed
`RCA_CANONICAL_KEY_SET`, with deterministic code clamping the output.

These tests use a GENERIC (non-anchor) narrative and a mocked LLM, proving
the path generalises over any narrative without a real Foundation Model
endpoint or any per-QID / per-anchor literal.
"""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from genie_space_optimizer.optimization import rca_kind_canonical as rca
from genie_space_optimizer.optimization.prompt_io import LLMOutputContract
from genie_space_optimizer.optimization.trial28_flags import (
    trial28_kit_reachability_enabled,
    trial28_rca_llm_tier_enabled,
)

# A generic narrative that does NOT resolve via the deterministic / alias /
# keyword tiers (so resolution can only come from the LLM tier) and is not
# tied to any anchor space.
_FREETEXT_NARRATIVE = (
    "The routing analysis concluded the question needs a worked example to "
    "demonstrate the canonical ranking shape before Genie will emit the "
    "right query."
)


@pytest.fixture(autouse=True)
def _clear_cache():
    rca._reset_cache_for_tests()
    yield
    rca._reset_cache_for_tests()


def test_llm_tier_resolves_freetext_via_explicit_w(monkeypatch):
    """When a workspace client is supplied and the deterministic tiers
    miss, the LLM tier resolves the narrative to a canonical key."""
    monkeypatch.setattr(
        rca, "_invoke_llm_tier",
        lambda raw, *, w: ("top_n_cardinality_collapse", 0.91),
    )
    result = rca.canonicalise_rca_kind(_FREETEXT_NARRATIVE, w=object())
    assert result.canonical_key == "top_n_cardinality_collapse"
    assert result.via == "llm"
    assert result.confidence == pytest.approx(0.91)


def test_llm_tier_offcanonical_clamps_to_unknown(monkeypatch):
    """An off-canonical LLM answer is clamped to unknown_kind / llm_invalid
    — the deterministic closed-set guarantee, not the LLM, is authoritative.
    """
    monkeypatch.setattr(
        rca, "_invoke_llm_tier",
        lambda raw, *, w: ("totally_made_up_kind", 0.99),
    )
    result = rca.canonicalise_rca_kind(_FREETEXT_NARRATIVE, w=object())
    assert result.canonical_key == "unknown_kind"
    assert result.via == "llm_invalid"


def test_llm_tier_error_falls_through_to_llm_error(monkeypatch):
    """A raising LLM tier (decline / provider error) → via=llm_error,
    sentinel unknown_kind (never crashes the kit gate)."""
    def _boom(raw, *, w):
        raise RuntimeError("provider 429")

    monkeypatch.setattr(rca, "_invoke_llm_tier", _boom)
    result = rca.canonicalise_rca_kind(_FREETEXT_NARRATIVE, w=object())
    assert result.canonical_key == "unknown_kind"
    assert result.via == "llm_error"


def test_lazy_autoacquire_fires_when_enabled(monkeypatch):
    """W28.1 lazy acquire: with no explicit w but autoacquire enabled, the
    canonicaliser acquires a client and routes it to the LLM tier."""
    sentinel_ws = object()
    captured = {}

    monkeypatch.setattr(rca, "_w28_autoacquire_w", lambda: True)
    import genie_space_optimizer._workspace_client as wc

    monkeypatch.setattr(wc, "make_workspace_client", lambda **k: sentinel_ws)

    def _capture(raw, *, w):
        captured["w"] = w
        return ("wrong_column", 0.8)

    monkeypatch.setattr(rca, "_invoke_llm_tier", _capture)
    result = rca.canonicalise_rca_kind(_FREETEXT_NARRATIVE, w=None)
    assert captured["w"] is sentinel_ws
    assert result.canonical_key == "wrong_column"
    assert result.via == "llm"


def test_no_autoacquire_is_byte_stable_unknown(monkeypatch):
    """With autoacquire OFF (the default under pytest) and no explicit w,
    an unresolved narrative stays unknown_kind via=unknown — byte-stable
    with pre-Trial-28 behaviour, so the offline corpus never makes an LLM
    call."""
    monkeypatch.setattr(rca, "_w28_autoacquire_w", lambda: False)
    # Guard: _invoke_llm_tier must never be called on this path.
    monkeypatch.setattr(
        rca, "_invoke_llm_tier",
        lambda raw, *, w: pytest.fail("LLM tier must not fire"),
    )
    result = rca.canonicalise_rca_kind(_FREETEXT_NARRATIVE, w=None)
    assert result.canonical_key == "unknown_kind"
    assert result.via == "unknown"


def test_autoacquire_suppressed_under_pytest():
    """The pytest guard keeps autoacquire OFF inside the test suite even
    though the W28.1 flag defaults ON, so the 9875-test suite never makes
    a network call through the kit gate."""
    assert trial28_rca_llm_tier_enabled() is True  # default ON
    assert rca._w28_autoacquire_w() is False  # but suppressed under pytest


def test_master_flag_off_disables_subflag(monkeypatch):
    monkeypatch.setenv("GSO_TRIAL28_KIT_REACHABILITY", "0")
    assert trial28_kit_reachability_enabled() is False
    assert trial28_rca_llm_tier_enabled() is False


def test_invoke_llm_tier_builds_typed_request(monkeypatch):
    """The real _invoke_llm_tier builds a typed LlmReasoningRequest whose
    result_cls is an LLMOutputContract subclass and whose system prompt
    enumerates the closed canonical vocabulary, then returns
    (canonical_key, confidence) from the parsed envelope."""
    import genie_space_optimizer.optimization.llm_reasoning_call as lrc

    seen = {}

    def _fake_invoke(self, *, w, request):
        seen["request"] = request
        seen["w"] = w
        return SimpleNamespace(
            succeeded=True,
            parsed_output={"canonical_key": "join_semantics_wrong",
                           "confidence": 0.77},
            declined=None,
            error=None,
        )

    monkeypatch.setattr(lrc.LlmReasoningCall, "invoke", _fake_invoke)
    key, conf = rca._invoke_llm_tier(_FREETEXT_NARRATIVE, w=object())
    assert key == "join_semantics_wrong"
    assert conf == pytest.approx(0.77)
    req = seen["request"]
    assert issubclass(req.result_cls, LLMOutputContract)
    assert req.skill_id == "rca_kind_canonicalise"
    # System prompt enumerates the canonical vocabulary (generalisable —
    # derived from RCA_CANONICAL_KEY_SET, no anchor/QID literal).
    assert "top_n_cardinality_collapse" in req.system_msg
    assert "unknown_kind" in req.system_msg


def test_invoke_llm_tier_raises_on_decline(monkeypatch):
    import genie_space_optimizer.optimization.llm_reasoning_call as lrc

    def _fake_invoke(self, *, w, request):
        return SimpleNamespace(
            succeeded=False, parsed_output=None,
            declined="PROMPT_TOO_LARGE", error=None,
        )

    monkeypatch.setattr(lrc.LlmReasoningCall, "invoke", _fake_invoke)
    with pytest.raises(RuntimeError):
        rca._invoke_llm_tier(_FREETEXT_NARRATIVE, w=object())
