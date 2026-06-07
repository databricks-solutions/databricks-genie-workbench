"""Trial 26 W26.1 — canonical RCA-kind normaliser.

Free-form English RCA labels (``"Top-N cardinality collapse via
spurious RANK()=1 filter"``) flow out of Stage 1 diagnose into the
kit-map gate (``stages/action_groups._kit_for_rca_companions``). The
gate only recognises canonical keys (``"top_n_cardinality_collapse"``).
When the English label does not collapse to a canonical key the kit
gate returns ``None`` and the Trial 24 / Trial 26 kit-at-source
mechanism never fires.

This module is a four-tier canonicaliser:

  1. ``deterministic`` — input already a canonical key.
  2. ``alias``         — input is a known shorthand (e.g.
                         ``"top_n_collapse"``).
  3. ``keyword``       — input matches a curated regex (catches the
                         most common English phrasings).
  4. ``llm``           — optional last-resort LLM call (only when ``w``
                         is provided AND the W26.1 sub-flag is ON);
                         clamped to the canonical set on output.

When nothing resolves, ``canonical_key`` is the ``"unknown_kind"``
sentinel and ``via`` records which tier produced that result
(``"unknown"`` / ``"llm_error"`` / ``"llm_invalid"`` / ``"empty"``).

The W26.1 sub-flag controls the entire pipeline. When OFF the
function returns ``via="disabled"`` and falls back to the legacy
``.strip().lower()`` behaviour byte-stably; the kit map then continues
to do its own (much weaker) lookup. The master Trial 26 flag forces
all sub-flags OFF when itself OFF — a single emergency rollback knob.

Results are memoised per process (the set of distinct RCA labels per
run is small) so the LLM tier costs at most one call per distinct raw
label. A structured ``GSO_TRIAL26_RCA_CANONICAL_V1`` marker is emitted
on every call when the sub-flag is ON for observability.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import os
import re
from typing import Any


# ─────────────────────────────────────────────────────────────────────
# Canonical key set — single source of truth.
#
# Built from the union of every kit map in
# ``stages.action_groups`` plus the ``unknown_kind`` sentinel. Imported
# lazily so the module load order matches the rest of the optimization
# package (action_groups imports trial26_flags which imports nothing
# heavy).
# ─────────────────────────────────────────────────────────────────────
def _canonical_key_set() -> frozenset[str]:
    from genie_space_optimizer.optimization.stages.action_groups import (
        KIT_FOR_RCA,
        _TRIAL24_KIT_FOR_RCA,
        _TRIAL26_KIT_FOR_RCA,
    )

    keys: set[str] = set()
    keys.update(KIT_FOR_RCA.keys())
    keys.update(_TRIAL24_KIT_FOR_RCA.keys())
    keys.update(_TRIAL26_KIT_FOR_RCA.keys())
    keys.add("unknown_kind")
    return frozenset(keys)


RCA_CANONICAL_KEY_SET: frozenset[str] = _canonical_key_set()


# ─────────────────────────────────────────────────────────────────────
# Aliases — shorthand keys that map onto canonical ones.
# ─────────────────────────────────────────────────────────────────────
_RCA_ALIASES: dict[str, str] = {
    "top_n_collapse": "top_n_cardinality_collapse",
    "plural_top_n_collapse": "top_n_cardinality_collapse",
    "defensive_filter": "extra_defensive_filter",
    "col_disambig": "column_disambiguation",
    "column_disambig": "column_disambiguation",
}


# ─────────────────────────────────────────────────────────────────────
# Keyword regex tier — anchored, ordered by specificity.
#
# Order matters: the first match wins, so the more specific patterns
# (e.g. top-N cardinality) must come before the generic ones (e.g.
# wrong column). Patterns use ``\W`` boundaries because the production
# labels mix punctuation freely.
# ─────────────────────────────────────────────────────────────────────
_KEYWORD_PATTERNS: tuple[tuple[re.Pattern[str], str], ...] = (
    # Top-N cardinality collapse (single permissive pattern — the
    # trailing alternations omit ``\b`` anchors deliberately so
    # inflections like "collapsed" / "ranking" / "windowed" still
    # match. Followed by a second variant that catches the bare
    # "plural top-N collapse" shorthand the live producer emits.)
    (
        re.compile(
            r"\btop[\s\W]*n\b.{0,60}"
            r"(collaps|cardinality|rank|row[\s_-]?number|window)",
            re.I,
        ),
        "top_n_cardinality_collapse",
    ),
    (
        re.compile(r"\bplural[\s\W]*top[\s\W]*n[\s\W]*collaps", re.I),
        "top_n_cardinality_collapse",
    ),
    # Extra defensive filter
    (
        re.compile(r"\b(extra|spurious|over[\s-]?reach|defensive)\b.{0,30}\bfilter", re.I),
        "extra_defensive_filter",
    ),
    (
        re.compile(r"\bfilter\b.{0,30}\b(dropp\w+|excludes?|too[\s-]?strict)", re.I),
        "extra_defensive_filter",
    ),
    # Wrong aggregation — handle both English ("wrong aggregation")
    # and snake_case ("wrong_aggregation") with arbitrary trailing
    # context ("wrong_aggregation (quantity vs revenue, ...)").
    (
        re.compile(r"\b(wrong|incorrect|bad)[\s_-]+aggreg", re.I),
        "wrong_aggregation",
    ),
    (
        re.compile(r"\b(wrong|incorrect|bad)\b.{0,15}\baggreg", re.I),
        "wrong_aggregation",
    ),
    (
        re.compile(
            r"\b(used|using|chose|picked|selected)\b.{0,15}\b"
            r"(count|sum|avg|min|max|distinct|approx)\b.{0,20}\b"
            r"(instead|should be|but should|expected)\b",
            re.I,
        ),
        "wrong_aggregation",
    ),
    # Time grain wrong
    (
        re.compile(
            r"\btime[\s_-]*grain\b.{0,20}\b(wrong|mismatch|incorrect)\b", re.I
        ),
        "time_grain_wrong",
    ),
    (
        re.compile(
            r"\b(day|hour|month|year|week|quarter)\b.{0,15}\b(instead|but)\b"
            r".{0,15}\b(day|hour|month|year|week|quarter)\b",
            re.I,
        ),
        "time_grain_wrong",
    ),
    # Join semantics wrong
    (
        re.compile(r"\bjoin\b.{0,15}\b(wrong|semantics?|incorrect)\b", re.I),
        "join_semantics_wrong",
    ),
    (
        re.compile(r"\b(inner|left|right|outer|cross)\b.{0,5}\bjoin\b.{0,30}\b(wrong|mismatch)", re.I),
        "join_semantics_wrong",
    ),
    (
        # Production label shape: ``wrong_join_spec`` / ``wrong-join``
        # — no trailing word-boundary so the snake_case ``_spec`` /
        # ``_predicate`` suffixes do not block the match.
        re.compile(r"\bwrong[\s_-]+join", re.I),
        "join_semantics_wrong",
    ),
    # Column disambiguation
    (
        re.compile(r"\bcolumn\b.{0,15}\b(disambig|ambig)\w*", re.I),
        "column_disambiguation",
    ),
    (
        re.compile(r"\bambiguous\b.{0,15}\bcolumn", re.I),
        "column_disambiguation",
    ),
    # Wrong column (must come AFTER column_disambiguation so the more
    # specific pattern wins for ambiguous-column phrasings)
    (
        re.compile(r"\b(wrong|incorrect|bad)\b.{0,15}\bcolumn\b", re.I),
        "wrong_column",
    ),
    (
        re.compile(r"\bcolumn\b.{0,20}\b(swap|mistaken|misidentif|misroute|misuse)", re.I),
        "wrong_column",
    ),
    # Table routing wrong
    (
        re.compile(r"\btable\b.{0,15}\b(routing|routed|swap)\b.{0,15}\b(wrong|incorrect)?", re.I),
        "table_routing_wrong",
    ),
    # Value mapping
    (
        re.compile(r"\bvalue\b.{0,15}\b(mapping|map)\b.{0,15}\b(missing|wrong)\b", re.I),
        "value_mapping_missing",
    ),
)


# ─────────────────────────────────────────────────────────────────────
# Result type — typed, deterministic, JSON-friendly.
# ─────────────────────────────────────────────────────────────────────
@dataclasses.dataclass(frozen=True)
class RcaKindCanonical:
    """Outcome of a single canonicalisation call.

    ``via`` records which tier produced the result:

    * ``deterministic`` — input was already a canonical key.
    * ``alias``         — known shorthand resolved to canonical.
    * ``keyword``       — keyword regex matched English label.
    * ``llm``           — LLM tier produced a canonical key.
    * ``llm_invalid``   — LLM tier returned an off-canonical value;
                          clamped to ``unknown_kind``.
    * ``llm_error``     — LLM tier raised; clamped to ``unknown_kind``.
    * ``unknown``       — no tier resolved; ``w`` was None so the LLM
                          tier was skipped.
    * ``empty``         — raw label was empty / None.
    * ``disabled``      — W26.1 sub-flag is OFF; legacy fallback.
    """

    canonical_key: str
    confidence: float
    via: str
    raw_label: str

    def to_json(self) -> dict:
        return {
            "canonical_key": self.canonical_key,
            "confidence": float(self.confidence),
            "via": self.via,
            "raw_label": self.raw_label,
        }


# ─────────────────────────────────────────────────────────────────────
# Per-process cache.
#
# The set of distinct raw RCA labels per optimization run is small;
# memoisation keeps the LLM tier bounded to one call per distinct
# label. The cache is keyed by ``(raw_label, has_w)`` so calls with
# and without a workspace client cannot mask each other.
# ─────────────────────────────────────────────────────────────────────
_CACHE: dict[tuple[str, bool], RcaKindCanonical] = {}


def _reset_cache_for_tests() -> None:
    """Test-only hook to clear the per-process cache. Production code
    should never need to call this.
    """
    _CACHE.clear()


# ─────────────────────────────────────────────────────────────────────
# Tier 4 — LLM (extracted so tests can monkeypatch).
# ─────────────────────────────────────────────────────────────────────
_LLM_OUTPUT_CLS: Any | None = None


def _rca_llm_output_cls() -> type:
    """Lazily build (once) the typed closed output contract for the
    LLM tier: ``canonical_key`` + ``confidence``.

    Built lazily so importing this module never forces
    :mod:`prompt_io` at import time (the canonicaliser is imported
    very early by the kit gate). The closed-set guarantee is enforced
    deterministically by :func:`canonicalise_rca_kind` clamping the
    returned ``canonical_key`` against :data:`RCA_CANONICAL_KEY_SET`;
    the contract here is the typed module-boundary (a Pydantic
    ``LLMOutputContract`` subclass, never ``dict[str, Any]``).
    """
    global _LLM_OUTPUT_CLS
    if _LLM_OUTPUT_CLS is None:
        from genie_space_optimizer.optimization.prompt_io import (
            LLMOutputContract,
        )

        class _RcaCanonicalLlmOutput(LLMOutputContract):
            canonical_key: str
            confidence: float

        _LLM_OUTPUT_CLS = _RcaCanonicalLlmOutput
    return _LLM_OUTPUT_CLS


def _build_llm_system_prompt() -> str:
    """Render the system prompt enumerating the closed canonical-key
    vocabulary. Derived from :data:`RCA_CANONICAL_KEY_SET` so the
    prompt and the kit-map vocabulary never drift (no per-anchor /
    per-QID literal — the model generalises over any narrative)."""
    keys = sorted(k for k in RCA_CANONICAL_KEY_SET if k != "unknown_kind")
    listed = "\n".join(f"  - {k}" for k in keys)
    return (
        "You are a root-cause-analysis (RCA) label normaliser for a "
        "Genie Space optimizer. You receive a free-text RCA narrative "
        "describing why a natural-language question produced incorrect "
        "SQL. Map it to EXACTLY ONE canonical RCA kind from this closed "
        "set (verbatim), or 'unknown_kind' when none genuinely fits:\n"
        f"{listed}\n  - unknown_kind\n\n"
        "The narrative is often routing prose that embeds the real "
        "defect (e.g. \"SQL shape: example SQL needed for "
        "ranking/comparison patterns\" describes a "
        "top_n_cardinality_collapse). Reason about the underlying SQL "
        "defect, NOT the lever or mechanism the narrative names. Emit "
        "canonical_key (one listed value, exactly) and a confidence in "
        "[0,1]. When genuinely ambiguous or out of domain, return "
        "canonical_key='unknown_kind' with a low confidence."
    )


def _invoke_llm_tier(raw_label: str, *, w: Any) -> tuple[str, float]:
    """Trial 28 W28.1 — tier-4 LLM categorisation of a free-text RCA
    narrative into the closed canonical-key vocabulary.

    Uses the standard typed-LLM infrastructure
    (:class:`LlmReasoningCall` + :class:`LlmReasoningRequest` with a
    :class:`LLMOutputContract` ``result_cls``). Returns
    ``(canonical_key, confidence)``; the caller
    (:func:`canonicalise_rca_kind`) deterministically clamps the key
    to :data:`RCA_CANONICAL_KEY_SET` (off-canonical → ``llm_invalid``).

    Raises on decline / provider error so the caller records
    ``via="llm_error"`` and falls through to the ``unknown_kind``
    sentinel. Tests monkeypatch this hook (or
    :class:`LlmReasoningCall`) to exercise the path without a real
    Foundation Model endpoint.
    """
    from genie_space_optimizer.optimization.llm_reasoning_call import (
        LlmReasoningCall,
    )
    from genie_space_optimizer.optimization.llm_reasoning_io import (
        LlmReasoningRequest,
    )

    digest = hashlib.sha1(raw_label.encode("utf-8")).hexdigest()[:12]
    request = LlmReasoningRequest(
        call_id=f"rca_canon::{digest}",
        skill_id="rca_kind_canonicalise",
        system_msg=_build_llm_system_prompt(),
        user_prompt=f"RCA narrative:\n{raw_label}",
        result_cls=_rca_llm_output_cls(),
        max_tokens=256,
    )
    resp = LlmReasoningCall().invoke(w=w, request=request)
    if not resp.succeeded or not resp.parsed_output:
        raise RuntimeError(
            "rca canonicaliser LLM tier did not succeed: "
            f"declined={resp.declined} error={resp.error}"
        )
    key = str(resp.parsed_output.get("canonical_key", "")).strip()
    confidence = float(resp.parsed_output.get("confidence", 0.0) or 0.0)
    return key, confidence


def _w28_autoacquire_w() -> bool:
    """W28.1 — may the canonicaliser lazily acquire a workspace client
    for the LLM tier when the caller passed none?

    True only when the W28.1 sub-flag is ON **and** we are not inside
    a pytest run. The pytest guard keeps the entire offline suite
    byte-stable (the kit gate calls the canonicaliser with no ``w``;
    without this guard a default-ON flag would make every offline
    forward-pipeline test attempt a real Databricks call). Production
    (the lever_loop job, no ``PYTEST_CURRENT_TEST``) acquires the
    client and the LLM tier fires.
    """
    if "PYTEST_CURRENT_TEST" in os.environ:
        return False
    try:
        from genie_space_optimizer.optimization.trial28_flags import (
            trial28_rca_llm_tier_enabled,
        )

        return trial28_rca_llm_tier_enabled()
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────
# Flag plumbing.
# ─────────────────────────────────────────────────────────────────────
def _subflag_on() -> bool:
    """Lazy + safe accessor for the W26.1 sub-flag.

    Returns False on any import error so a broken trial26_flags module
    cannot regress the legacy (pre-Trial-26) baseline.
    """
    try:
        from genie_space_optimizer.optimization.trial26_flags import (
            trial26_rca_kind_canonical_normalise_enabled,
        )

        return trial26_rca_kind_canonical_normalise_enabled()
    except Exception:
        return False


# ─────────────────────────────────────────────────────────────────────
# Marker emission.
# ─────────────────────────────────────────────────────────────────────
def _emit_marker(result: RcaKindCanonical) -> None:
    payload = json.dumps(result.to_json(), separators=(",", ":"))
    print(f"GSO_TRIAL26_RCA_CANONICAL_V1 {payload}")


# ─────────────────────────────────────────────────────────────────────
# Public surface.
# ─────────────────────────────────────────────────────────────────────
def canonicalise_rca_kind(
    raw_label: Any,
    *,
    w: Any | None = None,
) -> RcaKindCanonical:
    """Canonicalise a free-form RCA label.

    Always returns a typed :class:`RcaKindCanonical`. Never raises —
    LLM errors are swallowed and reported as ``via="llm_error"`` so
    downstream gates see a stable result and can fall through to their
    legacy behaviour.

    Args:
        raw_label: the free-form RCA label emitted by the Stage 1
            LLM, the canonicaliser's own alias table, or any
            downstream consumer that needs a canonical key.
        w: optional workspace client. When provided AND the W26.1
            sub-flag is ON, the LLM tier is invoked as last resort.

    Returns:
        :class:`RcaKindCanonical` whose ``canonical_key`` is
        guaranteed to be a member of :data:`RCA_CANONICAL_KEY_SET`.
    """
    # ── Sub-flag OFF → byte-stable legacy fallback ────────────────────
    if not _subflag_on():
        return RcaKindCanonical(
            canonical_key=(str(raw_label or "")).strip().lower(),
            confidence=0.0,
            via="disabled",
            raw_label=str(raw_label or ""),
        )

    # ── Empty / None input → sentinel ────────────────────────────────
    if raw_label is None or not str(raw_label).strip():
        result = RcaKindCanonical(
            canonical_key="unknown_kind",
            confidence=0.0,
            via="empty",
            raw_label=str(raw_label or ""),
        )
        _emit_marker(result)
        return result

    raw_str = str(raw_label)
    # W28.1: the cache key tracks whether the LLM tier was reachable
    # (explicit ``w`` OR lazy autoacquire), so a deterministic-only
    # resolution can never mask an LLM-tier resolution of the same
    # label (or vice versa) within one process.
    cache_key = (raw_str, w is not None or _w28_autoacquire_w())
    cached = _CACHE.get(cache_key)
    if cached is not None:
        # The marker has already been emitted for this label; do not
        # re-emit on cache hits (keeps the log signal tight).
        return cached

    normalised = raw_str.strip().lower()

    # ── Tier 1: exact canonical key ──────────────────────────────────
    if normalised in RCA_CANONICAL_KEY_SET:
        result = RcaKindCanonical(
            canonical_key=normalised,
            confidence=1.0,
            via="deterministic",
            raw_label=raw_str,
        )
        _CACHE[cache_key] = result
        _emit_marker(result)
        return result

    # ── Tier 2: alias table ──────────────────────────────────────────
    if normalised in _RCA_ALIASES:
        canonical = _RCA_ALIASES[normalised]
        # Defense: the alias target itself must be canonical.
        if canonical in RCA_CANONICAL_KEY_SET:
            result = RcaKindCanonical(
                canonical_key=canonical,
                confidence=0.95,
                via="alias",
                raw_label=raw_str,
            )
            _CACHE[cache_key] = result
            _emit_marker(result)
            return result

    # ── Tier 3: keyword regex ────────────────────────────────────────
    for pattern, canonical in _KEYWORD_PATTERNS:
        if pattern.search(raw_str):
            if canonical in RCA_CANONICAL_KEY_SET:
                result = RcaKindCanonical(
                    canonical_key=canonical,
                    confidence=0.85,
                    via="keyword",
                    raw_label=raw_str,
                )
                _CACHE[cache_key] = result
                _emit_marker(result)
                return result

    # ── Tier 4: LLM (explicit ``w``, or W28.1 lazily-acquired) ───────
    # W28.1: when no workspace client was supplied AND the sub-flag is
    # ON (and we are not under pytest), lazily acquire one so the kit
    # gate can resolve free-text narratives without threading ``w``
    # through every ``_normalize_rca_kind`` caller.
    w_eff = w
    if w_eff is None and _w28_autoacquire_w():
        try:
            from genie_space_optimizer._workspace_client import (
                make_workspace_client,
            )

            w_eff = make_workspace_client()
        except Exception:
            w_eff = None
    if w_eff is not None:
        try:
            canonical, confidence = _invoke_llm_tier(raw_str, w=w_eff)
        except Exception:
            result = RcaKindCanonical(
                canonical_key="unknown_kind",
                confidence=0.0,
                via="llm_error",
                raw_label=raw_str,
            )
            _CACHE[cache_key] = result
            _emit_marker(result)
            return result

        if canonical in RCA_CANONICAL_KEY_SET:
            result = RcaKindCanonical(
                canonical_key=canonical,
                confidence=float(confidence),
                via="llm",
                raw_label=raw_str,
            )
        else:
            result = RcaKindCanonical(
                canonical_key="unknown_kind",
                confidence=0.0,
                via="llm_invalid",
                raw_label=raw_str,
            )
        _CACHE[cache_key] = result
        _emit_marker(result)
        return result

    # ── No tier resolved AND no LLM available → unknown sentinel ─────
    result = RcaKindCanonical(
        canonical_key="unknown_kind",
        confidence=0.0,
        via="unknown",
        raw_label=raw_str,
    )
    _CACHE[cache_key] = result
    _emit_marker(result)
    return result


__all__ = [
    "RCA_CANONICAL_KEY_SET",
    "RcaKindCanonical",
    "canonicalise_rca_kind",
]
