"""Phase 3.3 + 3.4: canonical-header dedup pipeline.

Covers two guarantees:

1. ``_sanitize_plaintext_instructions`` is idempotent — already-canonical
   ALL-CAPS input passes through unchanged.
2. ``_canonicalize_and_dedup_instructions`` collapses parallel
   ``## CONSTRAINTS`` and ``CONSTRAINTS:`` blocks into one section,
   dedupes bullets within a section, and emits ALL-CAPS canonical form.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization import applier
from genie_space_optimizer.optimization.optimizer import (
    _is_already_canonical_plaintext,
    _sanitize_plaintext_instructions,
)


def _wrap_text(text: str) -> dict:
    """Build a minimal config with text_instructions = [text]."""
    cfg: dict = {
        "instructions": {
            "text_instructions": [
                {"id": "abc", "content": [text]},
            ],
        },
    }
    return cfg


def test_sanitizer_idempotent_on_canonical_text() -> None:
    canonical = (
        "PURPOSE:\n"
        "- Sales reporting space.\n"
        "\n"
        "CONSTRAINTS:\n"
        "- Use mv_sales for aggregates.\n"
    )
    assert _is_already_canonical_plaintext(canonical) is True
    out = _sanitize_plaintext_instructions(canonical)
    # Canonical input returns unchanged (modulo strip).
    assert out == canonical.strip()


def test_sanitizer_converts_markdown_headers() -> None:
    md = (
        "## PURPOSE\n"
        "- Sales reporting.\n"
        "\n"
        "## CONSTRAINTS\n"
        "- `mv_sales` is the metric view.\n"
    )
    out = _sanitize_plaintext_instructions(md)
    assert "PURPOSE:" in out
    assert "CONSTRAINTS:" in out
    assert "##" not in out
    assert "`" not in out


def test_dedup_collapses_dual_canonical_and_legacy_constraints() -> None:
    """Both ``## CONSTRAINTS`` and ``CONSTRAINTS:`` present => single section."""
    text = (
        "PURPOSE:\n"
        "- Sales space.\n"
        "\n"
        "## CONSTRAINTS\n"
        "- prefer mv_sales for aggregates\n"
        "\n"
        "CONSTRAINTS:\n"
        "- prefer mv_sales for aggregates\n"
        "- always join via location_number\n"
    )
    cfg = _wrap_text(text)
    rewrote = applier._canonicalize_and_dedup_instructions(cfg)
    assert rewrote is True

    final = cfg["instructions"]["text_instructions"][0]["content"][0]
    # Only one CONSTRAINTS section header (no ``## CONSTRAINTS``).
    assert final.count("CONSTRAINTS:") == 1
    assert "## CONSTRAINTS" not in final
    # Bullet-level dedup: the duplicate ``prefer mv_sales`` bullet
    # collapses to one occurrence.
    assert final.count("prefer mv_sales for aggregates") == 1
    # The unique bullet from the legacy block is preserved.
    assert "always join via location_number" in final


def test_dedup_idempotent_on_already_canonical() -> None:
    text = (
        "PURPOSE:\n"
        "- Sales space.\n"
        "\n"
        "CONSTRAINTS:\n"
        "- prefer mv_sales for aggregates\n"
    )
    cfg = _wrap_text(text)
    # First pass canonicalizes.
    applier._canonicalize_and_dedup_instructions(cfg)
    # Second pass should be a no-op (returns False).
    result = applier._canonicalize_and_dedup_instructions(cfg)
    assert result is False


def test_dedup_returns_false_for_empty_instructions() -> None:
    cfg: dict = {"instructions": {"text_instructions": []}}
    assert applier._canonicalize_and_dedup_instructions(cfg) is False
    cfg2: dict = {"instructions": {"text_instructions": [{"id": "x", "content": [""]}]}}
    assert applier._canonicalize_and_dedup_instructions(cfg2) is False


def test_canonicalize_enforces_purpose_first() -> None:
    """``CONSTRAINTS:`` written before ``PURPOSE:`` is reordered."""
    text = (
        "CONSTRAINTS:\n"
        "- prefer mv_sales for aggregates\n"
        "\n"
        "PURPOSE:\n"
        "- Sales reporting space.\n"
    )
    cfg = _wrap_text(text)
    rewrote = applier._canonicalize_and_dedup_instructions(cfg)
    assert rewrote is True

    final = cfg["instructions"]["text_instructions"][0]["content"][0]
    assert final.index("PURPOSE:") < final.index("CONSTRAINTS:")


def test_canonicalize_full_canonical_order() -> None:
    """All five canonical sections written in reverse come out in canonical order."""
    text = (
        "INSTRUCTIONS YOU MUST FOLLOW WHEN PROVIDING SUMMARIES:\n"
        "- Always state the date range.\n"
        "\n"
        "CONSTRAINTS:\n"
        "- prefer mv_sales for aggregates\n"
        "\n"
        "DATA QUALITY NOTES:\n"
        "- Use NOW() for current-period filtering.\n"
        "\n"
        "DISAMBIGUATION:\n"
        "- market refers to market_combination.\n"
        "\n"
        "PURPOSE:\n"
        "- Sales reporting space.\n"
    )
    cfg = _wrap_text(text)
    rewrote = applier._canonicalize_and_dedup_instructions(cfg)
    assert rewrote is True

    final = cfg["instructions"]["text_instructions"][0]["content"][0]
    expected_order = [
        "PURPOSE:",
        "DISAMBIGUATION:",
        "DATA QUALITY NOTES:",
        "CONSTRAINTS:",
        "INSTRUCTIONS YOU MUST FOLLOW WHEN PROVIDING SUMMARIES:",
    ]
    indices = [final.index(h) for h in expected_order]
    assert indices == sorted(indices), (
        f"Sections out of canonical order. Got: {final!r}"
    )


def test_canonicalize_preserves_non_canonical_position() -> None:
    """Non-canonical sections retain relative order and follow the canonical block."""
    text = (
        "CONSTRAINTS:\n"
        "- prefer mv_sales for aggregates\n"
        "\n"
        "MARKET DEFINITIONS:\n"
        "- North America = US + Canada.\n"
        "\n"
        "PURPOSE:\n"
        "- Sales reporting space.\n"
    )
    cfg = _wrap_text(text)
    rewrote = applier._canonicalize_and_dedup_instructions(cfg)
    assert rewrote is True

    final = cfg["instructions"]["text_instructions"][0]["content"][0]
    purpose_idx = final.index("PURPOSE:")
    constraints_idx = final.index("CONSTRAINTS:")
    market_idx = final.index("MARKET DEFINITIONS:")
    # Canonical order: PURPOSE before CONSTRAINTS.
    assert purpose_idx < constraints_idx
    # Non-canonical "MARKET DEFINITIONS" lands after the canonical block.
    assert constraints_idx < market_idx


def test_canonicalize_idempotent_when_already_canonical() -> None:
    """Already-canonical input with all five sections is unchanged on second run."""
    text = (
        "PURPOSE:\n"
        "- Sales reporting space.\n"
        "\n"
        "DISAMBIGUATION:\n"
        "- market refers to market_combination.\n"
        "\n"
        "DATA QUALITY NOTES:\n"
        "- Use NOW() for current-period filtering.\n"
        "\n"
        "CONSTRAINTS:\n"
        "- prefer mv_sales for aggregates\n"
        "\n"
        "INSTRUCTIONS YOU MUST FOLLOW WHEN PROVIDING SUMMARIES:\n"
        "- Always state the date range.\n"
    )
    cfg = _wrap_text(text)
    applier._canonicalize_and_dedup_instructions(cfg)
    assert applier._canonicalize_and_dedup_instructions(cfg) is False
