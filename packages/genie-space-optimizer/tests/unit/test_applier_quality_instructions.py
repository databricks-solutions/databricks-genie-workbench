"""Applier quality-instruction plumbing (D1-D3, baseline-eval-fix plan)."""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization import applier


def _read_general_text(config: dict) -> str:
    return applier._get_general_instructions(config)


def _fresh_config(seed_text: str = "") -> dict:
    """Build a minimal Genie-space-like config dict.

    ``_set_general_instructions`` fills in a generated id if the existing
    instruction block is empty; we only need a single valid text_instructions
    entry so the helpers have somewhere to write.
    """
    cfg: dict = {"instructions": {"text_instructions": []}}
    if seed_text:
        applier._set_general_instructions(cfg, seed_text)
    return cfg


# ─────────────────────────────────────────────────────────────────────────────
# Default-on: the three sentinel blocks are written exactly once.
# ─────────────────────────────────────────────────────────────────────────────
def test_default_on_writes_all_three_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config()
    changed = applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)
    assert changed is True
    for key, _body in applier._GSO_QUALITY_V1_BLOCKS:
        assert f"-- BEGIN GSO_QUALITY_V1:{key}" in text
        assert f"-- END GSO_QUALITY_V1:{key}" in text


def test_reapply_is_idempotent_no_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two consecutive applies must not duplicate any block."""
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config()
    applier.apply_gso_quality_instructions(cfg)
    first = _read_general_text(cfg)
    changed = applier.apply_gso_quality_instructions(cfg)
    second = _read_general_text(cfg)

    assert changed is False
    assert first == second
    for key, _body in applier._GSO_QUALITY_V1_BLOCKS:
        assert first.count(f"-- BEGIN GSO_QUALITY_V1:{key}") == 1


def test_customer_text_preserved_around_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config("Customer preamble\nKeep me intact.")
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)
    assert text.startswith("Customer preamble")
    assert "Keep me intact." in text
    assert "-- BEGIN GSO_QUALITY_V1:mv_preference" in text


def test_flag_off_strips_existing_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Flip to off + re-apply should remove all sentinel blocks."""
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config("Customer preamble")
    applier.apply_gso_quality_instructions(cfg)
    assert "GSO_QUALITY_V1" in _read_general_text(cfg)

    monkeypatch.setenv("GSO_APPLY_QUALITY_INSTRUCTIONS", "off")
    changed = applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)
    assert changed is True
    assert "GSO_QUALITY_V1" not in text
    assert "Customer preamble" in text  # customer text preserved


def test_flag_off_is_noop_when_no_blocks_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_APPLY_QUALITY_INSTRUCTIONS", "off")
    cfg = _fresh_config("Customer preamble")
    changed = applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)
    assert changed is False
    assert "GSO_QUALITY_V1" not in text
    assert text.strip().startswith("Customer preamble")


def test_flag_off_then_on_restores_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Revert/restore roundtrip — supports continuous rollout."""
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config()
    applier.apply_gso_quality_instructions(cfg)

    monkeypatch.setenv("GSO_APPLY_QUALITY_INSTRUCTIONS", "off")
    applier.apply_gso_quality_instructions(cfg)
    assert "GSO_QUALITY_V1" not in _read_general_text(cfg)

    monkeypatch.setenv("GSO_APPLY_QUALITY_INSTRUCTIONS", "on")
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)
    for key, _body in applier._GSO_QUALITY_V1_BLOCKS:
        assert f"-- BEGIN GSO_QUALITY_V1:{key}" in text


def test_block_body_content_matches_plan_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard: all three plan instructions must be rendered."""
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config()
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)
    assert "metric view `mv_*`" in text
    assert "by X, then Y" in text
    assert "NOW()" in text


# ─────────────────────────────────────────────────────────────────────────────
# strip helper is exposed as a primitive so reverts can be scripted from a
# notebook cell even if the applier isn't running.
# ─────────────────────────────────────────────────────────────────────────────
def test_strip_helper_handles_partial_manual_blocks() -> None:
    """Stripping must handle hand-edited / partially-formed blocks defensively."""
    seeded = (
        "Prologue\n\n"
        "-- BEGIN GSO_QUALITY_V1:mv_preference\n"
        "hand-edited body\n"
        "-- END GSO_QUALITY_V1:mv_preference\n"
        "Customer appendix"
    )
    stripped = applier._strip_gso_quality_blocks(seeded)
    assert "GSO_QUALITY_V1" not in stripped
    assert "Prologue" in stripped
    assert "Customer appendix" in stripped


def test_strip_is_noop_for_text_without_sentinels() -> None:
    original = "Plain text with no sentinel blocks."
    assert applier._strip_gso_quality_blocks(original) == original
