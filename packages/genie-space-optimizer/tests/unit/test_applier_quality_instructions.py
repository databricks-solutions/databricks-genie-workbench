"""Applier quality-instruction plumbing (D1-D3, baseline-eval-fix plan).

Option C rewrite: policy bullets land under their canonical ``##``
section without any customer-visible markers. Identification across
runs is exact-text match against a known-body allowlist scoped to the
policy's target header.
"""

from __future__ import annotations

import pytest

from genie_space_optimizer.optimization import applier


def _read_general_text(config: dict) -> str:
    return applier._get_general_instructions(config)


def _section_lines(text: str, header: str) -> list[str]:
    canonical, _legacy, _preamble = applier.parse_canonical_sections(text)
    return list(canonical.get(header, []))


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
# Default-on: every policy lands as a bullet under its target ## section,
# with no sentinels or other markers visible anywhere in the prose.
# ─────────────────────────────────────────────────────────────────────────────
def test_apply_inserts_bullets_under_canonical_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config()
    changed = applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)

    assert changed is True
    assert "GSO_QUALITY_V1" not in text
    assert "-- BEGIN" not in text
    assert "-- END" not in text

    for _key, header, body in applier._GSO_QUALITY_V1_POLICIES:
        bodies_in_section = {
            applier._bullet_text(ln) for ln in _section_lines(text, header)
        }
        assert body in bodies_in_section, (
            f"policy body not found under {header!r}: {body!r} "
            f"(got lines: {_section_lines(text, header)})"
        )


def test_reapply_is_idempotent_no_duplicates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two consecutive applies must not duplicate any policy bullet."""
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config()
    applier.apply_gso_quality_instructions(cfg)
    first = _read_general_text(cfg)
    changed = applier.apply_gso_quality_instructions(cfg)
    second = _read_general_text(cfg)

    assert changed is False
    assert first == second
    for _key, header, body in applier._GSO_QUALITY_V1_POLICIES:
        matches = [
            ln for ln in _section_lines(first, header)
            if applier._bullet_text(ln) == body
        ]
        assert len(matches) == 1


def test_customer_text_preserved_around_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Customer preamble + hand-written bullets survive apply and re-apply."""
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    seed = (
        "Customer preamble\n"
        "Keep me intact.\n"
        "\n"
        "## CONSTRAINTS\n"
        "- Always JOIN on account_id.\n"
    )
    cfg = _fresh_config(seed)
    applier.apply_gso_quality_instructions(cfg)
    text_after_first = _read_general_text(cfg)

    _canonical, _legacy, preamble = applier.parse_canonical_sections(
        text_after_first,
    )
    preamble_joined = "\n".join(preamble)
    assert "Customer preamble" in preamble_joined
    assert "Keep me intact." in preamble_joined

    constraint_bodies = {
        applier._bullet_text(ln)
        for ln in _section_lines(text_after_first, "## CONSTRAINTS")
    }
    assert "Always JOIN on account_id." in constraint_bodies

    applier.apply_gso_quality_instructions(cfg)
    text_after_second = _read_general_text(cfg)
    assert text_after_second == text_after_first


def test_flag_off_strips_current_policy_bullets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Flip to off + re-apply removes every current policy bullet, preserves customer text."""
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config("Customer preamble")
    applier.apply_gso_quality_instructions(cfg)
    text_on = _read_general_text(cfg)
    for _key, header, body in applier._GSO_QUALITY_V1_POLICIES:
        assert body in {
            applier._bullet_text(ln) for ln in _section_lines(text_on, header)
        }

    monkeypatch.setenv("GSO_APPLY_QUALITY_INSTRUCTIONS", "off")
    changed = applier.apply_gso_quality_instructions(cfg)
    text_off = _read_general_text(cfg)

    assert changed is True
    for _key, header, body in applier._GSO_QUALITY_V1_POLICIES:
        bodies = {
            applier._bullet_text(ln) for ln in _section_lines(text_off, header)
        }
        assert body not in bodies
    assert "Customer preamble" in text_off


def test_flag_off_does_not_touch_customer_variant(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A customer bullet that paraphrases (differs in wording) must survive ``=off``."""
    customer_paraphrase = (
        "Prefer metric views over base tables for aggregations."
    )
    seed = (
        "## CONSTRAINTS\n"
        f"- {customer_paraphrase}\n"
    )
    monkeypatch.setenv("GSO_APPLY_QUALITY_INSTRUCTIONS", "off")
    cfg = _fresh_config(seed)
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)

    bodies = {
        applier._bullet_text(ln) for ln in _section_lines(text, "## CONSTRAINTS")
    }
    assert customer_paraphrase in bodies


def test_flag_off_is_noop_when_no_policy_bullets_present(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GSO_APPLY_QUALITY_INSTRUCTIONS", "off")
    cfg = _fresh_config("Customer preamble")
    changed = applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)

    assert changed is False
    assert "Customer preamble" in text
    for _key, header, body in applier._GSO_QUALITY_V1_POLICIES:
        bodies = {
            applier._bullet_text(ln) for ln in _section_lines(text, header)
        }
        assert body not in bodies


def test_flag_off_then_on_restores_bullets(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Revert/restore roundtrip — supports continuous rollout."""
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config()
    applier.apply_gso_quality_instructions(cfg)

    monkeypatch.setenv("GSO_APPLY_QUALITY_INSTRUCTIONS", "off")
    applier.apply_gso_quality_instructions(cfg)
    text_off = _read_general_text(cfg)
    for _key, header, body in applier._GSO_QUALITY_V1_POLICIES:
        bodies = {
            applier._bullet_text(ln) for ln in _section_lines(text_off, header)
        }
        assert body not in bodies

    monkeypatch.setenv("GSO_APPLY_QUALITY_INSTRUCTIONS", "on")
    applier.apply_gso_quality_instructions(cfg)
    text_on = _read_general_text(cfg)
    for _key, header, body in applier._GSO_QUALITY_V1_POLICIES:
        bodies = {
            applier._bullet_text(ln) for ln in _section_lines(text_on, header)
        }
        assert body in bodies


def test_policy_body_content_matches_plan_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression guard: all three plan anchor phrases appear in emitted text."""
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config()
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)
    assert "metric view `mv_*`" in text
    assert "by X, then Y" in text
    assert "NOW()" in text


# ─────────────────────────────────────────────────────────────────────────────
# Deprecation sweep: stale wording from a previous release is cleaned up on
# the next apply, scoped to the canonical headers we own.
# ─────────────────────────────────────────────────────────────────────────────
def test_deprecated_bullet_is_stripped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    old_text = "Old wording we used to ship about metric views."
    monkeypatch.setattr(
        applier,
        "_GSO_QUALITY_V1_DEPRECATED_BULLETS",
        frozenset({old_text}),
    )
    seed = (
        "## CONSTRAINTS\n"
        f"- {old_text}\n"
        "- Keep this customer bullet around.\n"
    )
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config(seed)
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)

    bodies = {
        applier._bullet_text(ln) for ln in _section_lines(text, "## CONSTRAINTS")
    }
    assert old_text not in bodies
    assert "Keep this customer bullet around." in bodies


# ─────────────────────────────────────────────────────────────────────────────
# Legacy sentinel migration: pre-Option-C spaces carry
# ``-- BEGIN/END GSO_QUALITY_V1:<key>`` blocks in their text_instructions.
# The applier must sweep them out on any apply so customers aren't left
# with stale wrappers.
# ─────────────────────────────────────────────────────────────────────────────
def test_legacy_sentinel_blocks_are_swept(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seeded = (
        "## PURPOSE\n"
        "- Answer sales questions.\n"
        "\n"
        "-- BEGIN GSO_QUALITY_V1:mv_preference\n"
        "legacy body text\n"
        "-- END GSO_QUALITY_V1:mv_preference\n"
        "\n"
        "-- BEGIN GSO_QUALITY_V1:column_ordering\n"
        "legacy body text\n"
        "-- END GSO_QUALITY_V1:column_ordering\n"
    )
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    cfg = _fresh_config(seeded)
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)

    assert "-- BEGIN GSO_QUALITY_V1" not in text
    assert "-- END GSO_QUALITY_V1" not in text
    assert "legacy body text" not in text
    purpose_bodies = {
        applier._bullet_text(ln) for ln in _section_lines(text, "## PURPOSE")
    }
    assert "Answer sales questions." in purpose_bodies


def test_legacy_sentinel_sweep_under_flag_off(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``=off`` must still sweep legacy sentinel wrappers."""
    seeded = (
        "-- BEGIN GSO_QUALITY_V1:mv_preference\n"
        "legacy body\n"
        "-- END GSO_QUALITY_V1:mv_preference\n"
    )
    monkeypatch.setenv("GSO_APPLY_QUALITY_INSTRUCTIONS", "off")
    cfg = _fresh_config(seeded)
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)
    assert "GSO_QUALITY_V1" not in text


# ─────────────────────────────────────────────────────────────────────────────
# Cross-scheme dedup (root-cause fix for canonical/legacy duplication).
#
# Background: ``_sanitize_plaintext_instructions`` (and a handful of other
# plain-text rewrite paths) actively flip canonical ``## X`` headers to
# legacy ``X:`` form AND strip inline-code backticks. Without a semantic
# strip pass, ``apply_gso_quality_instructions`` cannot recognise its own
# prior output once it has been mutated; the next run emits a fresh
# canonical section on top of the legacy one and each subsequent run
# cements the duplication. The tests below pin down the cross-scheme
# match contract.
# ─────────────────────────────────────────────────────────────────────────────


def _legacy_section_lines(text: str, legacy_key: str) -> list[str]:
    _canonical, legacy, _preamble = applier.parse_canonical_sections(text)
    return list(legacy.get(legacy_key, []))


def test_legacy_data_quality_notes_bullet_stripped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Legacy ``DATA QUALITY NOTES:`` carrying the GSO body without
    backticks (the post-``_sanitize_plaintext_instructions`` shape) must be
    stripped on apply, with the canonical-form bullet emitted exactly
    once at the top.
    """
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    legacy_body = (
        "Interpret 'this year', 'last quarter', and 'YTD' relative to the "
        "current system date from NOW(). Do not rely on static flags in "
        "DIM_DATE for current-period filtering."
    )
    seed = (
        "PURPOSE:\n"
        "- Sales reporting.\n"
        "\n"
        "DATA QUALITY NOTES:\n"
        f"- {legacy_body}\n"
    )
    cfg = _fresh_config(seed)
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)

    canonical_bodies = [
        applier._bullet_text(ln)
        for ln in _section_lines(text, "## DATA QUALITY NOTES")
    ]
    matches = [b for b in canonical_bodies if "from `NOW()`" in b]
    assert len(matches) == 1, (
        f"expected exactly one canonical calendar-grounding bullet, "
        f"got {canonical_bodies!r}"
    )
    assert "DATA QUALITY NOTES:" not in text, (
        "legacy DATA QUALITY NOTES: header should be removed when the "
        "section is empty after the strip pass"
    )
    assert "PURPOSE:" in text


def test_legacy_constraints_with_customer_bullet_preserves_customer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Mixed legacy ``CONSTRAINTS:`` (one GSO bullet, one customer
    bullet) — only the GSO bullet is removed; the customer bullet plus
    the section header are preserved.
    """
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    legacy_gso = (
        "For aggregate questions over dates or periods, prefer the metric "
        "view mv_* when one exists that covers the requested dimensions; "
        "otherwise use a regular TABLE."
    )
    customer_bullet = "Always use UTC dates."
    seed = (
        "CONSTRAINTS:\n"
        f"- {legacy_gso}\n"
        f"- {customer_bullet}\n"
    )
    cfg = _fresh_config(seed)
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)

    legacy_bodies = {
        applier._bullet_text(ln)
        for ln in _legacy_section_lines(text, "CONSTRAINTS")
    }
    assert customer_bullet in legacy_bodies
    assert legacy_gso not in legacy_bodies, (
        "GSO-owned legacy bullet should have been stripped"
    )

    canonical_bodies = [
        applier._bullet_text(ln)
        for ln in _section_lines(text, "## CONSTRAINTS")
    ]
    matches = [b for b in canonical_bodies if "metric view `mv_*`" in b]
    assert len(matches) == 1


def test_legacy_and_canonical_dual_copies_collapse_to_one(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact production duplication case: input has BOTH the canonical
    ``## CONSTRAINTS`` (with backticks) AND the mutated legacy
    ``CONSTRAINTS:`` (without backticks). Output must have exactly one
    copy under the canonical header and no legacy duplicate.
    """
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    seed = (
        "## CONSTRAINTS\n"
        "- For aggregate questions over dates or periods, prefer the "
        "metric view `mv_*` when one exists that covers the requested "
        "dimensions; otherwise use a regular `TABLE`.\n"
        "\n"
        "CONSTRAINTS:\n"
        "- For aggregate questions over dates or periods, prefer the "
        "metric view mv_* when one exists that covers the requested "
        "dimensions; otherwise use a regular TABLE.\n"
    )
    cfg = _fresh_config(seed)
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)

    canonical_bodies = [
        applier._bullet_text(ln)
        for ln in _section_lines(text, "## CONSTRAINTS")
    ]
    matches = [b for b in canonical_bodies if "metric view `mv_*`" in b]
    assert len(matches) == 1, f"expected one canonical bullet, got {matches!r}"
    assert "CONSTRAINTS:" not in text, (
        "legacy CONSTRAINTS: header must be removed once empty"
    )


def test_byte_mutated_canonical_still_dedupes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A canonical ``## CONSTRAINTS`` whose body had backticks stripped
    (partial mutation, header still canonical) is recognised by the
    semantic match and not duplicated on the next emit.
    """
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    seed = (
        "## CONSTRAINTS\n"
        "- For aggregate questions over dates or periods, prefer the "
        "metric view mv_* when one exists that covers the requested "
        "dimensions; otherwise use a regular TABLE.\n"
    )
    cfg = _fresh_config(seed)
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)

    canonical_bodies = [
        applier._bullet_text(ln)
        for ln in _section_lines(text, "## CONSTRAINTS")
    ]
    matches = [b for b in canonical_bodies if "metric view `mv_*`" in b]
    assert len(matches) == 1


def test_non_owned_legacy_section_untouched(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``ASSET ROUTING:`` is not in CANONICAL_SECTION_HEADERS, so it has
    no canonical equivalent and nothing under it should ever be touched
    by the strip pass — even if the bullet text happens to overlap with
    a GSO policy fragment.
    """
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    asset_routing_bullet = "When the user asks about APSD, route to mv_apsd."
    seed = (
        "ASSET ROUTING:\n"
        f"- {asset_routing_bullet}\n"
    )
    cfg = _fresh_config(seed)
    applier.apply_gso_quality_instructions(cfg)
    text = _read_general_text(cfg)

    legacy_bodies = {
        applier._bullet_text(ln)
        for ln in _legacy_section_lines(text, "ASSET ROUTING")
    }
    assert asset_routing_bullet in legacy_bodies


def test_idempotent_across_two_runs_with_legacy_seed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two consecutive applies on legacy-shaped seed text converge: the
    first run cleans up duplicates, the second run is a no-op.
    """
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    seed = (
        "PURPOSE:\n"
        "- Sales reporting.\n"
        "\n"
        "DATA QUALITY NOTES:\n"
        "- Interpret 'this year', 'last quarter', and 'YTD' relative to "
        "the current system date from NOW(). Do not rely on static flags "
        "in DIM_DATE for current-period filtering.\n"
        "\n"
        "CONSTRAINTS:\n"
        "- For aggregate questions over dates or periods, prefer the "
        "metric view mv_* when one exists that covers the requested "
        "dimensions; otherwise use a regular TABLE.\n"
    )
    cfg = _fresh_config(seed)
    applier.apply_gso_quality_instructions(cfg)
    text_first = _read_general_text(cfg)

    changed_second = applier.apply_gso_quality_instructions(cfg)
    text_second = _read_general_text(cfg)

    assert changed_second is False
    assert text_first == text_second
    # And no residual legacy duplicates of owned headers.
    assert "DATA QUALITY NOTES:" not in text_first
    assert "CONSTRAINTS:" not in text_first


def test_normalize_policy_body_strips_backticks_and_whitespace() -> None:
    """Direct contract test for the body-normalisation helper."""
    canonical = (
        "Interpret 'this year', 'last quarter', and 'YTD' relative to the "
        "current system date from `NOW()`. Do not rely on static flags in "
        "`DIM_DATE` for current-period filtering."
    )
    mutated = (
        "Interpret 'this year', 'last quarter', and 'YTD' relative to the\n"
        "current system date from NOW(). Do not rely  on  static flags in\n"
        "DIM_DATE for current-period filtering."
    )
    assert applier._normalize_policy_body(canonical) == applier._normalize_policy_body(mutated)


def test_canonical_to_legacy_header_map_covers_owned_sections() -> None:
    """Sanity check the equivalence map keys/values match the
    parse_canonical_sections legacy-bucket key shape (uppercase, no ##,
    no colon).
    """
    assert applier._CANONICAL_TO_LEGACY_HEADER["## CONSTRAINTS"] == "CONSTRAINTS"
    assert (
        applier._CANONICAL_TO_LEGACY_HEADER["## DATA QUALITY NOTES"]
        == "DATA QUALITY NOTES"
    )
    assert applier._CANONICAL_TO_LEGACY_HEADER["## PURPOSE"] == "PURPOSE"


def test_dedup_logs_telemetry_when_stripping(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A legacy strip emits a ``gso_quality.dedup`` log line so we can
    measure how often this fix fires in production.
    """
    monkeypatch.delenv("GSO_APPLY_QUALITY_INSTRUCTIONS", raising=False)
    seed = (
        "CONSTRAINTS:\n"
        "- For aggregate questions over dates or periods, prefer the "
        "metric view mv_* when one exists that covers the requested "
        "dimensions; otherwise use a regular TABLE.\n"
    )
    cfg = _fresh_config(seed)
    with caplog.at_level("INFO", logger="genie_space_optimizer.optimization.applier"):
        applier.apply_gso_quality_instructions(cfg)
    assert any("gso_quality.dedup" in rec.message for rec in caplog.records), (
        f"expected gso_quality.dedup log line, got {[r.message for r in caplog.records]!r}"
    )
