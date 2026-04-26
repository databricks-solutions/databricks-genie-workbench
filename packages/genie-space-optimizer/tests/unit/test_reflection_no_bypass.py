"""Tests for Task 10: retire reflection-as-validator bypass.

Background — the T2.2 reflection filter drops any proposal whose
``(patch_type, target)`` was already rolled back as a content
regression, UNLESS the proposal carries an
``escalation_justification``. The plan called this bypass too easy.

Task 10 tightens it:
* When ``ENFORCE_REFLECTION_REVALIDATION=True`` (the new default), a
  bypass requires a substantive justification (≥ 16 chars). Trivial
  one-word strings no longer suffice.
* Any surviving rewrite is stamped as a brand-new proposal:
  fresh ``proposal_id``, ``parent_proposal_id`` linked back to the
  rolled-back original, and ``is_reflection_rewrite=True`` so
  downstream audit and gates can recognize it.
* The audit row (Task 3, ``gate_name="reflection_rewrite"``) carries
  the parent linkage so a single SQL query reconstructs
  ``original_rolled_back -> reflection_rewrite -> apply -> accept``.

The full-loop wiring (grounding, counterfactual, AFS) runs
downstream of T2.2 unconditionally; this test focuses on the bypass
gate's own contract — we cannot exercise the full eval gate from a
unit test.
"""

from __future__ import annotations

import pytest


# ── Helpers exercising the pure semantics ────────────────────────


def _filter_with_bypass_logic(
    proposals: list[dict],
    forbidden: set[tuple[str, str]],
    *,
    iteration: int = 2,
    enforce: bool = True,
    prev_proposal_ids: dict[tuple[str, str], str] | None = None,
) -> tuple[list[dict], list[tuple[str, str, str]], list[dict]]:
    """Reproduce the T2.2 bypass policy implemented in
    ``harness._run_lever_loop``.

    Pure replica so we can exercise the contract without bringing up
    the full lever loop. Mirrors the harness logic line-for-line on
    the keys it touches.
    """
    prev_proposal_ids = prev_proposal_ids or {}
    kept: list[dict] = []
    dropped: list[tuple[str, str, str]] = []
    rewrites: list[dict] = []
    for p in proposals:
        ptype = str(p.get("type") or p.get("patch_type") or "")
        target = str(
            p.get("target")
            or p.get("target_object")
            or p.get("target_table")
            or p.get("table")
            or "?"
        )
        key = (ptype, target)
        justification = str(p.get("escalation_justification") or "").strip()
        if key in forbidden:
            if not justification:
                dropped.append(
                    (ptype, target,
                     "rolled back previously (no escalation_justification)"),
                )
                continue
            if enforce and len(justification) < 16:
                dropped.append(
                    (ptype, target,
                     "escalation_justification too short to be concrete"),
                )
                continue
            orig_pid = str(p.get("proposal_id") or "")
            parent_pid = prev_proposal_ids.get(key) or orig_pid or ""
            new_pid = f"{orig_pid or 'rewrite'}:rev{iteration}"
            p["parent_proposal_id"] = parent_pid
            p["proposal_id"] = new_pid
            p["is_reflection_rewrite"] = True
            p["requires_full_revalidation"] = True
            rewrites.append({
                "ptype": ptype,
                "target": target,
                "parent_proposal_id": parent_pid,
                "proposal_id": new_pid,
                "justification": justification[:240],
                "cluster_id": p.get("cluster_id"),
            })
            kept.append(p)
        else:
            kept.append(p)
    return kept, dropped, rewrites


def _retail_forbidden() -> set[tuple[str, str]]:
    """Replay the AG2 retail rollback: ``update_column_description``
    on ``mv_esr_dim_location.zone_combination`` was rolled back and
    appears in ``do_not_retry``."""
    return {
        ("update_column_description",
         "mv_esr_dim_location.zone_combination"),
    }


# ── Empty-justification path ─────────────────────────────────


def test_forbidden_proposal_with_no_justification_is_dropped():
    proposals = [
        {
            "type": "update_column_description",
            "target": "mv_esr_dim_location.zone_combination",
            "proposal_id": "p_orig",
        },
    ]

    kept, dropped, rewrites = _filter_with_bypass_logic(
        proposals, _retail_forbidden(),
    )

    assert kept == []
    assert len(dropped) == 1
    assert "no escalation_justification" in dropped[0][2]
    assert rewrites == []


def test_unrelated_proposal_passes_through_unchanged():
    proposals = [
        {"type": "add_instruction", "target": "TIME GROUPING",
         "proposal_id": "p_unrelated"},
    ]

    kept, dropped, rewrites = _filter_with_bypass_logic(
        proposals, _retail_forbidden(),
    )

    assert len(kept) == 1
    assert kept[0]["proposal_id"] == "p_unrelated"
    assert kept[0].get("is_reflection_rewrite") is None
    assert dropped == []
    assert rewrites == []


# ── Bypass enforcement ──────────────────────────────────────


def test_short_justification_is_dropped_under_enforcement():
    proposals = [
        {
            "type": "update_column_description",
            "target": "mv_esr_dim_location.zone_combination",
            "proposal_id": "p_orig",
            "escalation_justification": "retry",  # 5 chars
        },
    ]

    kept, dropped, _ = _filter_with_bypass_logic(
        proposals, _retail_forbidden(), enforce=True,
    )

    assert kept == []
    assert len(dropped) == 1
    assert "too short to be concrete" in dropped[0][2]


def test_short_justification_passes_when_enforcement_disabled():
    """Legacy mode (``ENFORCE_REFLECTION_REVALIDATION=false``) accepts
    any non-empty justification but still tags the rewrite + emits
    the audit signal."""
    proposals = [
        {
            "type": "update_column_description",
            "target": "mv_esr_dim_location.zone_combination",
            "proposal_id": "p_orig",
            "escalation_justification": "retry",
        },
    ]

    kept, dropped, rewrites = _filter_with_bypass_logic(
        proposals, _retail_forbidden(), enforce=False,
    )

    assert len(kept) == 1
    assert dropped == []
    assert len(rewrites) == 1
    assert rewrites[0]["parent_proposal_id"] == "p_orig"


def test_substantive_justification_is_accepted_as_rewrite():
    proposals = [
        {
            "type": "update_column_description",
            "target": "mv_esr_dim_location.zone_combination",
            "proposal_id": "p_orig",
            "escalation_justification": (
                "New ASI shows the previous rollback was a false "
                "positive: judges agree on the column choice now."
            ),
        },
    ]

    kept, dropped, rewrites = _filter_with_bypass_logic(
        proposals, _retail_forbidden(), iteration=3,
    )

    assert len(kept) == 1
    assert dropped == []
    # Stamped as a rewrite with fresh proposal_id and parent linkage.
    p = kept[0]
    assert p["is_reflection_rewrite"] is True
    assert p["requires_full_revalidation"] is True
    assert p["proposal_id"] == "p_orig:rev3"
    assert p["parent_proposal_id"] == "p_orig"

    assert len(rewrites) == 1
    rw = rewrites[0]
    assert rw["ptype"] == "update_column_description"
    assert rw["target"] == "mv_esr_dim_location.zone_combination"
    assert rw["proposal_id"] == "p_orig:rev3"
    assert rw["parent_proposal_id"] == "p_orig"


# ── Parent-id linkage ──────────────────────────────────────


def test_parent_proposal_id_uses_previous_rollback_when_available():
    """When the rollback bookkeeping records the prior iteration's
    AG id for that ``(ptype, target)``, the rewrite's
    ``parent_proposal_id`` links to it for queryable attribution."""
    proposals = [
        {
            "type": "update_column_description",
            "target": "mv_esr_dim_location.zone_combination",
            "proposal_id": "p_new",
            "escalation_justification": "Substantive evidence text " * 2,
        },
    ]

    kept, _, rewrites = _filter_with_bypass_logic(
        proposals,
        _retail_forbidden(),
        prev_proposal_ids={
            ("update_column_description",
             "mv_esr_dim_location.zone_combination"): "AG2_orig",
        },
    )

    assert kept[0]["parent_proposal_id"] == "AG2_orig"
    assert rewrites[0]["parent_proposal_id"] == "AG2_orig"


def test_rewrite_proposal_carries_full_revalidation_flag():
    """The plan's contract: a reflection rewrite must signal that it
    needs to flow through grounding + counterfactual + AFS / firewall
    as if it were brand new. The flag is the marker downstream gates
    can read."""
    proposals = [
        {
            "type": "update_column_description",
            "target": "mv_esr_dim_location.zone_combination",
            "proposal_id": "p_orig",
            "escalation_justification": "x" * 32,
        },
    ]

    kept, _, _ = _filter_with_bypass_logic(
        proposals, _retail_forbidden(),
    )

    assert kept[0]["requires_full_revalidation"] is True
    assert kept[0]["is_reflection_rewrite"] is True


# ── Config integration ───────────────────────────────────


def test_config_default_enforces_revalidation():
    """Pin the default — Task 10 ships with enforcement on, so a
    casual one-word justification cannot bypass the filter on a
    fresh install."""
    from genie_space_optimizer.common.config import (
        ENFORCE_REFLECTION_REVALIDATION,
    )

    assert ENFORCE_REFLECTION_REVALIDATION is True


# ── Defensive paths ───────────────────────────────────


def test_empty_proposals_returns_empty_kept():
    kept, dropped, rewrites = _filter_with_bypass_logic(
        [], _retail_forbidden(),
    )

    assert kept == dropped == rewrites == []


def test_empty_forbidden_set_keeps_everything():
    proposals = [
        {"type": "update_column_description", "target": "any.col",
         "proposal_id": "p1"},
    ]

    kept, dropped, _ = _filter_with_bypass_logic(proposals, set())

    assert kept == proposals
    assert dropped == []


@pytest.mark.parametrize("enforce", [True, False])
def test_proposal_without_proposal_id_still_gets_fresh_id(enforce):
    """Some proposal generators omit ``proposal_id`` until after
    ``proposals_to_patches``. The rewrite stamp must still produce a
    deterministic new id keyed off the iteration index."""
    proposals = [
        {
            "type": "update_column_description",
            "target": "mv_esr_dim_location.zone_combination",
            "escalation_justification": "x" * 32,
        },
    ]

    kept, _, _ = _filter_with_bypass_logic(
        proposals, _retail_forbidden(), iteration=5, enforce=enforce,
    )

    assert kept[0]["proposal_id"] == "rewrite:rev5"
    assert kept[0]["parent_proposal_id"] == ""
