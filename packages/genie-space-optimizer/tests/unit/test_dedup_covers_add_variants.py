"""Trial 16 Chunk 3 — Stage 3 dedup must cover ``add_*`` patch variants,
not just their ``update_*`` siblings.

Why this test exists:
    Production postmortems 575892594490176 (gs_024) and
    319530250904653 (multiple qids) showed ``add_column_description``
    proposals being applied, deployed to the live Genie space, and
    then *rejected by the applier* as no-ops because the proposed
    description was already present on the target column. The
    rejection path bounced the qid between APPLYABLE and PROPOSED
    (RC3) for 9-32 iterations, burning the iteration budget.

    Stage 3 dedup (``feature_mining.apply_dedup_contract``) already
    handles this for ``update_column_description`` via the
    ``_description_already_conveys`` helper at line 1004 of
    ``feature_mining.py``. But ``add_column_description`` and
    ``add_description`` (table-level) have no parallel dedup branch,
    so the no-op proposals survive Stage 3 and trigger the RC3
    applier loop downstream.

    The fix is the minimum-impact-mirror-the-sibling: add an ``elif``
    branch for each ``add_*`` patch type that has a corresponding
    ``update_*`` already dedup-checked, reusing the same helper. No
    new validator, no new logic — every additional branch is ~3
    lines and ~1 helper call.

This test pins the contract for ``add_column_description``, the
specific patch type whose proliferation in the production postmortems
drove the 27+ applier rejections on gs_024 alone.
"""
from __future__ import annotations

import pytest

from genie_space_optimizer.optimization.feature_mining import (
    apply_dedup_contract,
)


# Trial 16 Chunk 3 — sibling-branch parity for add_column_description
# (line 1011 of feature_mining.py). Production postmortems 575892594490176
# + 319530250904653 showed add_column_description no-op proposals
# surviving Stage 3 because the dedup contract had no parallel branch
# for the add_* variant; they then triggered the RC3 recycle loop.
def test_add_column_description_drops_when_description_already_present() -> None:
    """An ``add_column_description`` candidate whose proposed text is
    already substantively present on the target column must be
    dropped with ``dedup_dropped_reason = 'description_already_present'``,
    matching the ``update_column_description`` branch's existing
    behaviour at line 1004 of feature_mining.py.
    """
    candidate = {
        "type": "add_column_description",
        "column": "amount",
        "proposed_description": "Order amount in USD, exclusive of tax.",
    }
    snap = {
        "tables": [
            {
                "name": "orders",
                "columns": [
                    {
                        "name": "amount",
                        # Existing description already substantively covers
                        # the proposed text — Jaccard well above 0.6.
                        "description": (
                            "Order amount in USD, exclusive of tax."
                        ),
                    }
                ],
            }
        ]
    }

    out = apply_dedup_contract([candidate], snap, leakage_oracle=None)

    assert out == [], (
        f"add_column_description candidate should have been dropped "
        f"by dedup; instead Stage 3 kept it: {out!r}. The applier "
        f"will then reject it as a no-op (target column already has "
        f"this description) and the RC3 recycle loop burns the qid's "
        f"iteration budget."
    )
    assert candidate.get("dedup_dropped_reason") == (
        "description_already_present"
    ), (
        f"Dropped candidate must carry the typed reason "
        f"'description_already_present' so the strategist's "
        f"forbidden_signatures channel sees the dedup cause; got "
        f"{candidate.get('dedup_dropped_reason')!r}."
    )


def test_add_column_description_passes_when_description_is_new() -> None:
    """Symmetric to the ``update_column_description`` passthrough test
    at line 352 — when the proposed description has no overlap with
    the existing one, the candidate must survive dedup unchanged.
    This pins the boundary so the new ``add_column_description``
    branch does not over-eagerly drop genuinely new descriptions.

    This case is NOT xfail because the current bug is *under*-dedup
    (the dedup branch is missing entirely), so the candidate already
    survives. The fix must preserve this behaviour.
    """
    candidate = {
        "type": "add_column_description",
        "column": "amount",
        "proposed_description": (
            "Always include the currency code suffix when rendering."
        ),
    }
    snap = {
        "tables": [
            {
                "name": "orders",
                "columns": [
                    {
                        "name": "amount",
                        "description": "Order amount.",
                    }
                ],
            }
        ]
    }

    out = apply_dedup_contract([candidate], snap, leakage_oracle=None)

    assert out == [candidate]
    assert "dedup_dropped_reason" not in candidate
