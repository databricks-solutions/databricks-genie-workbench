"""Phase 2 Action 2.2 — kit-safety replay test.

Loads the ccf1d60d iter-1 8-patch slate fixture and asserts that the
kit-aware wrapper detects at least one kit-atomicity violation when the
legacy cap is forced to drop members of a multi-patch kit."""

from __future__ import annotations

import json
from pathlib import Path

from genie_space_optimizer.optimization.kit_safety import (
    KitSafetyPolicy,
    select_kit_aware_patch_cap,
)


FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "kit_safety"
    / "ccf1d60d_iter1_kit_atomicity.json"
)


def test_kit_aware_wrapper_detects_atomicity_violation_on_canonical_slate() -> None:
    fixture = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    patches = fixture["patches"]

    selected, decisions, kit_outcomes = select_kit_aware_patch_cap(
        patches,
        target_qids=("gs_026",),
        max_patches=3,  # Forces the fragmentation observed in iter-1
        cluster_target_qids=("gs_026",),
        policy=KitSafetyPolicy(passing_dependents_threshold=15),
    )

    atomicity_violations = [
        o for o in kit_outcomes if o["reason"] == "kit_atomicity_violation"
    ]
    assert (
        len(atomicity_violations)
        >= fixture["expected_kit_outcomes_with_max_patches_3"][
            "kit_atomicity_violation_count_min"
        ]
    ), (
        "Canonical ccf1d60d iter-1 fragmentation must surface as at least one "
        f"kit_atomicity_violation; got kit_outcomes={kit_outcomes}"
    )
