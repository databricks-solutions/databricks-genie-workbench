"""PATCH_TYPE_SEMANTICS must classify every PatchType enum member.

Prevents the Task 1.1/1.2 regression where the typed classifier raised
KeyError on legitimate patch types that were missing from the map.
"""
from __future__ import annotations


def test_every_patch_type_enum_member_has_a_semantic_classification():
    from genie_space_optimizer.optimization.patch_semantic import (
        PATCH_TYPE_SEMANTICS,
    )
    from genie_space_optimizer.optimization.repair_intent import PatchType

    enum_values = {p.value for p in PatchType}
    declared = set(PATCH_TYPE_SEMANTICS.keys())
    missing = sorted(enum_values - declared)
    assert not missing, (
        f"PATCH_TYPE_SEMANTICS missing classifications for: {missing}. "
        f"New PatchType enum members must be added to the map."
    )
