"""Phase 2 Action 2.1 — replay test for the canonical ccf1d60d gs_026
RepairKit. Loads a hand-curated fixture and asserts plan_repair
produces a byte-identical kit under each propagation outcome."""

from __future__ import annotations

import json
from pathlib import Path

import pytest


FIXTURE_PATH = (
    Path(__file__).parent
    / "fixtures"
    / "repair_planner"
    / "ccf1d60d_gs026_repair_kit.json"
)


def _load_fixture() -> dict:
    return json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))


def _card_from_fixture(raw: dict):
    from genie_space_optimizer.optimization.rca import RCACard, RcaKind

    return RCACard(
        card_id=raw["card_id"],
        cluster_id=raw["cluster_id"],
        qids=tuple(raw["qids"]),
        root_cause=RcaKind(raw["root_cause"]),
        grounding_terms=frozenset(raw["grounding_terms"]),
        intended_patch_shape=raw["intended_patch_shape"],
        allowed_patch_families=frozenset(raw["allowed_patch_families"]),
        forbidden_patch_families=frozenset(raw["forbidden_patch_families"]),
        rationale=raw["rationale"],
    )


def _normalize_kit(kit: dict) -> dict:
    """Render kit fields with the same JSON-serialisable shape as the
    fixture (tuples → lists, sorted grounding_terms)."""
    return {
        **kit,
        "target_qids": list(kit["target_qids"]),
        "grounding_terms": list(kit["grounding_terms"]),
        "required_companions": list(kit["required_companions"]),
    }


@pytest.mark.parametrize(
    ("propagation_root_cause", "expected_key"),
    [
        ("unknown", "expected_kit"),
        (
            "instruction_insufficient_force",
            "expected_kit_under_instruction_insufficient_force",
        ),
    ],
)
def test_replay_plan_repair_matches_expected_kit(
    propagation_root_cause: str, expected_key: str,
) -> None:
    from genie_space_optimizer.optimization.repair_planner import plan_repair

    fixture = _load_fixture()
    card = _card_from_fixture(fixture["card"])
    cluster = fixture["cluster"]

    kit = plan_repair(
        card=card,
        cluster=cluster,
        propagation_root_cause=propagation_root_cause,
    )
    assert kit is not None
    assert _normalize_kit(kit) == fixture[expected_key]
