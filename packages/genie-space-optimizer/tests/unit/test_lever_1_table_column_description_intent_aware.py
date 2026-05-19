"""Plan 8 Task 10 — confirm lever-1-table-column-description SKILL.md
documents the intent-aware path that absorbed lever-1-rca-bridge."""
from __future__ import annotations

from pathlib import Path


def test_skill_md_documents_intent_aware_inputs():
    skill_md = (
        Path(__file__).parent.parent.parent
        / "src/genie_space_optimizer/skills/"
          "lever-1-table-column-description/SKILL.md"
    )
    body = skill_md.read_text(encoding="utf-8")
    # The absorbed RCA-bridge responsibility is documented as an
    # intent-aware section that reads the typed RepairIntent's
    # intent_id / blame_set / repair_shape.
    assert "intent_id" in body
    assert "blame_set" in body
    # The deprecated skill folder is removed.
    rca_bridge = (
        skill_md.parent.parent
        / "lever-1-rca-bridge"
    )
    assert not rca_bridge.exists(), (
        "lever-1-rca-bridge/ folder must be deleted in Plan 8 Task 10"
    )
