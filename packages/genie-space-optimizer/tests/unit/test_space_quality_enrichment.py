from __future__ import annotations

import copy
from unittest.mock import MagicMock

from genie_space_optimizer.optimization import space_quality_enrichment as sqe


def _raw_space(*, description: str = "", instructions: str = "") -> dict:
    text_instructions = []
    if instructions:
        text_instructions.append({"id": "a" * 32, "content": [instructions]})
    return {
        "description": description,
        "_parsed_space": {
            "version": 2,
            "data_sources": {
                "tables": [
                    {
                        "identifier": "main.sales.orders",
                        "description": ["Orders for sales analytics"],
                        "column_configs": [
                            {
                                "column_name": "region",
                                "data_type": "STRING",
                                "description": ["Sales region"],
                            }
                        ],
                    }
                ],
                "metric_views": [],
                "functions": [],
            },
            "instructions": {"text_instructions": text_instructions},
            "config": {},
        },
    }


def _stub_state(monkeypatch):
    stages: list[tuple[str, str, dict]] = []
    patches: list[dict] = []

    def fake_stage(_spark, _run_id, stage, status, **kwargs):
        stages.append((stage, status, kwargs))

    def fake_patch(
        _spark, _run_id, _iteration, _lever, _patch_index, patch_record, *_args,
    ):
        patches.append(patch_record)

    monkeypatch.setattr(sqe, "write_stage", fake_stage)
    monkeypatch.setattr(sqe, "write_patch", fake_patch)
    return stages, patches


def test_missing_description_uses_top_level_space_patch(monkeypatch) -> None:
    stages, patches = _stub_state(monkeypatch)
    raw = _raw_space(
        description="",
        instructions=(
            "## PURPOSE\n- Answer sales questions using main.sales.orders.\n\n"
            "## DISAMBIGUATION\n- Ask for a time range when missing.\n\n"
            "## CONSTRAINTS\n- Use configured data sources only.\n\n"
            "## Instructions you must follow when providing summaries\n"
            "- State relevant filters."
        ),
    )
    updated_descriptions: list[str] = []
    config_patches: list[dict] = []

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.optimizer._generate_space_description",
        lambda _parsed, _w: "Sales analytics space for orders, regions, and revenue reporting.",
    )
    monkeypatch.setattr(
        sqe,
        "update_space_description",
        lambda _w, _sid, desc: updated_descriptions.append(desc),
    )
    monkeypatch.setattr(
        sqe,
        "patch_space_config",
        lambda _w, _sid, target: config_patches.append(copy.deepcopy(target)),
    )
    monkeypatch.setattr(sqe, "fetch_space_config", lambda _w, _sid: raw)

    result = sqe.run_space_quality_enrichment(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        raw_config=raw,
        catalog="cat",
        schema="sch",
    )

    assert result.applied_count == 1
    assert updated_descriptions == [
        "Sales analytics space for orders, regions, and revenue reporting."
    ]
    assert config_patches == []
    assert [p["patch_type"] for p in patches] == ["update_space_description"]
    assert patches[0]["scope"] == "genie_space"
    assert stages[-1][0] == "SPACE_QUALITY_ENRICHMENT"
    assert stages[-1][1] == "COMPLETE"
    assert stages[-1][2]["detail"]["applied_count"] == 1


def test_thin_instructions_seed_full_serialized_space(monkeypatch) -> None:
    _stages, patches = _stub_state(monkeypatch)
    raw = _raw_space(description="Sales analytics space for regional order reporting.")
    patched_configs: list[dict] = []

    def fake_patch(_w, _sid, target):
        patched_configs.append(copy.deepcopy(target))

    monkeypatch.setattr(sqe, "patch_space_config", fake_patch)
    monkeypatch.setattr(
        sqe,
        "fetch_space_config",
        lambda _w, _sid: {**raw, "_parsed_space": patched_configs[-1]},
    )

    result = sqe.run_space_quality_enrichment(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        raw_config=raw,
        catalog="cat",
        schema="sch",
    )

    assert result.applied_count == 1
    assert len(patched_configs) == 1
    target = patched_configs[0]
    assert target["version"] == 2
    assert target["data_sources"]["tables"][0]["identifier"] == "main.sales.orders"
    instruction_text = target["instructions"]["text_instructions"][0]["content"][0]
    assert "## PURPOSE" in instruction_text
    assert "main.sales.orders" in instruction_text
    assert [p["patch_type"] for p in patches] == ["add_instruction"]
    assert patches[0]["scope"] == "genie_config"


def test_scan_input_scores_top_level_description_without_mutating_serialized_space() -> None:
    raw = _raw_space(description="Useful top-level description for this sales space.")
    parsed_before = copy.deepcopy(raw["_parsed_space"])

    scan_input = sqe.scan_input_for_iq(raw)

    assert scan_input["description"] == "Useful top-level description for this sales space."
    assert raw["_parsed_space"] == parsed_before
