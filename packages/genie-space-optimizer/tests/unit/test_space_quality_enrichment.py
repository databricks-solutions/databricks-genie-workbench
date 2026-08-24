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
    artifacts: list[tuple[str, dict]] = []
    monkeypatch.setattr(
        sqe,
        "write_artifact",
        lambda _spark, _run_id, kind, payload, **_kwargs: artifacts.append((kind, payload)),
    )
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
        "genie_space_optimizer.optimization.optimizer_utils._generate_space_description",
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
    assert artifacts == [(
        "space_quality_enrichment",
        {
            "description_present": True,
            "description": "Sales analytics space for orders, regions, and revenue reporting.",
        },
    )]


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


def test_prompt_matching_context_keeps_bounded_proposal_values() -> None:
    context = sqe.build_prompt_matching_context({
        "_uc_columns": [{
            "catalog_name": "main",
            "schema_name": "sales",
            "table_name": "orders",
            "column_name": "region",
            "data_type": "STRING",
            "comment": "Sales region",
        }],
        "_data_profile": {
            "main.sales.orders": {
                "row_count": 100,
                "columns": {
                    "region": {
                        "cardinality": 4,
                        "distinct_values": ["East", "West"],
                        "min": "East",
                        "max": "West",
                    },
                },
            },
        },
        "_rls_audit": {
            "main.sales.orders": {
                "verdict": "clean",
                "reason": "no policies",
            },
        },
        "_asset_semantics": {"main.sales.orders": {"kind": "table"}},
    })

    assert context["uc_columns"] == [{
        "catalog_name": "main",
        "schema_name": "sales",
        "table_name": "orders",
        "column_name": "region",
        "data_type": "STRING",
    }]
    assert context["data_profile"] == {
        "main.sales.orders": {
            "row_count": 100,
            "columns": {"region": {"cardinality": 4}},
        },
    }
    assert context["proposal_data_profile"] == {
        "main.sales.orders": {
            "row_count": 100,
            "columns": {
                "region": {
                    "cardinality": 4,
                    "distinct_values": ["East", "West"],
                    "min": "East",
                    "max": "West",
                },
            },
        },
    }
    assert context["rls_audit"] == {
        "main.sales.orders": {"verdict": "clean"},
    }


def test_prompt_matching_context_drops_inactive_profile_columns() -> None:
    context = sqe.build_prompt_matching_context({
        "_uc_columns": [{
            "catalog_name": "Main",
            "schema_name": "Sales",
            "table_name": "Orders",
            "column_name": "Region",
            "data_type": "STRING",
        }],
        "_data_profile": {
            "`main`.`sales`.`orders`": {
                "row_count": 100,
                "columns": {
                    "region": {"cardinality": 4},
                    "amount": {"cardinality": 87},
                },
            },
        },
    })

    assert context["data_profile"] == {
        "`main`.`sales`.`orders`": {
            "row_count": 100,
            "columns": {"region": {"cardinality": 4}},
        },
    }
    assert context["proposal_data_profile"] == {}


def test_prompt_matching_context_caps_proposal_values() -> None:
    context = sqe.build_prompt_matching_context({
        "_uc_columns": [{
            "catalog_name": "main",
            "schema_name": "sales",
            "table_name": "orders",
            "column_name": "status",
            "data_type": "STRING",
        }],
        "_data_profile": {
            "main.sales.orders": {
                "row_count": 20,
                "columns": {
                    "status": {
                        "cardinality": 20,
                        "distinct_values": ["x" * 200] + [
                            f"status_{index}" for index in range(20)
                        ],
                    },
                },
            },
        },
        "_rls_audit": {"main.sales.orders": {"verdict": "clean"}},
    })

    values = context["proposal_data_profile"]["main.sales.orders"]["columns"][
        "status"
    ]["distinct_values"]
    assert len(values) == 12
    assert len(values[0]) == 120


def test_prompt_matching_context_excludes_values_without_clean_rls() -> None:
    base = {
        "_uc_columns": [{
            "catalog_name": "main",
            "schema_name": "sales",
            "table_name": "orders",
            "column_name": "status",
            "data_type": "STRING",
        }],
        "_data_profile": {
            "main.sales.orders": {
                "row_count": 2,
                "columns": {
                    "status": {
                        "cardinality": 2,
                        "distinct_values": ["ACTIVE", "CLOSED"],
                    }
                },
            }
        },
    }

    unknown = sqe.build_prompt_matching_context(base)
    tainted = sqe.build_prompt_matching_context({
        **base,
        "_rls_audit": {"main.sales.orders": {"verdict": "tainted"}},
    })

    assert unknown["proposal_data_profile"] == {}
    assert tainted["proposal_data_profile"] == {}


def test_prompt_matching_context_excludes_sensitive_column_values() -> None:
    context = sqe.build_prompt_matching_context({
        "_uc_columns": [{
            "catalog_name": "main",
            "schema_name": "sales",
            "table_name": "customers",
            "column_name": "customer_email",
            "data_type": "STRING",
            "comment": "Customer email address",
        }],
        "_data_profile": {
            "main.sales.customers": {
                "row_count": 1,
                "columns": {
                    "customer_email": {
                        "cardinality": 1,
                        "distinct_values": ["person@example.com"],
                    }
                },
            }
        },
        "_rls_audit": {"main.sales.customers": {"verdict": "clean"}},
    })

    assert context["proposal_data_profile"] == {}


def test_prompt_matching_context_excludes_governed_sensitive_column_values() -> None:
    context = sqe.build_prompt_matching_context({
        "_uc_columns": [{
            "catalog_name": "main",
            "schema_name": "sales",
            "table_name": "customers",
            "column_name": "contact",
            "data_type": "STRING",
        }],
        "_uc_tags": [{
            "catalog_name": "main",
            "schema_name": "sales",
            "table_name": "customers",
            "column_name": "contact",
            "tag_name": "classification",
            "tag_value": "PII",
        }],
        "_data_profile": {
            "main.sales.customers": {
                "row_count": 1,
                "columns": {
                    "contact": {
                        "cardinality": 1,
                        "distinct_values": ["person@example.com"],
                    }
                },
            }
        },
        "_rls_audit": {"main.sales.customers": {"verdict": "clean"}},
    })

    assert context["proposal_data_profile"] == {}


def test_active_enrichment_applies_and_audits_prompt_matching(monkeypatch) -> None:
    stages, patches = _stub_state(monkeypatch)
    raw = _raw_space(
        description="Sales analytics space for regional order reporting.",
        instructions="Use the configured data sources for regional sales reporting and summaries.",
    )
    refreshed: dict = {}
    waits: list[int] = []

    def fake_prompt_matching(_w, _sid, config, *, benchmarks=None):
        assert benchmarks == [{"question": "Revenue by region"}]
        cc = config["_parsed_space"]["data_sources"]["tables"][0]["column_configs"][0]
        cc["enable_format_assistance"] = True
        cc["enable_entity_matching"] = True
        refreshed.update(copy.deepcopy(config["_parsed_space"]))
        return {
            "applied": [
                {
                    "type": "enable_example_values",
                    "table": "main.sales.orders",
                    "column": "region",
                },
                {
                    "type": "enable_value_dictionary",
                    "table": "main.sales.orders",
                    "column": "region",
                    "score": 6.0,
                    "reason": "ok",
                },
            ],
        }

    monkeypatch.setattr(sqe, "auto_apply_prompt_matching", fake_prompt_matching)
    monkeypatch.setattr(sqe.time, "sleep", lambda seconds: waits.append(seconds))
    monkeypatch.setattr(
        sqe,
        "fetch_space_config",
        lambda _w, _sid: {**raw, "_parsed_space": copy.deepcopy(refreshed)},
    )
    monkeypatch.setattr(sqe, "write_artifact", lambda *_args, **_kwargs: None)

    result = sqe.run_space_quality_enrichment(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        raw_config=raw,
        catalog="cat",
        schema="sch",
        prompt_matching_context={
            "version": 1,
            "uc_columns": [{
                "table_name": "orders",
                "column_name": "region",
                "data_type": "STRING",
            }],
            "data_profile": {},
            "rls_audit": {},
            "asset_semantics": {},
        },
        benchmarks=[{"question": "Revenue by region"}],
    )

    assert result.applied_count == 2
    assert [patch["patch_type"] for patch in patches] == [
        "enable_example_values",
        "enable_value_dictionary",
    ]
    assert patches[0]["rollback"]["enable_format_assistance"] is False
    assert patches[1]["rollback"]["enable_entity_matching"] is False
    assert patches[1]["provenance"]["iq_check_id"] == 8
    assert waits == [sqe.PROPAGATION_WAIT_ENTITY_MATCHING_SECONDS]
    entity_check = next(
        check
        for check in result.scan_after["checks"]
        if check["label"] == "Entity/format matching"
    )
    assert entity_check["passed"] is True
    assert stages[-1][2]["detail"]["applied_count"] == 2
