"""Unit tests for genie_space_optimizer.optimization.optimizer — pure functions."""

from __future__ import annotations

import copy
import re

import pytest

from genie_space_optimizer.optimization.optimizer import (
    _EXAMPLE_SQL_SIMILARITY_THRESHOLD,
    _JOIN_FQN_RE,
    _MIN_DESCRIPTION_LENGTH,
    _SQL_FROM_TABLE_RE,
    _collect_blank_columns,
    _collect_insufficient_tables,
    _convert_fk_to_candidates,
    _deduplicate_proposals,
    _detect_instruction_content_in_description,
    _extract_equijoin_predicates,
    _extract_proven_joins,
    _filter_no_op_proposals,
    _format_enrichment_context,
    _format_table_enrichment_context,
    _is_description_insufficient,
    _is_fuzzy_match,
    _map_to_lever,
    _mine_benchmark_example_sqls,
    _ngram_similarity,
    _resolve_lever5_llm_result,
    _sanitize_join_sql,
    _types_compatible,
    _validate_lever5_proposals,
    cluster_failures,
    detect_conflicts_and_batch,
    detect_regressions,
    discover_join_candidates,
    enrich_metadata_with_uc_types,
    generate_proposals_from_strategy,
    validate_join_spec_types,
    validate_patch_set,
)


class TestMapToLever:
    def test_wrong_column_maps_to_lever_1(self):
        assert _map_to_lever("wrong_column") == 1

    def test_wrong_table_maps_to_lever_1(self):
        assert _map_to_lever("wrong_table") == 1

    def test_description_mismatch_maps_to_lever_1(self):
        assert _map_to_lever("description_mismatch") == 1

    def test_wrong_aggregation_maps_to_lever_2(self):
        assert _map_to_lever("wrong_aggregation") == 2

    def test_missing_filter_maps_to_lever_2(self):
        assert _map_to_lever("missing_filter") == 2

    def test_tvf_parameter_error_maps_to_lever_3(self):
        assert _map_to_lever("tvf_parameter_error") == 3

    def test_wrong_join_maps_to_lever_4(self):
        assert _map_to_lever("wrong_join") == 4

    def test_missing_synonym_maps_to_lever_1(self):
        assert _map_to_lever("missing_synonym") == 1

    def test_asset_routing_error_maps_to_lever_5(self):
        assert _map_to_lever("asset_routing_error") == 5

    def test_unknown_defaults_to_lever_5(self):
        assert _map_to_lever("unknown_cause") == 5

    def test_asi_failure_type_takes_precedence(self):
        assert _map_to_lever("wrong_column", asi_failure_type="wrong_join") == 4

    def test_repeatability_tvf_blame_goes_to_lever_5(self):
        assert _map_to_lever("repeatability_issue", blame_set="TVF_calc") == 5

    def test_repeatability_non_tvf_goes_to_lever_1(self):
        assert _map_to_lever("repeatability_issue", blame_set="TABLE_orders") == 1


class TestClusterFailures:
    def test_empty_results_returns_empty(self):
        assert cluster_failures({}, {}) == []

    def test_clusters_from_feedback_rows(self, sample_eval_results):
        clusters = cluster_failures(sample_eval_results, {})
        assert isinstance(clusters, list)
        for c in clusters:
            assert "cluster_id" in c
            assert "root_cause" in c
            assert "question_ids" in c

    def test_singletons_included(self):
        """Single-question clusters are kept so even lone failures are actionable."""
        eval_results = {
            "eval_results": [
                {
                    "inputs/question_id": "q1",
                    "feedback/syntax": "no",
                    "rationale/syntax": "unique error that won't cluster",
                },
            ]
        }
        clusters = cluster_failures(eval_results, {})
        assert len(clusters) >= 1
        assert any("q1" in c["question_ids"] for c in clusters)

    def test_clusters_sorted_by_size(self, sample_eval_results):
        clusters = cluster_failures(sample_eval_results, {})
        if len(clusters) > 1:
            for i in range(len(clusters) - 1):
                assert len(clusters[i]["question_ids"]) >= len(clusters[i + 1]["question_ids"])

    def test_asi_failure_type_extracted_from_rationale(self):
        """When rows lack explicit metadata, cluster_failures should parse
        the embedded JSON from format_asi_markdown rationale strings."""
        import json

        asi_payload = json.dumps(
            {
                "judge": "schema_accuracy",
                "verdict": "Fail",
                "raw_value": "no",
                "failure_type": "wrong_table",
                "severity": "major",
                "wrong_clause": "FROM invoices",
                "blame_set": ["invoices"],
                "confidence": 0.95,
                "rationale": "Used invoices instead of orders",
            },
            indent=2,
            sort_keys=True,
        )
        rationale_with_json = (
            "### schema_accuracy\n"
            "**Verdict:** Fail\n\n"
            "Used invoices instead of orders\n\n"
            f"```json\n{asi_payload}\n```"
        )

        eval_results = {
            "rows": [
                {
                    "inputs/question_id": "q1",
                    "feedback/schema_accuracy/value": "no",
                    "feedback/schema_accuracy/rationale": rationale_with_json,
                },
                {
                    "inputs/question_id": "q2",
                    "feedback/schema_accuracy/value": "no",
                    "feedback/schema_accuracy/rationale": rationale_with_json,
                },
            ]
        }
        clusters = cluster_failures(eval_results, {})
        schema_clusters = [
            c for c in clusters if c["affected_judge"] == "schema_accuracy"
        ]
        assert len(schema_clusters) >= 1
        c = schema_clusters[0]
        assert c["asi_failure_type"] == "wrong_table"
        assert c["asi_blame_set"] is not None

    def test_asi_failure_type_from_sql_escaped_rationale(self):
        """After SQL round-trip, newlines become literal \\n sequences.
        cluster_failures should still extract ASI metadata."""
        import json

        asi_payload = json.dumps(
            {
                "failure_type": "wrong_column",
                "severity": "major",
                "wrong_clause": "SELECT invoice_id",
                "blame_set": ["invoice_id"],
                "confidence": 0.8,
                "rationale": "Used wrong column",
            },
            indent=2,
            sort_keys=True,
        )
        rationale_with_json = (
            "### schema_accuracy\n"
            "**Verdict:** Fail\n\n"
            "Used wrong column\n\n"
            f"```json\n{asi_payload}\n```"
        )
        escaped_rationale = rationale_with_json.replace("\n", "\\n")

        eval_results = {
            "rows": [
                {
                    "inputs/question_id": "q1",
                    "feedback/schema_accuracy/value": "no",
                    "feedback/schema_accuracy/rationale": escaped_rationale,
                },
            ]
        }
        clusters = cluster_failures(eval_results, {})
        schema_clusters = [
            c for c in clusters if c["affected_judge"] == "schema_accuracy"
        ]
        assert len(schema_clusters) >= 1
        assert schema_clusters[0]["asi_failure_type"] == "wrong_column"

    def test_asi_from_flattened_metadata_keys(self):
        """cluster_failures should find ASI data in metadata/{judge}/{field} keys."""
        eval_results = {
            "rows": [
                {
                    "inputs/question_id": "q1",
                    "feedback/schema_accuracy/value": "no",
                    "feedback/schema_accuracy/rationale": "mismatch",
                    "metadata/schema_accuracy/failure_type": "wrong_join",
                    "metadata/schema_accuracy/blame_set": ["orders"],
                },
            ]
        }
        clusters = cluster_failures(eval_results, {})
        schema_clusters = [
            c for c in clusters if c["affected_judge"] == "schema_accuracy"
        ]
        assert len(schema_clusters) >= 1
        assert schema_clusters[0]["asi_failure_type"] == "wrong_join"


class TestDetectRegressions:
    def test_no_regression_when_scores_improve(self):
        prev = {"judge_a": 80.0, "judge_b": 70.0}
        curr = {"judge_a": 85.0, "judge_b": 75.0}
        assert detect_regressions(curr, prev) == []

    def test_regression_detected_on_drop(self):
        prev = {"judge_a": 80.0}
        curr = {"judge_a": 75.0}
        regressions = detect_regressions(curr, prev, threshold=2.0)
        assert len(regressions) == 1
        assert regressions[0]["judge"] == "judge_a"
        assert regressions[0]["drop"] == pytest.approx(5.0)

    def test_no_regression_within_threshold(self):
        prev = {"judge_a": 80.0}
        curr = {"judge_a": 79.0}
        assert detect_regressions(curr, prev, threshold=2.0) == []

    def test_multiple_regressions(self):
        prev = {"a": 90.0, "b": 85.0, "c": 70.0}
        curr = {"a": 80.0, "b": 75.0, "c": 70.0}
        regressions = detect_regressions(curr, prev, threshold=2.0)
        assert len(regressions) == 2


class TestDetectConflictsAndBatch:
    def test_no_conflicts_single_batch(self):
        proposals = [
            {"patch_type": "add_description"},
            {"patch_type": "add_column_description"},
        ]
        batches = detect_conflicts_and_batch(proposals)
        assert len(batches) == 1
        assert len(batches[0]) == 2

    def test_empty_proposals(self):
        assert detect_conflicts_and_batch([]) == []

    def test_single_proposal(self):
        batches = detect_conflicts_and_batch([{"patch_type": "add_instruction"}])
        assert len(batches) == 1
        assert len(batches[0]) == 1


class TestValidatePatchSet:
    def test_empty_patch_set_is_valid(self):
        valid, errors = validate_patch_set([], None)
        assert valid is True
        assert errors == []

    def test_unknown_type_flagged(self):
        patches = [{"type": "totally_made_up_type"}]
        valid, errors = validate_patch_set(patches)
        assert valid is False
        assert any("unknown type" in e for e in errors)

    def test_valid_patch_types_pass(self):
        patches = [{"type": "add_instruction", "target": "test"}]
        valid, errors = validate_patch_set(patches)
        assert valid is True

    def test_metadata_validation_missing_table(self, sample_metadata):
        patches = [{"type": "add_description", "table": "nonexistent_table"}]
        valid, errors = validate_patch_set(patches, sample_metadata)
        assert valid is False
        assert any("not found" in e for e in errors)

    def test_metadata_validation_missing_column(self, sample_metadata):
        patches = [{"type": "add_column_description", "table": "orders", "column": "no_such_col"}]
        valid, errors = validate_patch_set(patches, sample_metadata)
        assert valid is False


class TestDiscoverJoinCandidates:
    """Tests for the hint-based discover_join_candidates() function."""

    def _star_schema(self):
        return {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.fact_sales",
                        "column_configs": [
                            {"column_name": "date_key", "data_type": "INT"},
                            {"column_name": "product_key", "data_type": "INT"},
                            {"column_name": "store_key", "data_type": "INT"},
                            {"column_name": "amount", "data_type": "DOUBLE"},
                        ],
                    },
                    {
                        "identifier": "cat.sch.dim_date",
                        "column_configs": [
                            {"column_name": "date_key", "data_type": "INT"},
                            {"column_name": "year", "data_type": "INT"},
                        ],
                    },
                    {
                        "identifier": "cat.sch.dim_product",
                        "column_configs": [
                            {"column_name": "product_key", "data_type": "INT"},
                            {"column_name": "product_name", "data_type": "STRING"},
                        ],
                    },
                    {
                        "identifier": "cat.sch.dim_store",
                        "column_configs": [
                            {"column_name": "store_key", "data_type": "INT"},
                            {"column_name": "store_name", "data_type": "STRING"},
                        ],
                    },
                ],
                "join_specs": [],
            }
        }

    def test_discovers_all_shared_key_pairs(self):
        snap = self._star_schema()
        hints = discover_join_candidates(snap)
        table_pairs = set()
        for h in hints:
            table_pairs.add(tuple(sorted((h["left_table"], h["right_table"]))))
        assert tuple(sorted(("cat.sch.fact_sales", "cat.sch.dim_date"))) in table_pairs
        assert tuple(sorted(("cat.sch.fact_sales", "cat.sch.dim_product"))) in table_pairs
        assert tuple(sorted(("cat.sch.fact_sales", "cat.sch.dim_store"))) in table_pairs

    def test_hint_format(self):
        snap = self._star_schema()
        hints = discover_join_candidates(snap)
        for h in hints:
            assert "left_table" in h
            assert "right_table" in h
            assert "candidate_columns" in h
            assert "type_compatible" in h
            assert isinstance(h["candidate_columns"], list)
            for cc in h["candidate_columns"]:
                assert "left_col" in cc
                assert "right_col" in cc
                assert "reason" in cc

    def test_existing_specs_are_excluded(self):
        snap = self._star_schema()
        snap["data_sources"]["join_specs"] = [
            {
                "left": {"identifier": "cat.sch.fact_sales", "alias": "fact_sales"},
                "right": {"identifier": "cat.sch.dim_date", "alias": "dim_date"},
                "sql": ["`fact_sales`.`date_key` = `dim_date`.`date_key`"],
            },
        ]
        hints = discover_join_candidates(snap)
        table_pairs = set()
        for h in hints:
            table_pairs.add(tuple(sorted((h["left_table"], h["right_table"]))))
        assert tuple(sorted(("cat.sch.fact_sales", "cat.sch.dim_date"))) not in table_pairs
        assert tuple(sorted(("cat.sch.fact_sales", "cat.sch.dim_product"))) in table_pairs

    def test_no_candidates_when_no_shared_keys(self):
        snap = {
            "data_sources": {
                "tables": [
                    {"identifier": "a.b.t1", "column_configs": [{"column_name": "col_a"}]},
                    {"identifier": "a.b.t2", "column_configs": [{"column_name": "col_b"}]},
                ],
                "join_specs": [],
            }
        }
        assert discover_join_candidates(snap) == []

    def test_empty_metadata_returns_empty(self):
        assert discover_join_candidates({}) == []

    def test_candidate_columns_reported(self):
        snap = self._star_schema()
        hints = discover_join_candidates(snap)
        for h in hints:
            assert len(h["candidate_columns"]) >= 1

    def test_type_compatible_flag(self):
        snap = self._star_schema()
        hints = discover_join_candidates(snap)
        for h in hints:
            assert h["type_compatible"] is True

    def test_type_incompatible_flagged(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "a.b.t1",
                        "column_configs": [
                            {"column_name": "order_id", "data_type": "INT"},
                        ],
                    },
                    {
                        "identifier": "a.b.t2",
                        "column_configs": [
                            {"column_name": "order_id", "data_type": "STRING"},
                        ],
                    },
                ],
                "join_specs": [],
            }
        }
        hints = discover_join_candidates(snap)
        assert len(hints) == 1
        assert hints[0]["type_compatible"] is False

    def test_fuzzy_match_detected(self):
        """prod_key in one table matches product_key in another via stem."""
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "a.b.orders",
                        "column_configs": [
                            {"column_name": "prod_key", "data_type": "INT"},
                        ],
                    },
                    {
                        "identifier": "a.b.products",
                        "column_configs": [
                            {"column_name": "product_key", "data_type": "INT"},
                        ],
                    },
                ],
                "join_specs": [],
            }
        }
        hints = discover_join_candidates(snap)
        assert len(hints) == 1
        cols = hints[0]["candidate_columns"]
        assert any("fuzzy" in c["reason"] for c in cols)


class TestFuzzyMatch:
    def test_exact_match(self):
        assert _is_fuzzy_match("customer_id", "customer_id") is True

    def test_substring_match(self):
        assert _is_fuzzy_match("prod_key", "product_key") is True

    def test_stem_substring_match(self):
        assert _is_fuzzy_match("cust_id", "customer_id") is True
        assert _is_fuzzy_match("order_id", "order_key") is True
        assert _is_fuzzy_match("prod_key", "product_key") is True

    def test_no_match(self):
        assert _is_fuzzy_match("amount", "quantity") is False

    def test_same_stem_different_suffix(self):
        assert _is_fuzzy_match("product_key", "product_id") is True


class TestTypesCompatible:
    def test_same_type(self):
        assert _types_compatible("INT", "INT") is True

    def test_int_bigint(self):
        assert _types_compatible("INT", "BIGINT") is True

    def test_string_varchar(self):
        assert _types_compatible("STRING", "VARCHAR") is True

    def test_int_string_incompatible(self):
        assert _types_compatible("INT", "STRING") is False

    def test_empty_type_is_compatible(self):
        assert _types_compatible("INT", "") is True
        assert _types_compatible("", "STRING") is True

    def test_decimal_with_params(self):
        assert _types_compatible("DECIMAL(10,2)", "DOUBLE") is True

    def test_timestamp_variants(self):
        assert _types_compatible("TIMESTAMP", "TIMESTAMP_NTZ") is True


class TestEnrichMetadataWithUcTypes:
    def test_enriches_column_types(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.orders",
                        "column_configs": [
                            {"column_name": "order_id"},
                            {"column_name": "amount"},
                        ],
                    },
                ],
            }
        }
        uc_cols = [
            {"table_name": "orders", "column_name": "order_id", "data_type": "INT", "comment": "Primary key"},
            {"table_name": "orders", "column_name": "amount", "data_type": "DOUBLE", "comment": "Order total"},
        ]
        enrich_metadata_with_uc_types(snap, uc_cols)
        cc = snap["data_sources"]["tables"][0]["column_configs"]
        assert cc[0]["data_type"] == "INT"
        assert cc[0]["uc_comment"] == "Primary key"
        assert cc[1]["data_type"] == "DOUBLE"
        assert cc[1]["uc_comment"] == "Order total"

    def test_does_not_overwrite_existing_type(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.orders",
                        "column_configs": [
                            {"column_name": "order_id", "data_type": "BIGINT"},
                        ],
                    },
                ],
            }
        }
        uc_cols = [
            {"table_name": "orders", "column_name": "order_id", "data_type": "INT", "comment": "PK"},
        ]
        enrich_metadata_with_uc_types(snap, uc_cols)
        assert snap["data_sources"]["tables"][0]["column_configs"][0]["data_type"] == "BIGINT"

    def test_empty_uc_columns_no_op(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t1",
                        "column_configs": [{"column_name": "col_a"}],
                    },
                ],
            }
        }
        original = copy.deepcopy(snap)
        enrich_metadata_with_uc_types(snap, [])
        assert snap == original

    def test_unmatched_uc_rows_ignored(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.orders",
                        "column_configs": [{"column_name": "amount"}],
                    },
                ],
            }
        }
        uc_cols = [
            {"table_name": "customers", "column_name": "name", "data_type": "STRING", "comment": ""},
        ]
        enrich_metadata_with_uc_types(snap, uc_cols)
        assert "data_type" not in snap["data_sources"]["tables"][0]["column_configs"][0]


class TestValidateJoinSpecTypes:
    def _make_snap_with_types(self):
        return {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.fact_sales",
                        "column_configs": [
                            {"column_name": "product_key", "data_type": "INT"},
                            {"column_name": "amount", "data_type": "DOUBLE"},
                        ],
                    },
                    {
                        "identifier": "cat.sch.dim_product",
                        "column_configs": [
                            {"column_name": "product_key", "data_type": "INT"},
                            {"column_name": "product_name", "data_type": "STRING"},
                        ],
                    },
                ],
            }
        }

    def test_compatible_types_pass(self):
        snap = self._make_snap_with_types()
        spec = {
            "left": {"identifier": "cat.sch.fact_sales", "alias": "fact_sales"},
            "right": {"identifier": "cat.sch.dim_product", "alias": "dim_product"},
            "sql": ["`fact_sales`.`product_key` = `dim_product`.`product_key`"],
        }
        valid, reason = validate_join_spec_types(spec, snap)
        assert valid is True

    def test_incompatible_types_rejected(self):
        snap = self._make_snap_with_types()
        spec = {
            "left": {"identifier": "cat.sch.fact_sales", "alias": "fact_sales"},
            "right": {"identifier": "cat.sch.dim_product", "alias": "dim_product"},
            "sql": ["`fact_sales`.`amount` = `dim_product`.`product_name`"],
        }
        valid, reason = validate_join_spec_types(spec, snap)
        assert valid is False
        assert "incompatible" in reason.lower()

    def test_missing_types_pass(self):
        snap = {
            "data_sources": {
                "tables": [
                    {
                        "identifier": "cat.sch.t1",
                        "column_configs": [{"column_name": "id"}],
                    },
                    {
                        "identifier": "cat.sch.t2",
                        "column_configs": [{"column_name": "id"}],
                    },
                ],
            }
        }
        spec = {
            "left": {"identifier": "cat.sch.t1", "alias": "t1"},
            "right": {"identifier": "cat.sch.t2", "alias": "t2"},
            "sql": ["`t1`.`id` = `t2`.`id`"],
        }
        valid, reason = validate_join_spec_types(spec, snap)
        assert valid is True

    def test_empty_sql_passes(self):
        valid, reason = validate_join_spec_types({"sql": []}, {})
        assert valid is True

    def test_unparseable_condition_passes(self):
        spec = {
            "left": {"identifier": "a.b.c", "alias": "c"},
            "right": {"identifier": "a.b.d", "alias": "d"},
            "sql": ["c.id = d.id"],
        }
        valid, reason = validate_join_spec_types(spec, {})
        assert valid is True


class TestResolveLever5LlmResult:
    def test_example_sql_returns_add_example_sql(self):
        result = {"instruction_type": "example_sql", "example_question": "Q", "example_sql": "SELECT 1"}
        ptype, extra = _resolve_lever5_llm_result(result, "add_example_sql")
        assert ptype == "add_example_sql"
        assert extra["example_question"] == "Q"

    def test_text_instruction_from_routing_sets_downgraded_flag(self):
        result = {"instruction_type": "text_instruction", "instruction_text": "Do X"}
        ptype, extra = _resolve_lever5_llm_result(result, "add_example_sql")
        assert ptype == "add_instruction"
        assert extra["downgraded_from_example_sql"] is True

    def test_text_instruction_normal_no_downgrade(self):
        result = {"instruction_type": "text_instruction", "instruction_text": "Do X"}
        ptype, extra = _resolve_lever5_llm_result(result, "add_instruction")
        assert ptype == "add_instruction"
        assert extra.get("downgraded_from_example_sql") is False


class TestValidateLever5Proposals:
    _SNAP = {
        "tables": [{"name": "cat.sch.dim_property"}],
        "functions": [{"name": "cat.sch.get_revenue_summary"}],
        "metric_views": [{"name": "cat.sch.booking_analytics_metrics"}],
    }

    def test_rejects_empty_instruction(self):
        proposals = [{"patch_type": "add_instruction", "proposed_value": ""}]
        result = _validate_lever5_proposals(proposals, self._SNAP)
        assert len(result) == 0

    def test_rejects_generic_instruction(self):
        proposals = [
            {"patch_type": "add_instruction", "proposed_value": "Revenue refers to gross revenue."}
        ]
        result = _validate_lever5_proposals(proposals, self._SNAP)
        assert len(result) == 0

    def test_keeps_instruction_with_asset_reference(self):
        proposals = [
            {"patch_type": "add_instruction", "proposed_value": "Use dim_property for property lookups."}
        ]
        result = _validate_lever5_proposals(proposals, self._SNAP)
        assert len(result) == 1

    def test_rejects_overlength_instruction(self):
        proposals = [
            {"patch_type": "add_instruction", "proposed_value": "dim_property " + "x" * 3000}
        ]
        result = _validate_lever5_proposals(proposals, self._SNAP)
        assert len(result) == 0

    def test_rejects_empty_example_sql(self):
        proposals = [
            {"patch_type": "add_example_sql", "example_question": "", "example_sql": ""}
        ]
        result = _validate_lever5_proposals(proposals, self._SNAP)
        assert len(result) == 0

    def test_keeps_valid_example_sql(self):
        proposals = [
            {"patch_type": "add_example_sql", "example_question": "Q?", "example_sql": "SELECT 1"}
        ]
        result = _validate_lever5_proposals(proposals, self._SNAP)
        assert len(result) == 1

    def test_passes_through_non_lever5_proposals(self):
        proposals = [{"patch_type": "update_column_description", "proposed_value": "whatever"}]
        result = _validate_lever5_proposals(proposals, self._SNAP)
        assert len(result) == 1


class TestDetectInstructionContentInDescription:
    def test_clean_description_returns_empty(self):
        snap = {"config": {"description": "Revenue analytics for Wanderbricks."}}
        assert _detect_instruction_content_in_description(snap) == []

    def test_instruction_content_detected(self):
        snap = {
            "config": {
                "description": (
                    "Revenue analytics for Wanderbricks.\n\n"
                    "## ROUTING RULES\n"
                    "MUST follow these routing rules for TVF ROUTING."
                )
            }
        }
        proposals = _detect_instruction_content_in_description(snap)
        assert len(proposals) == 1
        assert proposals[0]["patch_type"] == "update_description"
        assert "Revenue analytics" in proposals[0]["proposed_value"]
        assert "ROUTING RULES" not in proposals[0]["proposed_value"]

    def test_empty_description_returns_empty(self):
        snap = {"config": {"description": ""}}
        assert _detect_instruction_content_in_description(snap) == []

    def test_missing_config_returns_empty(self):
        snap = {}
        assert _detect_instruction_content_in_description(snap) == []


# ---------------------------------------------------------------------------
# Proactive Description Enrichment
# ---------------------------------------------------------------------------


class TestIsDescriptionInsufficient:
    def test_none_is_insufficient(self):
        assert _is_description_insufficient(None) is True

    def test_empty_string_is_insufficient(self):
        assert _is_description_insufficient("") is True

    def test_empty_list_is_insufficient(self):
        assert _is_description_insufficient([]) is True

    def test_list_with_empty_string_is_insufficient(self):
        assert _is_description_insufficient([""]) is True

    def test_list_with_whitespace_is_insufficient(self):
        assert _is_description_insufficient(["  ", ""]) is True

    def test_long_string_is_sufficient(self):
        assert _is_description_insufficient("Primary key for orders") is False

    def test_long_list_is_sufficient(self):
        assert _is_description_insufficient(["Primary key"]) is False

    def test_whitespace_only_string_is_insufficient(self):
        assert _is_description_insufficient("   ") is True

    def test_short_string_under_threshold_is_insufficient(self):
        assert _is_description_insufficient("abc") is True

    def test_string_at_threshold_is_sufficient(self):
        assert _is_description_insufficient("a" * _MIN_DESCRIPTION_LENGTH) is False

    def test_string_just_under_threshold_is_insufficient(self):
        assert _is_description_insufficient("a" * (_MIN_DESCRIPTION_LENGTH - 1)) is True

    def test_short_list_elements_combined_sufficient(self):
        assert _is_description_insufficient(["hello", "world"]) is False

    def test_short_list_elements_combined_insufficient(self):
        assert _is_description_insufficient(["ab", "cd"]) is True


class TestCollectBlankColumns:
    def _make_snapshot(self, tables):
        return {"data_sources": {"tables": tables}}

    def test_no_blanks_when_all_described(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.orders",
                "description": ["Order data"],
                "column_configs": [
                    {"column_name": "order_id", "description": ["Primary key"], "data_type": "BIGINT"},
                    {"column_name": "amount", "description": ["Order amount"], "data_type": "DOUBLE"},
                ],
            }
        ])
        assert _collect_blank_columns(snap) == []

    def test_blank_description_no_uc_comment(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.orders",
                "description": ["Order data"],
                "column_configs": [
                    {"column_name": "order_id", "description": None, "data_type": "BIGINT"},
                    {"column_name": "amount", "description": ["Order amount"], "data_type": "DOUBLE"},
                ],
            }
        ])
        result = _collect_blank_columns(snap)
        assert len(result) == 1
        assert result[0]["column"] == "order_id"
        assert result[0]["table"] == "cat.sch.orders"
        assert result[0]["entity_type"] == "column_key"

    def test_blank_description_with_uc_comment_excluded(self):
        """Columns with a UC comment but no Genie description are NOT eligible."""
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.orders",
                "description": [],
                "column_configs": [
                    {
                        "column_name": "order_id",
                        "description": None,
                        "data_type": "BIGINT",
                        "uc_comment": "Primary key for orders",
                    },
                ],
            }
        ])
        assert _collect_blank_columns(snap) == []

    def test_hidden_columns_excluded(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.orders",
                "description": [],
                "column_configs": [
                    {"column_name": "internal_id", "description": None, "data_type": "BIGINT", "hidden": True},
                ],
            }
        ])
        assert _collect_blank_columns(snap) == []

    def test_sibling_columns_populated(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.orders",
                "description": ["Order data"],
                "column_configs": [
                    {"column_name": "order_id", "description": None, "data_type": "BIGINT"},
                    {"column_name": "amount", "description": ["Order amount in USD"], "data_type": "DOUBLE"},
                    {"column_name": "status", "description": ["Current fulfillment status"], "data_type": "STRING"},
                ],
            }
        ])
        result = _collect_blank_columns(snap)
        assert len(result) == 1
        assert "amount" in result[0]["sibling_columns"]
        assert "status" in result[0]["sibling_columns"]

    def test_empty_list_description_is_blank(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.t",
                "description": [],
                "column_configs": [
                    {"column_name": "col_a", "description": [], "data_type": "STRING"},
                ],
            }
        ])
        result = _collect_blank_columns(snap)
        assert len(result) == 1
        assert result[0]["column"] == "col_a"

    def test_multiple_tables_multiple_blanks(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.t1",
                "description": [],
                "column_configs": [
                    {"column_name": "a", "description": None, "data_type": "INT"},
                    {"column_name": "b", "description": ["A longer column description"], "data_type": "INT"},
                ],
            },
            {
                "identifier": "cat.sch.t2",
                "description": [],
                "column_configs": [
                    {"column_name": "x", "description": [""], "data_type": "STRING"},
                ],
            },
        ])
        result = _collect_blank_columns(snap)
        assert len(result) == 2
        tables = {r["table"] for r in result}
        assert tables == {"cat.sch.t1", "cat.sch.t2"}

    def test_short_description_under_threshold_is_insufficient(self):
        """Descriptions shorter than _MIN_DESCRIPTION_LENGTH are treated as insufficient."""
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.t1",
                "description": [],
                "column_configs": [
                    {"column_name": "a", "description": ["col"], "data_type": "INT"},
                ],
            }
        ])
        result = _collect_blank_columns(snap)
        assert len(result) == 1
        assert result[0]["column"] == "a"

    def test_short_uc_comment_does_not_exclude(self):
        """UC comments shorter than threshold don't prevent enrichment."""
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.t1",
                "description": [],
                "column_configs": [
                    {"column_name": "a", "description": None, "data_type": "INT", "uc_comment": "col"},
                ],
            }
        ])
        result = _collect_blank_columns(snap)
        assert len(result) == 1


class TestFormatEnrichmentContext:
    def test_basic_formatting(self):
        blanks = [
            {
                "table": "cat.sch.orders",
                "column": "amount",
                "data_type": "DOUBLE",
                "entity_type": "column_measure",
                "table_description": "Order data",
                "sibling_columns": ["order_id", "amount", "status"],
            },
        ]
        ctx = _format_enrichment_context(blanks)
        assert "cat.sch.orders" in ctx
        assert "amount (DOUBLE) [column_measure]" in ctx
        assert "Sibling columns" in ctx
        assert "order_id" in ctx
        assert "status" in ctx

    def test_target_column_excluded_from_siblings(self):
        blanks = [
            {
                "table": "cat.sch.t",
                "column": "col_a",
                "data_type": "STRING",
                "entity_type": "column_dim",
                "table_description": "",
                "sibling_columns": ["col_a", "col_b"],
            },
        ]
        ctx = _format_enrichment_context(blanks)
        lines = ctx.split("\n")
        sibling_line = [l for l in lines if "Sibling columns" in l]
        assert len(sibling_line) == 1
        assert "col_b" in sibling_line[0]
        assert "col_a" not in sibling_line[0].split("Sibling columns")[1]


# ---------------------------------------------------------------------------
# Proactive Table Description Enrichment
# ---------------------------------------------------------------------------


class TestCollectInsufficientTables:
    def _make_snapshot(self, tables, metric_views=None):
        ds = {"tables": tables}
        if metric_views:
            ds["metric_views"] = metric_views
        return {"data_sources": ds}

    def test_no_tables_need_enrichment(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.orders",
                "description": "Transactional order data with one row per order",
                "column_configs": [
                    {"column_name": "order_id", "data_type": "BIGINT"},
                ],
            }
        ])
        assert _collect_insufficient_tables(snap) == []

    def test_empty_description_is_insufficient(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.orders",
                "description": "",
                "column_configs": [
                    {"column_name": "order_id", "data_type": "BIGINT"},
                    {"column_name": "amount", "data_type": "DOUBLE"},
                ],
            }
        ])
        result = _collect_insufficient_tables(snap)
        assert len(result) == 1
        assert result[0]["table"] == "cat.sch.orders"
        assert "order_id" in result[0]["column_names"]
        assert "amount" in result[0]["column_names"]
        assert result[0]["is_metric_view"] is False

    def test_short_description_is_insufficient(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.t1",
                "description": "table1",
                "column_configs": [
                    {"column_name": "a", "data_type": "INT"},
                ],
            }
        ])
        result = _collect_insufficient_tables(snap)
        assert len(result) == 1
        assert result[0]["current_description"] == "table1"

    def test_none_description_is_insufficient(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.t1",
                "description": None,
                "column_configs": [],
            }
        ])
        result = _collect_insufficient_tables(snap)
        assert len(result) == 1

    def test_list_description_below_threshold(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.t1",
                "description": ["ab", "cd"],
                "column_configs": [],
            }
        ])
        result = _collect_insufficient_tables(snap)
        assert len(result) == 1

    def test_list_description_above_threshold(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.t1",
                "description": ["A long enough description for the table"],
                "column_configs": [],
            }
        ])
        assert _collect_insufficient_tables(snap) == []

    def test_metric_view_flagged(self):
        snap = self._make_snapshot(
            tables=[],
            metric_views=[
                {
                    "identifier": "cat.sch.mv1",
                    "description": "",
                    "column_configs": [
                        {"column_name": "metric_a", "data_type": "DOUBLE"},
                    ],
                }
            ],
        )
        result = _collect_insufficient_tables(snap)
        assert len(result) == 1
        assert result[0]["is_metric_view"] is True

    def test_column_types_populated(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.t1",
                "description": "",
                "column_configs": [
                    {"column_name": "id", "data_type": "BIGINT"},
                    {"column_name": "name", "data_type": "STRING"},
                ],
            }
        ])
        result = _collect_insufficient_tables(snap)
        assert result[0]["column_types"] == {"id": "BIGINT", "name": "STRING"}

    def test_mixed_sufficient_and_insufficient(self):
        snap = self._make_snapshot([
            {
                "identifier": "cat.sch.good",
                "description": "A table with a proper long description",
                "column_configs": [],
            },
            {
                "identifier": "cat.sch.bad",
                "description": "short",
                "column_configs": [],
            },
        ])
        result = _collect_insufficient_tables(snap)
        assert len(result) == 1
        assert result[0]["table"] == "cat.sch.bad"


class TestFormatTableEnrichmentContext:
    def test_basic_formatting(self):
        tables = [
            {
                "table": "cat.sch.orders",
                "current_description": "",
                "column_names": ["order_id", "amount", "status"],
                "column_types": {"order_id": "BIGINT", "amount": "DOUBLE", "status": "STRING"},
                "is_metric_view": False,
            },
        ]
        ctx = _format_table_enrichment_context(tables)
        assert "cat.sch.orders" in ctx
        assert "order_id (BIGINT)" in ctx
        assert "amount (DOUBLE)" in ctx
        assert "(none)" in ctx

    def test_existing_short_description_shown(self):
        tables = [
            {
                "table": "cat.sch.t1",
                "current_description": "short",
                "column_names": ["a"],
                "column_types": {"a": "INT"},
                "is_metric_view": False,
            },
        ]
        ctx = _format_table_enrichment_context(tables)
        assert "(short)" in ctx

    def test_metric_view_type_shown(self):
        tables = [
            {
                "table": "cat.sch.mv1",
                "current_description": "",
                "column_names": [],
                "column_types": {},
                "is_metric_view": True,
            },
        ]
        ctx = _format_table_enrichment_context(tables)
        assert "Metric View" in ctx


# ---------------------------------------------------------------------------
# Regex Relaxation Tests
# ---------------------------------------------------------------------------


class TestJoinRegexRelaxation:
    """Verify _JOIN_FQN_RE and _SQL_FROM_TABLE_RE match 2-part names."""

    def test_from_three_part_unquoted(self):
        sql = "SELECT * FROM catalog.schema.orders"
        m = _SQL_FROM_TABLE_RE.search(sql)
        assert m is not None
        assert m.group(1) == "catalog.schema.orders"

    def test_from_two_part_unquoted(self):
        sql = "SELECT * FROM schema.orders"
        m = _SQL_FROM_TABLE_RE.search(sql)
        assert m is not None
        assert m.group(1) == "schema.orders"

    def test_from_three_part_backticked(self):
        sql = "SELECT * FROM `catalog`.`schema`.`orders`"
        m = _SQL_FROM_TABLE_RE.search(sql)
        assert m is not None

    def test_from_two_part_backticked(self):
        sql = "SELECT * FROM `schema`.`orders`"
        m = _SQL_FROM_TABLE_RE.search(sql)
        assert m is not None

    def test_join_three_part_with_on(self):
        sql = (
            "SELECT * FROM catalog.schema.fact_sales f "
            "JOIN catalog.schema.dim_product p ON f.product_key = p.product_key"
        )
        matches = list(_JOIN_FQN_RE.finditer(sql))
        assert len(matches) == 1
        assert "dim_product" in matches[0].group(1)

    def test_join_two_part_with_on(self):
        sql = (
            "SELECT * FROM schema.fact_sales f "
            "JOIN schema.dim_product p ON f.product_key = p.product_key"
        )
        matches = list(_JOIN_FQN_RE.finditer(sql))
        assert len(matches) == 1
        assert "dim_product" in matches[0].group(1)

    def test_join_captures_alias(self):
        sql = "SELECT * FROM a.b.c JOIN a.b.d AS dim ON c.id = dim.id"
        matches = list(_JOIN_FQN_RE.finditer(sql))
        assert len(matches) == 1
        assert matches[0].group(2) == "dim"

    def test_join_without_alias(self):
        sql = "SELECT * FROM a.b.c JOIN a.b.d ON c.id = d.id"
        matches = list(_JOIN_FQN_RE.finditer(sql))
        assert len(matches) == 1
        assert matches[0].group(2) is None or matches[0].group(2) == ""


# ---------------------------------------------------------------------------
# Equijoin Extraction Tests
# ---------------------------------------------------------------------------


class TestExtractEquijoinPredicates:
    def test_simple_equijoin(self):
        result = _extract_equijoin_predicates("a.id = b.id")
        assert result == "a.id = b.id"

    def test_multiple_equijoins(self):
        result = _extract_equijoin_predicates(
            "a.id = b.id AND a.key = b.key"
        )
        assert "a.id = b.id" in result
        assert "a.key = b.key" in result

    def test_filters_non_equijoin_predicate(self):
        sql = "fact.product_key = dim.product_key AND dim.is_current = true"
        result = _extract_equijoin_predicates(sql)
        assert "product_key" in result
        assert "is_current" not in result

    def test_backtick_equijoin(self):
        result = _extract_equijoin_predicates("`a`.`id` = `b`.`id`")
        assert "id" in result

    def test_empty_returns_empty(self):
        assert _extract_equijoin_predicates("") == ""

    def test_no_equijoin_returns_empty(self):
        assert _extract_equijoin_predicates("1 = 1") == ""

    def test_sanitize_join_sql_strips_filter(self):
        sql = "fact.key = dim.key AND dim.is_active = true"
        result = _sanitize_join_sql(sql)
        assert "key" in result
        assert "is_active" not in result

    def test_sanitize_join_sql_strips_prose(self):
        sql = "fact.key = dim.key, MANY_TO_ONE. Use this join for lookups"
        result = _sanitize_join_sql(sql)
        assert "key" in result
        assert "MANY_TO_ONE" not in result
        assert "lookups" not in result


# ---------------------------------------------------------------------------
# Extract Proven Joins Tests
# ---------------------------------------------------------------------------


class TestExtractProvenJoins:
    """Tests for _extract_proven_joins with expanded verdicts and regex."""

    def _make_metadata(self, tables):
        return {
            "data_sources": {
                "tables": [
                    {"identifier": t, "column_configs": []}
                    for t in tables
                ],
                "metric_views": [],
            }
        }

    def test_both_correct_extracted(self):
        snap = self._make_metadata(["cat.sch.orders", "cat.sch.customers"])
        rows = [
            {
                "arbiter/value": "both_correct",
                "inputs/question_id": "q1",
                "request": {"expected_sql": "SELECT * FROM cat.sch.orders o JOIN cat.sch.customers c ON o.cust_id = c.id"},
                "response": {"response": "SELECT * FROM cat.sch.orders o JOIN cat.sch.customers c ON o.cust_id = c.id"},
            }
        ]
        result = _extract_proven_joins(rows, snap)
        assert len(result) >= 1
        tables = {(r["left_table"], r["right_table"]) for r in result}
        flat = set()
        for pair in tables:
            flat.update(pair)
        assert "cat.sch.orders" in flat
        assert "cat.sch.customers" in flat

    def test_ground_truth_correct_extracted(self):
        snap = self._make_metadata(["cat.sch.orders", "cat.sch.products"])
        rows = [
            {
                "arbiter/value": "ground_truth_correct",
                "inputs/question_id": "q2",
                "request": {"expected_sql": "SELECT * FROM cat.sch.orders o JOIN cat.sch.products p ON o.prod_id = p.id"},
                "response": {"response": ""},
            }
        ]
        result = _extract_proven_joins(rows, snap)
        assert len(result) >= 1

    def test_negative_verdict_skipped(self):
        snap = self._make_metadata(["cat.sch.orders", "cat.sch.products"])
        rows = [
            {
                "arbiter/value": "both_wrong",
                "inputs/question_id": "q3",
                "request": {"expected_sql": "SELECT * FROM cat.sch.orders o JOIN cat.sch.products p ON o.id = p.id"},
                "response": {"response": ""},
            }
        ]
        result = _extract_proven_joins(rows, snap)
        assert len(result) == 0

    def test_two_part_names_resolved(self):
        snap = self._make_metadata(["cat.sch.orders", "cat.sch.items"])
        rows = [
            {
                "arbiter/value": "genie_correct",
                "inputs/question_id": "q4",
                "request": {"expected_sql": "SELECT * FROM sch.orders o JOIN sch.items i ON o.id = i.order_id"},
                "response": {"response": ""},
            }
        ]
        result = _extract_proven_joins(rows, snap)
        assert len(result) >= 1


# ---------------------------------------------------------------------------
# FK Candidate Conversion Tests
# ---------------------------------------------------------------------------


class TestConvertFkToCandidates:
    def test_basic_conversion(self):
        fk_rows = [
            {
                "child_table": "cat.sch.orders",
                "child_columns": ["customer_id"],
                "parent_table": "cat.sch.customers",
                "parent_columns": ["id"],
                "constraint_name": "fk_orders_customer",
            }
        ]
        result = _convert_fk_to_candidates(fk_rows)
        assert len(result) == 1
        c = result[0]
        assert c["fk_constraint"] is True
        assert "customer_id" in c["on_condition"]
        assert c["frequency"] == 0

    def test_deduplicates_same_pair(self):
        fk_rows = [
            {
                "child_table": "cat.sch.a",
                "child_columns": ["id"],
                "parent_table": "cat.sch.b",
                "parent_columns": ["id"],
                "constraint_name": "fk1",
            },
            {
                "child_table": "cat.sch.b",
                "child_columns": ["id"],
                "parent_table": "cat.sch.a",
                "parent_columns": ["id"],
                "constraint_name": "fk2",
            },
        ]
        result = _convert_fk_to_candidates(fk_rows)
        assert len(result) == 1

    def test_skips_incomplete_rows(self):
        fk_rows = [
            {"child_table": "cat.sch.a", "child_columns": [], "parent_table": "cat.sch.b", "parent_columns": ["id"]},
        ]
        result = _convert_fk_to_candidates(fk_rows)
        assert len(result) == 0

    def test_composite_key(self):
        fk_rows = [
            {
                "child_table": "cat.sch.line_items",
                "child_columns": ["order_id", "product_id"],
                "parent_table": "cat.sch.order_products",
                "parent_columns": ["order_id", "product_id"],
                "constraint_name": "fk_composite",
            }
        ]
        result = _convert_fk_to_candidates(fk_rows)
        assert len(result) == 1
        assert "AND" in result[0]["on_condition"]


# ---------------------------------------------------------------------------
# Multi-catalog Ambiguity Tests
# ---------------------------------------------------------------------------


class TestMultiCatalogAmbiguity:
    """Verify short-name collisions across catalogs don't produce wrong joins."""

    def test_ambiguous_short_name_skipped(self):
        snap = {
            "data_sources": {
                "tables": [
                    {"identifier": "cat1.sch.orders", "column_configs": []},
                    {"identifier": "cat2.sch.orders", "column_configs": []},
                    {"identifier": "cat1.sch.customers", "column_configs": []},
                ],
                "metric_views": [],
            }
        }
        rows = [
            {
                "arbiter/value": "both_correct",
                "inputs/question_id": "q1",
                "request": {"expected_sql": "SELECT * FROM sch.orders o JOIN sch.customers c ON o.id = c.id"},
                "response": {"response": ""},
            }
        ]
        result = _extract_proven_joins(rows, snap)
        for cand in result:
            assert cand["left_table"] != cand["right_table"]

    def test_fqn_resolves_despite_ambiguous_short(self):
        snap = {
            "data_sources": {
                "tables": [
                    {"identifier": "cat1.sch.orders", "column_configs": []},
                    {"identifier": "cat2.sch.orders", "column_configs": []},
                    {"identifier": "cat1.sch.customers", "column_configs": []},
                ],
                "metric_views": [],
            }
        }
        rows = [
            {
                "arbiter/value": "both_correct",
                "inputs/question_id": "q1",
                "request": {"expected_sql": "SELECT * FROM cat1.sch.orders o JOIN cat1.sch.customers c ON o.id = c.id"},
                "response": {"response": ""},
            }
        ]
        result = _extract_proven_joins(rows, snap)
        assert len(result) >= 1
        flat_tables = set()
        for cand in result:
            flat_tables.add(cand["left_table"])
            flat_tables.add(cand["right_table"])
        assert "cat1.sch.orders" in flat_tables
        assert "cat1.sch.customers" in flat_tables


# ── Example SQL Array Parsing ──────────────────────────────────────────


class TestExampleSqlArrayParsing:
    """Test that generate_proposals_from_strategy handles both array and legacy formats."""

    _METADATA = {
        "tables": [{"name": "cat.sch.orders", "columns": []}],
        "functions": [],
        "metric_views": [],
        "config": {},
    }

    _STRATEGY_BASE = {
        "global_instruction_rewrite": "",
        "action_groups": [{"id": "AG1", "root_cause_summary": "test", "source_cluster_ids": ["C1"]}],
    }

    def _make_ag(self, lever5_dir):
        return {
            "id": "AG1",
            "root_cause_summary": "test routing",
            "source_cluster_ids": ["C1"],
            "affected_questions": ["q1"],
            "lever_directives": {"5": lever5_dir},
            "coordination_notes": "",
        }

    def test_legacy_single_example_sql(self):
        ag = self._make_ag({
            "instruction_guidance": "",
            "example_sql": {
                "question": "How many orders?",
                "sql_sketch": "SELECT COUNT(*) FROM cat.sch.orders",
            },
        })
        proposals = generate_proposals_from_strategy(
            strategy=self._STRATEGY_BASE,
            action_group=ag,
            metadata_snapshot=self._METADATA,
            target_lever=5,
        )
        ex_proposals = [p for p in proposals if p.get("patch_type") == "add_example_sql"]
        assert len(ex_proposals) == 1
        assert ex_proposals[0]["example_question"] == "How many orders?"

    def test_new_array_example_sqls(self):
        ag = self._make_ag({
            "instruction_guidance": "",
            "example_sqls": [
                {"question": "Q1?", "sql_sketch": "SELECT 1"},
                {"question": "Q2?", "sql_sketch": "SELECT 2"},
                {"question": "Q3?", "sql_sketch": "SELECT 3"},
            ],
        })
        proposals = generate_proposals_from_strategy(
            strategy=self._STRATEGY_BASE,
            action_group=ag,
            metadata_snapshot=self._METADATA,
            target_lever=5,
        )
        ex_proposals = [p for p in proposals if p.get("patch_type") == "add_example_sql"]
        assert len(ex_proposals) == 3
        questions = {p["example_question"] for p in ex_proposals}
        assert questions == {"Q1?", "Q2?", "Q3?"}

    def test_array_overrides_legacy(self):
        ag = self._make_ag({
            "instruction_guidance": "",
            "example_sql": {"question": "Legacy?", "sql_sketch": "SELECT 0"},
            "example_sqls": [
                {"question": "Array1?", "sql_sketch": "SELECT 1"},
            ],
        })
        proposals = generate_proposals_from_strategy(
            strategy=self._STRATEGY_BASE,
            action_group=ag,
            metadata_snapshot=self._METADATA,
            target_lever=5,
        )
        ex_proposals = [p for p in proposals if p.get("patch_type") == "add_example_sql"]
        assert len(ex_proposals) == 1
        assert ex_proposals[0]["example_question"] == "Array1?"

    def test_example_sql_from_non_lever5_array(self):
        ag = {
            "id": "AG1",
            "root_cause_summary": "test",
            "source_cluster_ids": ["C1"],
            "affected_questions": ["q1"],
            "lever_directives": {
                "1": {
                    "tables": [],
                    "columns": [],
                    "example_sqls": [
                        {"question": "Cross-lever Q?", "sql_sketch": "SELECT 1"},
                    ],
                },
            },
            "coordination_notes": "",
        }
        proposals = generate_proposals_from_strategy(
            strategy=self._STRATEGY_BASE,
            action_group=ag,
            metadata_snapshot=self._METADATA,
            target_lever=1,
        )
        ex_proposals = [p for p in proposals if p.get("patch_type") == "add_example_sql"]
        assert len(ex_proposals) == 1
        assert ex_proposals[0]["example_question"] == "Cross-lever Q?"


# ── Example SQL Dedup Against Existing Config ──────────────────────────


class TestExampleSqlDedup:
    """Test that _validate_lever5_proposals rejects duplicates against existing config."""

    _METADATA = {
        "tables": [{"name": "cat.sch.orders", "columns": []}],
        "functions": [],
        "metric_views": [],
        "config": {
            "example_question_sqls": [
                {"question": ["How many orders are there?"], "sql": "SELECT COUNT(*) FROM orders"},
            ],
        },
    }

    def test_rejects_duplicate_of_existing(self):
        proposals = [
            {
                "patch_type": "add_example_sql",
                "example_question": "How many orders are there?",
                "example_sql": "SELECT COUNT(*) FROM cat.sch.orders",
                "parameters": [],
            },
        ]
        result = _validate_lever5_proposals(proposals, self._METADATA)
        assert len(result) == 0

    def test_accepts_novel_question(self):
        proposals = [
            {
                "patch_type": "add_example_sql",
                "example_question": "What is the average order value?",
                "example_sql": "SELECT AVG(amount) FROM cat.sch.orders",
                "parameters": [],
            },
        ]
        result = _validate_lever5_proposals(proposals, self._METADATA)
        assert len(result) == 1

    def test_dedup_within_batch(self):
        proposals = [
            {
                "patch_type": "add_example_sql",
                "example_question": "Unique question?",
                "example_sql": "SELECT 1",
                "parameters": [],
            },
            {
                "patch_type": "add_example_sql",
                "example_question": "Unique question?",
                "example_sql": "SELECT 2",
                "parameters": [],
            },
        ]
        result = _validate_lever5_proposals(proposals, self._METADATA)
        ex_results = [p for p in result if p.get("patch_type") == "add_example_sql"]
        assert len(ex_results) == 1

    def test_filter_no_op_rejects_existing(self):
        proposals = [
            {
                "patch_type": "add_example_sql",
                "example_question": "How many orders are there?",
                "example_sql": "SELECT COUNT(*) FROM cat.sch.orders",
            },
        ]
        result = _filter_no_op_proposals(proposals, self._METADATA)
        assert len(result) == 0

    def test_rejects_fuzzy_duplicate_of_existing(self):
        proposals = [
            {
                "patch_type": "add_example_sql",
                "example_question": "How many orders are there",
                "example_sql": "SELECT COUNT(*) FROM cat.sch.orders",
                "parameters": [],
            },
        ]
        result = _validate_lever5_proposals(proposals, self._METADATA)
        assert len(result) == 0

    def test_rejects_fuzzy_duplicate_within_batch(self):
        proposals = [
            {
                "patch_type": "add_example_sql",
                "example_question": "What is the total order count?",
                "example_sql": "SELECT COUNT(*) FROM cat.sch.orders",
                "parameters": [],
            },
            {
                "patch_type": "add_example_sql",
                "example_question": "What is total order count?",
                "example_sql": "SELECT COUNT(*) FROM cat.sch.orders",
                "parameters": [],
            },
        ]
        result = _validate_lever5_proposals(proposals, self._METADATA)
        ex_results = [p for p in result if p.get("patch_type") == "add_example_sql"]
        assert len(ex_results) == 1

    def test_accepts_genuinely_different_question(self):
        proposals = [
            {
                "patch_type": "add_example_sql",
                "example_question": "What is the average order value?",
                "example_sql": "SELECT AVG(amount) FROM cat.sch.orders",
                "parameters": [],
            },
        ]
        sim = _ngram_similarity(
            "what is the average order value?",
            "how many orders are there?",
        )
        assert sim < _EXAMPLE_SQL_SIMILARITY_THRESHOLD
        result = _validate_lever5_proposals(proposals, self._METADATA)
        assert len(result) == 1

    def test_filter_no_op_rejects_fuzzy_duplicate(self):
        proposals = [
            {
                "patch_type": "add_example_sql",
                "example_question": "How many orders are there",
                "example_sql": "SELECT COUNT(*) FROM cat.sch.orders",
            },
        ]
        result = _filter_no_op_proposals(proposals, self._METADATA)
        assert len(result) == 0

    def test_dedup_proposals_fuzzy_keeps_higher_impact(self):
        proposals = [
            {
                "patch_type": "add_example_sql",
                "proposed_value": "What is the total order count?",
                "example_question": "What is the total order count?",
                "example_sql": "SELECT COUNT(*) FROM orders",
                "net_impact": 0.5,
            },
            {
                "patch_type": "add_example_sql",
                "proposed_value": "What is total order count?",
                "example_question": "What is total order count?",
                "example_sql": "SELECT COUNT(*) FROM orders",
                "net_impact": 0.9,
            },
        ]
        result = _deduplicate_proposals(proposals)
        ex_results = [p for p in result if p.get("patch_type") == "add_example_sql"]
        assert len(ex_results) == 1
        assert ex_results[0]["net_impact"] == 0.9

    def test_dedup_proposals_fuzzy_keeps_first_when_equal_impact(self):
        proposals = [
            {
                "patch_type": "add_example_sql",
                "proposed_value": "Show me total revenue by region?",
                "example_question": "Show me total revenue by region?",
                "example_sql": "SELECT region, SUM(revenue) FROM sales GROUP BY region",
                "net_impact": 0.7,
            },
            {
                "patch_type": "add_example_sql",
                "proposed_value": "Show me the total revenue by region?",
                "example_question": "Show me the total revenue by region?",
                "example_sql": "SELECT region, SUM(revenue) FROM sales GROUP BY 1",
                "net_impact": 0.7,
            },
        ]
        result = _deduplicate_proposals(proposals)
        ex_results = [p for p in result if p.get("patch_type") == "add_example_sql"]
        assert len(ex_results) == 1
        assert ex_results[0]["example_question"] == "Show me total revenue by region?"


# ── Benchmark Mining ───────────────────────────────────────────────────


class TestMineBenchmarkExampleSqls:
    """Test _mine_benchmark_example_sqls helper."""

    _METADATA = {
        "tables": [{"name": "cat.sch.orders", "columns": []}],
        "functions": [],
        "metric_views": [],
        "config": {
            "example_question_sqls": [
                {"question": ["Existing question?"], "sql": "SELECT 1"},
            ],
        },
    }

    def test_produces_proposals_from_benchmarks(self):
        benchmarks = [
            {"question": "How many orders?", "expected_sql": "SELECT COUNT(*) FROM cat.sch.orders"},
            {"question": "Total revenue?", "expected_sql": "SELECT SUM(amount) FROM cat.sch.orders"},
        ]
        result = _mine_benchmark_example_sqls(benchmarks, self._METADATA)
        assert len(result) == 2
        for p in result:
            assert p["patch_type"] == "add_example_sql"
            assert p["example_question"]
            assert p["example_sql"]
            assert p["confidence"] == 0.95

    def test_skips_benchmarks_without_sql(self):
        benchmarks = [
            {"question": "No SQL here", "expected_sql": ""},
            {"question": "Also missing"},
        ]
        result = _mine_benchmark_example_sqls(benchmarks, self._METADATA)
        assert len(result) == 0

    def test_skips_duplicate_of_existing(self):
        benchmarks = [
            {"question": "Existing question?", "expected_sql": "SELECT 1"},
        ]
        result = _mine_benchmark_example_sqls(benchmarks, self._METADATA)
        assert len(result) == 0

    def test_dedup_within_benchmarks(self):
        benchmarks = [
            {"question": "Same question?", "expected_sql": "SELECT 1"},
            {"question": "Same question?", "expected_sql": "SELECT 2"},
        ]
        result = _mine_benchmark_example_sqls(benchmarks, self._METADATA)
        assert len(result) == 1

    def test_skips_fuzzy_duplicate_of_existing(self):
        benchmarks = [
            {"question": "Existing question", "expected_sql": "SELECT 1"},
        ]
        result = _mine_benchmark_example_sqls(benchmarks, self._METADATA)
        assert len(result) == 0

    def test_fuzzy_dedup_within_benchmarks(self):
        benchmarks = [
            {"question": "How many total orders?", "expected_sql": "SELECT COUNT(*) FROM cat.sch.orders"},
            {"question": "How many total orders", "expected_sql": "SELECT COUNT(*) FROM cat.sch.orders"},
        ]
        result = _mine_benchmark_example_sqls(benchmarks, self._METADATA)
        assert len(result) == 1

    def test_caps_at_slot_budget(self):
        """With 98 slots used, only 2 proposals should be produced from 5 valid benchmarks."""
        metadata = {
            "tables": [{"name": "cat.sch.orders", "columns": []}],
            "functions": [],
            "metric_views": [],
            "config": {"example_question_sqls": []},
            "data_sources": {
                "tables": [
                    {"identifier": f"cat.sch.t{i}", "description": [f"Table {i}"]}
                    for i in range(8)
                ],
            },
            "instructions": {
                "text_instructions": [{"id": "ti1", "content": ["text"]}],
                "example_question_sqls": [
                    {"id": f"eq{i:032x}", "question": [f"Q{i}?"], "sql": [f"SELECT {i}"]}
                    for i in range(85)
                ],
                "sql_functions": [
                    {"id": f"sf{i:032x}", "identifier": f"cat.sch.fn{i}"}
                    for i in range(4)
                ],
            },
        }
        # 1 (text) + 85 (examples) + 4 (functions) + 8 (table descs) = 98 slots
        from genie_space_optimizer.common.genie_schema import count_instruction_slots
        assert count_instruction_slots(metadata) == 98

        benchmarks = [
            {"question": f"Bench question {i}?", "expected_sql": f"SELECT {i} FROM cat.sch.orders"}
            for i in range(5)
        ]
        result = _mine_benchmark_example_sqls(benchmarks, metadata)
        assert len(result) == 2

    def test_caps_at_slot_budget(self):
        """Mining stops once the instruction slot budget is exhausted."""
        metadata = {
            "data_sources": {
                "tables": [],
                "metric_views": [],
            },
            "instructions": {
                "example_question_sqls": [
                    {"id": f"{i:032x}", "question": [f"Q{i}?"], "sql": [f"SELECT {i}"]}
                    for i in range(98)
                ],
            },
        }
        benchmarks = [
            {"question": f"New question {j}?", "expected_sql": f"SELECT {j} FROM t"}
            for j in range(5)
        ]
        result = _mine_benchmark_example_sqls(benchmarks, metadata)
        assert len(result) == 2
