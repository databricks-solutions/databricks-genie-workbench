"""GSO v2 — unified loop runs full native benchmark evals only."""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from unittest.mock import MagicMock

from genie_space_optimizer.optimization import eval_budget
from genie_space_optimizer.optimization import unified_loop


def test_target_accuracy_normalizes_fraction_to_percent() -> None:
    assert unified_loop.target_accuracy_percent(0.9) == 90.0
    assert unified_loop.target_accuracy_percent(90.0) == 90.0


def test_full_benchmark_estimate_is_one_full_run() -> None:
    one_full = eval_budget.estimate_full_benchmark_seconds(30)
    assert one_full == eval_budget.estimate_eval_run_seconds(30)
    three_gate = eval_budget.estimate_three_gate_seconds(working_set_size=30)
    assert one_full < three_gate


def test_unified_loop_does_not_call_slice_or_p0_gates() -> None:
    src = inspect.getsource(unified_loop.run_unified_optimization_loop)
    assert "_run_gate_checks" not in src
    assert "SLICE" not in src
    assert "P0" not in src
    assert "eval_scope=FULL" in src


def _eval_result(accuracy: float, *, failed: bool = False) -> dict:
    return {
        "overall_accuracy": accuracy,
        "total_questions": 2,
        "correct_count": int(accuracy > 0),
        "rows": [],
        "failures": [],
        "remaining_failures": [],
        "eval_run_failed": failed,
    }


def _structured_failure_eval_result(accuracy: float) -> dict:
    result = _eval_result(accuracy)
    result["rows"] = [
        {
            "question_id": "q1",
            "assessment": "BAD",
            "assessment_reasons": ["RESULT_MISSING_COLUMNS"],
        }
    ]
    return result


def test_unified_loop_stops_when_baseline_reaches_target(monkeypatch) -> None:
    writes: list[tuple[int, dict]] = []
    champions: list[int] = []
    status_updates: list[dict] = []

    monkeypatch.setattr(unified_loop, "fetch_space_config", lambda _w, _space_id: {"title": "s"})
    native_eval = MagicMock(return_value=_eval_result(95.0))
    monkeypatch.setattr(unified_loop, "_native_eval", native_eval)
    propose = MagicMock()
    monkeypatch.setattr(unified_loop, "propose_patches", propose)
    monkeypatch.setattr(
        unified_loop,
        "write_iteration",
        lambda _spark, _run_id, iteration, eval_result, **_kwargs: writes.append(
            (iteration, eval_result)
        ),
    )
    monkeypatch.setattr(
        unified_loop,
        "mark_champion_iteration",
        lambda _spark, _run_id, iteration, **_kwargs: champions.append(iteration),
    )
    monkeypatch.setattr(
        unified_loop,
        "update_run_status",
        lambda _spark, _run_id, _catalog, _schema, **kwargs: status_updates.append(kwargs),
    )

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        domain="default",
        benchmarks=[{"question": "q"}],
        catalog="cat",
        schema="sch",
        levers=[1],
        max_attempts=3,
        target_accuracy=0.9,
    )

    assert result["terminal_reason"] == "TARGET_REACHED"
    assert result["best_iteration"] == 0
    assert writes == [(0, _eval_result(95.0))]
    assert champions == [0]
    assert native_eval.call_count == 1
    propose.assert_not_called()
    assert status_updates[-1]["convergence_reason"] == "TARGET_REACHED"


def test_unified_loop_runs_space_quality_enrichment_before_baseline(monkeypatch) -> None:
    events: list[str] = []
    writes: list[tuple[int, dict]] = []

    raw_config = {
        "description": "",
        "_parsed_space": {
            "version": 2,
            "data_sources": {
                "tables": [{"identifier": "main.sales.orders"}],
                "metric_views": [],
                "functions": [],
            },
            "instructions": {"text_instructions": []},
        },
    }
    enriched_config = {
        **raw_config["_parsed_space"],
        "_gso_top_level_description": "Generated sales description",
    }

    monkeypatch.setattr(unified_loop, "fetch_space_config", lambda _w, _space_id: raw_config)

    def fake_enrichment(*_args, **_kwargs):
        events.append("enrichment")
        return SimpleNamespace(current_config=enriched_config)

    def fake_eval(*_args, **_kwargs):
        events.append("baseline")
        return _eval_result(95.0)

    monkeypatch.setattr(unified_loop, "run_space_quality_enrichment", fake_enrichment)
    monkeypatch.setattr(unified_loop, "_native_eval", fake_eval)
    monkeypatch.setattr(unified_loop, "propose_patches", MagicMock())
    monkeypatch.setattr(
        unified_loop,
        "write_iteration",
        lambda _spark, _run_id, iteration, eval_result, **_kwargs: writes.append(
            (iteration, eval_result)
        ),
    )
    monkeypatch.setattr(unified_loop, "mark_champion_iteration", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(unified_loop, "update_run_status", lambda *_args, **_kwargs: None)

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        domain="default",
        benchmarks=[{"question": "q"}],
        catalog="cat",
        schema="sch",
        levers=[1],
        max_attempts=3,
        target_accuracy=0.9,
    )

    assert events == ["enrichment", "baseline"]
    assert result["terminal_reason"] == "TARGET_REACHED"
    assert writes == [(0, _eval_result(95.0))]


def test_unified_loop_rolls_back_non_improving_candidate(monkeypatch) -> None:
    writes: list[int] = []
    rolled_back: list[tuple[int, str]] = []
    champions: list[int] = []

    monkeypatch.setattr(unified_loop, "fetch_space_config", lambda _w, _space_id: {"title": "s"})
    monkeypatch.setattr(
        unified_loop,
        "_native_eval",
        MagicMock(side_effect=[_eval_result(50.0), _eval_result(40.0)]),
    )
    monkeypatch.setattr(
        unified_loop,
        "propose_patches",
        lambda *_args, **_kwargs: (
            1,
            "try table description",
            [{"type": "update_description", "lever": 1, "target": "t", "new_text": "better"}],
            "{}",
        ),
    )
    monkeypatch.setattr(
        unified_loop,
        "apply_patch_set",
        lambda *_args, **_kwargs: {
            "patch_deployed": True,
            "post_snapshot": {"title": "candidate"},
            "applied": [
                {
                    "patch": {"type": "update_description", "lever": 1, "target": "t"},
                    "action": {"risk_level": "low", "target": "t"},
                }
            ],
        },
    )
    rollback = MagicMock()
    monkeypatch.setattr(unified_loop, "rollback", rollback)
    monkeypatch.setattr(
        unified_loop,
        "write_iteration",
        lambda _spark, _run_id, iteration, _eval_result, **_kwargs: writes.append(iteration),
    )
    monkeypatch.setattr(unified_loop, "write_patch", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        unified_loop,
        "mark_patches_rolled_back",
        lambda _spark, _run_id, iteration, reason, *_args: rolled_back.append((iteration, reason)),
    )
    monkeypatch.setattr(unified_loop, "mark_iteration_rolled_back", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(unified_loop, "update_iteration_loop_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        unified_loop,
        "mark_champion_iteration",
        lambda _spark, _run_id, iteration, **_kwargs: champions.append(iteration),
    )
    monkeypatch.setattr(unified_loop, "update_run_status", lambda *_args, **_kwargs: None)

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        domain="default",
        benchmarks=[{"question": "q"}],
        catalog="cat",
        schema="sch",
        levers=[1],
        max_attempts=1,
        target_accuracy=90.0,
    )

    assert result["terminal_reason"] == "MAX_ATTEMPTS"
    assert result["best_iteration"] == 0
    assert result["levers_rolled_back"] == [1]
    assert writes == [0, 1]
    assert rolled_back and rolled_back[0][0] == 1
    assert champions == [0]
    rollback.assert_called_once()


def test_unified_loop_retries_after_preapply_rejects_all_patches(monkeypatch) -> None:
    writes: list[int] = []
    patch_writes: list[tuple[int, int]] = []
    champions: list[int] = []

    monkeypatch.setattr(unified_loop, "fetch_space_config", lambda _w, _space_id: {"title": "s"})
    monkeypatch.setattr(
        unified_loop,
        "_native_eval",
        MagicMock(side_effect=[_eval_result(40.0), _eval_result(95.0)]),
    )
    propose = MagicMock(
        side_effect=[
            (
                5,
                "text instruction without routing evidence",
                [
                    {
                        "type": "add_instruction",
                        "lever": 5,
                        "new_text": "Ask for a time range when customer performance is ambiguous.",
                    }
                ],
                '{"patches": [{"type": "add_instruction"}]}',
            ),
            (
                1,
                "use metadata instead",
                [
                    {
                        "type": "update_description",
                        "lever": 1,
                        "target": "cat.sch.orders",
                        "new_text": "Orders table for regional sales analysis.",
                    }
                ],
                '{"patches": [{"type": "update_description"}]}',
            ),
        ]
    )
    monkeypatch.setattr(unified_loop, "propose_patches", propose)
    monkeypatch.setattr(
        unified_loop,
        "apply_patch_set",
        lambda *_args, **_kwargs: {
            "patch_deployed": True,
            "post_snapshot": {"title": "candidate"},
            "applied": [
                {
                    "patch": {"type": "update_description", "lever": 1, "target": "cat.sch.orders"},
                    "action": {"risk_level": "low", "target": "cat.sch.orders"},
                }
            ],
        },
    )
    monkeypatch.setattr(
        unified_loop,
        "write_iteration",
        lambda _spark, _run_id, iteration, _eval_result, **_kwargs: writes.append(iteration),
    )
    monkeypatch.setattr(
        unified_loop,
        "write_patch",
        lambda _spark, _run_id, iteration, lever, *_args: patch_writes.append((iteration, lever)),
    )
    monkeypatch.setattr(unified_loop, "update_iteration_loop_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        unified_loop,
        "mark_champion_iteration",
        lambda _spark, _run_id, iteration, **_kwargs: champions.append(iteration),
    )
    monkeypatch.setattr(unified_loop, "update_run_status", lambda *_args, **_kwargs: None)

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        domain="default",
        benchmarks=[{"question": "q"}],
        catalog="cat",
        schema="sch",
        levers=[1, 5],
        max_attempts=1,
        target_accuracy=90.0,
    )

    assert result["terminal_reason"] == "TARGET_REACHED"
    assert result["surgical_attempts_used"] == 1
    assert propose.call_count == 2
    assert writes == [0, 1]
    assert patch_writes == [(1, 1)]
    assert champions == [1]
    assert result["reflections"][0]["stage"] == "preapply_rejected_all_patches"
    assert result["reflections"][0]["dropped_patch_summary"][0]["drop_reason"] == (
        "instruction_routing_unjustified"
    )


def test_unified_loop_retries_when_structured_patch_drops_to_text_only(
    monkeypatch,
) -> None:
    writes: list[int] = []
    patch_writes: list[tuple[int, int]] = []
    applied_patch_sets: list[list[str]] = []
    champions: list[int] = []

    monkeypatch.setattr(unified_loop, "fetch_space_config", lambda _w, _space_id: {"title": "s"})
    monkeypatch.setattr(
        unified_loop,
        "_native_eval",
        MagicMock(side_effect=[_structured_failure_eval_result(40.0), _eval_result(95.0)]),
    )

    def fail_snippet_validation(*args, **kwargs):
        return False, "Execution failed: SELECT SELECT ...", args[0]

    monkeypatch.setattr(
        "genie_space_optimizer.optimization.benchmarks.validate_sql_snippet",
        fail_snippet_validation,
    )

    propose = MagicMock(
        side_effect=[
            (
                6,
                "try a snippet and instruction",
                [
                    {
                        "type": "add_sql_snippet_measure",
                        "lever": 6,
                        "sql": "SELECT account_type, COUNT(*) FROM accounts GROUP BY account_type",
                        "display_name": "Account Type Count",
                        "instruction": "Use to count accounts by type.",
                        "synonyms": ["account type count"],
                        "target_table": "cat.sch.accounts",
                        "snippet_type": "measure",
                    },
                    {
                        "type": "update_instruction_section",
                        "lever": 5,
                        "section": "DISAMBIGUATION",
                        "new_text": "Prefer explicit account type terminology when users ask for account categories.",
                        "routing_evidence": [
                            {
                                "type": "structured_behavior",
                                "reason": "The structured SQL snippet was also attempted.",
                            }
                        ],
                    },
                ],
                '{"patches": [{"type": "add_sql_snippet_measure"}, {"type": "update_instruction_section"}]}',
            ),
            (
                4,
                "use join spec instead",
                [
                    {
                        "type": "add_join_spec",
                        "lever": 4,
                        "target": "cat.sch.accounts",
                        "join_spec": {"left_table": "accounts", "right_table": "customers"},
                    }
                ],
                '{"patches": [{"type": "add_join_spec"}]}',
            ),
        ]
    )
    monkeypatch.setattr(unified_loop, "propose_patches", propose)

    def apply_patch_set(_w, _space_id, patches, *_args, **_kwargs):
        applied_patch_sets.append([p["type"] for p in patches])
        patch = patches[0]
        return {
            "patch_deployed": True,
            "post_snapshot": {"title": "candidate"},
            "applied": [
                {
                    "patch": patch,
                    "action": {"risk_level": "low", "target": patch.get("target", "")},
                }
            ],
        }

    monkeypatch.setattr(unified_loop, "apply_patch_set", apply_patch_set)
    monkeypatch.setattr(
        unified_loop,
        "write_iteration",
        lambda _spark, _run_id, iteration, _eval_result, **_kwargs: writes.append(iteration),
    )
    monkeypatch.setattr(
        unified_loop,
        "write_patch",
        lambda _spark, _run_id, iteration, lever, *_args: patch_writes.append((iteration, lever)),
    )
    monkeypatch.setattr(unified_loop, "update_iteration_loop_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        unified_loop,
        "mark_champion_iteration",
        lambda _spark, _run_id, iteration, **_kwargs: champions.append(iteration),
    )
    monkeypatch.setattr(unified_loop, "update_run_status", lambda *_args, **_kwargs: None)

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        domain="default",
        benchmarks=[{"question": "q"}],
        catalog="cat",
        schema="sch",
        levers=[4, 5, 6],
        max_attempts=1,
        target_accuracy=90.0,
    )

    assert result["terminal_reason"] == "TARGET_REACHED"
    assert result["surgical_attempts_used"] == 1
    assert propose.call_count == 2
    assert applied_patch_sets == [["add_join_spec"]]
    assert writes == [0, 1]
    assert patch_writes == [(1, 4)]
    assert champions == [1]
    assert result["reflections"][0]["stage"] == "preapply_lost_structured_intent"
    assert result["reflections"][0]["dropped_patch_summary"][0]["type"] == (
        "add_sql_snippet_measure"
    )


def test_unified_loop_preserves_no_hypothesis_details_after_retry(monkeypatch) -> None:
    loop_states: list[dict] = []
    champions: list[int] = []

    monkeypatch.setattr(unified_loop, "fetch_space_config", lambda _w, _space_id: {"title": "s"})
    monkeypatch.setattr(unified_loop, "_native_eval", MagicMock(return_value=_eval_result(40.0)))
    propose = MagicMock(
        return_value=(
            None,
            "no actionable patch",
            [],
            '{"rationale": "no actionable patch", "patches": []}',
        )
    )
    monkeypatch.setattr(unified_loop, "propose_patches", propose)
    apply_patch_set = MagicMock()
    monkeypatch.setattr(unified_loop, "apply_patch_set", apply_patch_set)
    monkeypatch.setattr(unified_loop, "write_iteration", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(unified_loop, "write_patch", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        unified_loop,
        "update_iteration_loop_state",
        lambda *_args, **kwargs: loop_states.append(kwargs["loop_state"]),
    )
    monkeypatch.setattr(
        unified_loop,
        "mark_champion_iteration",
        lambda _spark, _run_id, iteration, **_kwargs: champions.append(iteration),
    )
    monkeypatch.setattr(unified_loop, "update_run_status", lambda *_args, **_kwargs: None)

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        domain="default",
        benchmarks=[{"question": "q"}],
        catalog="cat",
        schema="sch",
        levers=[1, 5],
        max_attempts=1,
        target_accuracy=90.0,
    )

    assert result["terminal_reason"] == "NO_NEW_HYPOTHESIS"
    assert result["surgical_attempts_used"] == 0
    assert propose.call_count == 2
    apply_patch_set.assert_not_called()
    assert champions == [0]
    terminal_state = loop_states[-1]
    assert terminal_state["current_hypothesis"]["failure_stage"] == "llm_no_supported_patches"
    assert terminal_state["decision_reason"] == (
        "NO_NEW_HYPOTHESIS: LLM returned no supported patches"
    )
    assert terminal_state["do_not_repeat"][0]["stage"] == "llm_no_supported_patches"


def test_unified_loop_rejects_repeated_metadata_only_for_structured_failures(
    monkeypatch,
) -> None:
    writes: list[int] = []
    patch_writes: list[tuple[int, int]] = []

    monkeypatch.setattr(unified_loop, "fetch_space_config", lambda _w, _space_id: {"title": "s"})
    monkeypatch.setattr(
        unified_loop,
        "_native_eval",
        MagicMock(
            side_effect=[
                _structured_failure_eval_result(40.0),
                _structured_failure_eval_result(50.0),
                _structured_failure_eval_result(60.0),
            ]
        ),
    )
    propose = MagicMock(
        side_effect=[
            (
                1,
                "first clarify table",
                [
                    {
                        "type": "update_description",
                        "lever": 1,
                        "target": "cat.sch.orders",
                        "new_text": "Orders table for sales analysis.",
                    }
                ],
                '{"patches": [{"type": "update_description"}]}',
            ),
            (
                1,
                "clarify another column",
                [
                    {
                        "type": "update_column_description",
                        "lever": 1,
                        "table": "cat.sch.orders",
                        "column": "amount",
                        "new_text": "Order amount.",
                    }
                ],
                '{"patches": [{"type": "update_column_description"}]}',
            ),
            (
                4,
                "use join spec instead",
                [
                    {
                        "type": "add_join_spec",
                        "lever": 4,
                        "target": "cat.sch.orders",
                        "join_spec": {"left_table": "orders", "right_table": "customers"},
                    }
                ],
                '{"patches": [{"type": "add_join_spec"}]}',
            ),
        ]
    )
    monkeypatch.setattr(unified_loop, "propose_patches", propose)

    def apply_patch_set(_w, _space_id, patches, *_args, **_kwargs):
        patch = patches[0]
        return {
            "patch_deployed": True,
            "post_snapshot": {"title": f"candidate-{patch['type']}"},
            "applied": [
                {
                    "patch": patch,
                    "action": {"risk_level": "low", "target": patch.get("target", "")},
                }
            ],
        }

    monkeypatch.setattr(unified_loop, "apply_patch_set", apply_patch_set)
    monkeypatch.setattr(
        unified_loop,
        "write_iteration",
        lambda _spark, _run_id, iteration, _eval_result, **_kwargs: writes.append(iteration),
    )
    monkeypatch.setattr(
        unified_loop,
        "write_patch",
        lambda _spark, _run_id, iteration, lever, *_args: patch_writes.append((iteration, lever)),
    )
    monkeypatch.setattr(unified_loop, "update_iteration_loop_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(unified_loop, "mark_champion_iteration", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(unified_loop, "update_run_status", lambda *_args, **_kwargs: None)

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        domain="default",
        benchmarks=[{"question": "q"}],
        catalog="cat",
        schema="sch",
        levers=[1, 4],
        max_attempts=2,
        target_accuracy=90.0,
    )

    assert result["terminal_reason"] == "MAX_ATTEMPTS"
    assert result["surgical_attempts_used"] == 2
    assert result["levers_accepted"] == [1, 4]
    assert propose.call_count == 3
    assert writes == [0, 1, 2]
    assert patch_writes == [(1, 1), (2, 4)]
    rejected = [
        r for r in result["reflections"]
        if r.get("stage") == "preapply_rejected_all_patches"
    ]
    assert rejected
    assert rejected[0]["dropped_patch_summary"][0]["drop_reason"] == (
        "metadata_repeat_without_structured_behavior"
    )


def test_unified_loop_pivots_after_benchmark_leak_wipeout(monkeypatch) -> None:
    """Reproduces run f5edfa0d: the LLM proposes add_example_sql patches that

    the firewall wipes as benchmark leaks. Rather than terminating
    NO_NEW_HYPOTHESIS at iteration 0, the loop must ban add_example_sql, steer
    the retry via banned_patch_types, and pivot to a surviving snippet patch
    that improves accuracy (Fixes #1 + #2).
    """
    writes: list[int] = []
    patch_writes: list[tuple[int, int]] = []
    champions: list[int] = []
    propose_banned_kwargs: list[set] = []

    monkeypatch.setattr(unified_loop, "fetch_space_config", lambda _w, _space_id: {"title": "s"})
    monkeypatch.setattr(
        unified_loop,
        "_native_eval",
        MagicMock(side_effect=[_structured_failure_eval_result(53.3), _eval_result(95.0)]),
    )

    def propose(_w, *, allowed_levers, current_config, eval_result, reflections, catalog, schema, banned_patch_types=None):
        propose_banned_kwargs.append(set(banned_patch_types or ()))
        if "add_example_sql" not in (banned_patch_types or set()):
            # First proposal: full-query example SQL (will be firewalled).
            return (
                5,
                "teach output shape with example SQL",
                [
                    {
                        "type": "add_example_sql",
                        "lever": 5,
                        "example_question": "ranking within partitions",
                        "example_sql": "WITH base AS (SELECT ...) SELECT ...",
                        "usage_guidance": "g",
                        "source_failure_pattern": "ranking",
                        "affected_qids": ["q1"],
                        "semantic_delta_from_benchmark": "d",
                        "why_not_benchmark_copy": "w",
                    }
                ],
                '{"lever": 5, "patches": [{"type": "add_example_sql"}]}',
            )
        # Pivot proposal: reusable expression primitive (firewall-exempt).
        return (
            6,
            "teach the ranking primitive as a reusable expression",
            [
                {
                    "type": "add_sql_snippet_expression",
                    "lever": 6,
                    "sql": "RANK() OVER (PARTITION BY product_category ORDER BY avg_balance DESC)",
                    "display_name": "Rank within category",
                    "instruction": "Use to rank rows within a category.",
                    "synonyms": ["rank in category"],
                    "target_table": "cat.sch.accounts",
                    "snippet_type": "expression",
                }
            ],
            '{"lever": 6, "patches": [{"type": "add_sql_snippet_expression"}]}',
        )

    monkeypatch.setattr(unified_loop, "propose_patches", propose)

    def screen(patches, **_kwargs):
        # Simulate the firewall: drop every add_example_sql as a benchmark
        # leak; let structured snippet patches through.
        kept, dropped = [], []
        for p in patches:
            if p["type"] == "add_example_sql":
                d = dict(p)
                d["drop_reason"] = "benchmark_example_sql_leak"
                d["drop_detail"] = "ngram_similarity_sql_qid=gs_001"
                dropped.append(d)
            else:
                kept.append(p)
        return kept, dropped

    monkeypatch.setattr(unified_loop, "_preapply_safety_screen", screen)

    def apply_patch_set(_w, _space_id, patches, *_args, **_kwargs):
        patch = patches[0]
        return {
            "patch_deployed": True,
            "post_snapshot": {"title": "candidate"},
            "applied": [
                {
                    "patch": patch,
                    "action": {"risk_level": "low", "target": patch.get("target_table", "")},
                }
            ],
        }

    monkeypatch.setattr(unified_loop, "apply_patch_set", apply_patch_set)
    monkeypatch.setattr(
        unified_loop,
        "write_iteration",
        lambda _spark, _run_id, iteration, _eval_result, **_kwargs: writes.append(iteration),
    )
    monkeypatch.setattr(
        unified_loop,
        "write_patch",
        lambda _spark, _run_id, iteration, lever, *_args: patch_writes.append((iteration, lever)),
    )
    monkeypatch.setattr(unified_loop, "update_iteration_loop_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        unified_loop,
        "mark_champion_iteration",
        lambda _spark, _run_id, iteration, **_kwargs: champions.append(iteration),
    )
    monkeypatch.setattr(unified_loop, "update_run_status", lambda *_args, **_kwargs: None)

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        domain="default",
        benchmarks=[{"question": "q"}],
        catalog="cat",
        schema="sch",
        levers=[1, 2, 3, 4, 5, 6],
        max_attempts=3,
        target_accuracy=90.0,
    )

    # The run pivoted and applied the surviving snippet instead of dying at 0.
    assert result["terminal_reason"] == "TARGET_REACHED"
    assert result["surgical_attempts_used"] == 1
    assert result["levers_accepted"] == [6]
    assert patch_writes == [(1, 6)]
    assert champions == [1]
    # First proposal saw no ban; the pivot proposal was told to avoid example SQL.
    assert propose_banned_kwargs[0] == set()
    assert "add_example_sql" in propose_banned_kwargs[1]
    # A leak-wipeout reflection recorded the ban and the viable pivot set.
    leak_reflections = [
        r for r in result["reflections"]
        if r.get("banned_patch_types")
    ]
    assert leak_reflections
    assert "add_example_sql" in leak_reflections[0]["banned_patch_types"]
    assert "add_sql_snippet_expression" in leak_reflections[0]["viable_patch_types"]


# ─────────────────────────────────────────────────────────────────────
# Fix B — transient eval retry; Option A-min — champion preservation
# ─────────────────────────────────────────────────────────────────────


def test_native_eval_retries_transient_timeout_then_succeeds(monkeypatch) -> None:
    """Fix B: an EVALUATION_TIMEOUT is retried and a later success is returned.

    Reproduces run 32f5b395's restart baseline, which timed out once. The retry
    must recover instead of surfacing eval_run_failed.
    """
    monkeypatch.setattr(
        unified_loop, "resolve_space_benchmark_qids", lambda _w, _s, _b: ["q1"]
    )
    monkeypatch.setattr(unified_loop, "OfficialBenchmarkRunner", lambda _w: MagicMock())

    statuses = iter(["EVALUATION_TIMEOUT", "DONE"])

    def fake_build(_result, *, iteration, eval_scope, model_id=None):
        status = next(statuses)
        return {
            "eval_run_status": status,
            "eval_run_failed": status != "DONE",
            "overall_accuracy": 0.0 if status != "DONE" else 76.67,
            "rows": [],
        }

    monkeypatch.setattr(unified_loop, "build_eval_output_from_official", fake_build)

    out = unified_loop._native_eval(
        MagicMock(), space_id="space", benchmarks=[{"question": "q"}], iteration=0,
    )
    assert out["eval_run_status"] == "DONE"
    assert out["eval_run_failed"] is False
    assert out["overall_accuracy"] == 76.67


def test_native_eval_does_not_retry_hard_eval_failure(monkeypatch) -> None:
    """Fix B: EVALUATION_FAILED is NOT transient — it is returned on attempt 1."""
    monkeypatch.setattr(
        unified_loop, "resolve_space_benchmark_qids", lambda _w, _s, _b: ["q1"]
    )
    monkeypatch.setattr(unified_loop, "OfficialBenchmarkRunner", lambda _w: MagicMock())

    calls = {"n": 0}

    def fake_build(_result, *, iteration, eval_scope, model_id=None):
        calls["n"] += 1
        return {
            "eval_run_status": "EVALUATION_FAILED",
            "eval_run_failed": True,
            "overall_accuracy": 0.0,
            "rows": [],
        }

    monkeypatch.setattr(unified_loop, "build_eval_output_from_official", fake_build)

    out = unified_loop._native_eval(
        MagicMock(), space_id="space", benchmarks=[{"question": "q"}], iteration=0,
    )
    assert out["eval_run_failed"] is True
    assert calls["n"] == 1  # no retry


def test_native_eval_gives_up_after_max_transient_retries(monkeypatch) -> None:
    """Fix B: a persistently-timing-out eval stops after the bounded retries."""
    monkeypatch.setattr(
        unified_loop, "resolve_space_benchmark_qids", lambda _w, _s, _b: ["q1"]
    )
    monkeypatch.setattr(unified_loop, "OfficialBenchmarkRunner", lambda _w: MagicMock())

    calls = {"n": 0}

    def fake_build(_result, *, iteration, eval_scope, model_id=None):
        calls["n"] += 1
        return {
            "eval_run_status": "EVALUATION_TIMEOUT",
            "eval_run_failed": True,
            "overall_accuracy": 0.0,
            "rows": [],
        }

    monkeypatch.setattr(unified_loop, "build_eval_output_from_official", fake_build)

    out = unified_loop._native_eval(
        MagicMock(), space_id="space", benchmarks=[{"question": "q"}], iteration=0,
    )
    assert out["eval_run_failed"] is True
    assert calls["n"] == unified_loop._MAX_TRANSIENT_EVAL_RETRIES + 1  # 1 + 2 = 3


def test_failed_baseline_preserves_persisted_champion(monkeypatch) -> None:
    """Option A-min: a failed baseline eval on restart must stamp the persisted

    champion (iter 2 @ 76.67% committed by a prior execution), not baseline
    iteration 0 @ 0. Reproduces run 32f5b395's EVAL_INVALID clobber.
    """
    champions: list[int] = []
    status_updates: list[dict] = []

    monkeypatch.setattr(unified_loop, "fetch_space_config", lambda _w, _space_id: {"title": "s"})
    # Restart's baseline eval times out (eval_run_failed=True) even after Fix B retries.
    monkeypatch.setattr(
        unified_loop,
        "_native_eval",
        MagicMock(return_value=_eval_result(0.0, failed=True)),
    )
    # Prior execution already committed iterations to genie_opt_iterations.
    persisted = [
        {"iteration": 0, "eval_scope": "full", "overall_accuracy": 50.0, "rolled_back": False},
        {"iteration": 1, "eval_scope": "full", "overall_accuracy": 56.67, "rolled_back": False},
        {"iteration": 2, "eval_scope": "full", "overall_accuracy": 76.67, "rolled_back": False},
        # the current restart's timed-out baseline row scores 0.0 → never wins
        {"iteration": 0, "eval_scope": "full", "overall_accuracy": 0.0, "rolled_back": False},
    ]
    monkeypatch.setattr(
        unified_loop, "load_all_scored_iterations",
        lambda _spark, _run_id, _catalog, _schema: persisted,
    )
    monkeypatch.setattr(unified_loop, "write_iteration", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(unified_loop, "update_iteration_loop_state", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        unified_loop, "mark_champion_iteration",
        lambda _spark, _run_id, iteration, **_kwargs: champions.append(iteration),
    )
    monkeypatch.setattr(
        unified_loop, "update_run_status",
        lambda _spark, _run_id, _catalog, _schema, **kwargs: status_updates.append(kwargs),
    )

    result = unified_loop.run_unified_optimization_loop(
        MagicMock(),
        MagicMock(),
        run_id="run",
        space_id="space",
        domain="default",
        benchmarks=[{"question": "q"}],
        catalog="cat",
        schema="sch",
        levers=[1, 2, 3, 4, 5, 6],
        max_attempts=4,
        target_accuracy=85.0,
    )

    assert result["terminal_reason"] == "EVAL_INVALID"
    # The champion is the persisted iter 2, NOT baseline iteration 0.
    assert result["best_iteration"] == 2
    assert result["accuracy"] == 76.67
    assert champions[-1] == 2
    # The terminal run-status write carries the preserved champion, not 0/50.
    terminal_status = [s for s in status_updates if s.get("convergence_reason") == "EVAL_INVALID"]
    assert terminal_status
    assert terminal_status[-1]["best_iteration"] == 2
    assert terminal_status[-1]["best_accuracy"] == 76.67
