from __future__ import annotations

import json

from genie_space_optimizer.optimization import benchmarking


def _contexts() -> dict[str, str]:
    return {
        "valid_assets_context": "assets",
        "tables_context": "tables",
        "column_allowlist": "columns",
        "metric_views_context": "metric views",
        "tvfs_context": "functions",
        "join_specs_context": "joins",
        "instructions_context": "instructions",
        "sample_questions_context": "samples",
        "data_profile_context": "profile",
    }


def _allowlist() -> dict:
    return {
        "assets": set(),
        "columns": set(),
        "column_index": {},
        "routines": set(),
    }


def test_curated_sql_generation_uses_stable_id_and_preserves_question(
    monkeypatch,
) -> None:
    captured: dict[str, str] = {}

    monkeypatch.setattr(benchmarking, "_build_schema_contexts", lambda *_a, **_k: _contexts())

    def _fake_call(_w, prompt, *_args, **_kwargs):
        captured["prompt"] = prompt
        return [{
            "question_id": "native_q1",
            "question": "Rewritten question. Join tickets to accounts.",
            "expected_sql": "SELECT 1",
            "unfixable_reason": None,
        }]

    monkeypatch.setattr(benchmarking, "_call_llm_for_scoring", _fake_call)
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.benchmarks.validate_ground_truth_sql",
        lambda *_a, **_k: (True, ""),
    )

    rows = benchmarking._generate_sql_for_curated_questions(
        None,
        {},
        [],
        [],
        [{
            "space_question_id": "native_q1",
            "question": "How many support tickets were created per account segment?",
            "expected_sql": "",
            "source": "genie_benchmark",
            "category": "user_benchmark",
        }],
        "cat",
        "sch",
        object(),
    )

    assert len(rows) == 1
    assert rows[0]["question"] == "How many support tickets were created per account segment?"
    assert rows[0]["space_question_id"] == "native_q1"
    assert rows[0]["expected_sql"] == "SELECT 1"
    assert '"question_id": "native_q1"' in captured["prompt"]


def test_sql_correction_reconstructs_original_row_by_id(monkeypatch) -> None:
    captured: dict[str, str] = {}
    original = {
        "id": "q1",
        "question": "Where did revenue from renewals increase?",
        "expected_sql": "SELEC 1",
        "source": "llm_generated",
        "provenance": "synthetic",
        "required_tables": [],
        "required_columns": [],
        "validation_error": "syntax error",
    }

    monkeypatch.setattr(benchmarking, "_build_schema_contexts", lambda *_a, **_k: _contexts())

    def _fake_call(_w, prompt, *_args, **_kwargs):
        captured["prompt"] = prompt
        return [{
            "question_id": "q1",
            "question": "Use table renewals and join it to accounts.",
            "expected_sql": "SELECT 1",
            "source": "model_controlled",
            "unfixable_reason": None,
        }]

    monkeypatch.setattr(benchmarking, "_call_llm_for_scoring", _fake_call)
    monkeypatch.setattr(benchmarking, "_validate_benchmark_sql", lambda *_a, **_k: (True, ""))

    rows = benchmarking._attempt_sql_correction(
        None,
        {},
        [],
        [],
        [original],
        "cat",
        "sch",
        object(),
        _allowlist(),
        correction_prompt_template="{{ benchmarks_to_fix }}",
        correction_prompt_key="benchmark_correction",
    )

    assert len(rows) == 1
    assert rows[0]["question"] == original["question"]
    assert rows[0]["source"] == "llm_generated"
    assert rows[0]["expected_sql"] == "SELECT 1"
    payload = json.loads(captured["prompt"])
    assert payload[0]["question_id"] == "q1"
    assert payload[0]["question"] == original["question"]


def test_sql_correction_ignores_response_without_known_identity(monkeypatch) -> None:
    monkeypatch.setattr(benchmarking, "_build_schema_contexts", lambda *_a, **_k: _contexts())
    monkeypatch.setattr(
        benchmarking,
        "_call_llm_for_scoring",
        lambda *_a, **_k: [{"question": "replacement", "expected_sql": "SELECT 1"}],
    )

    rows = benchmarking._attempt_sql_correction(
        None,
        {},
        [],
        [],
        [{"id": "q1", "question": "Original", "expected_sql": "bad"}],
        "cat",
        "sch",
        object(),
        _allowlist(),
        correction_prompt_template="{{ benchmarks_to_fix }}",
        correction_prompt_key="benchmark_correction",
    )

    assert rows == []


def test_generated_corpus_retains_native_id_for_curated_benchmark(monkeypatch) -> None:
    monkeypatch.setattr(
        "genie_space_optimizer.optimization.benchmarks.validate_question_sql_alignment",
        lambda rows: [
            {"question": row["question"], "aligned": True, "issues": []}
            for row in rows
        ],
    )
    monkeypatch.setattr(benchmarking, "_fill_coverage_gaps", lambda **_kwargs: [])

    rows = benchmarking.generate_benchmarks(
        None,
        {},
        [],
        [],
        [],
        "support",
        "cat",
        "sch",
        object(),
        target_count=1,
        max_benchmark_count=1,
        genie_space_benchmarks=[{
            "space_question_id": "native_q1",
            "question": "How many support tickets were created?",
            "expected_sql": "SELECT 1",
            "source": "genie_benchmark",
            "provenance": "curated",
            "validation_status": "valid",
        }],
    )

    assert len(rows) == 1
    assert rows[0]["id"] == "native_q1"
    assert rows[0]["space_question_id"] == "native_q1"
    assert rows[0]["question"] == "How many support tickets were created?"
