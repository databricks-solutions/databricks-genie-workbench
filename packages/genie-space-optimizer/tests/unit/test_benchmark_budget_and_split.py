from __future__ import annotations

from genie_space_optimizer.optimization.benchmarks import (
    assign_splits,
    benchmark_corpus_for_optimization,
    build_eval_records,
    load_benchmarks_from_dataset,
)


def _rows(n: int) -> list[dict]:
    return [
        {
            "id": f"q{i:03d}",
            "question": f"Question {i}",
            "provenance": "curated" if i < 10 else "synthetic",
        }
        for i in range(n)
    ]


def test_assign_splits_marks_every_question_full_scope() -> None:
    rows = assign_splits(_rows(30), seed=0)

    assert len(rows) == 30
    assert {row["split"] for row in rows} == {"full"}
    assert sum(1 for row in rows if row["provenance"] == "curated") == 10
    assert sum(1 for row in rows if row["provenance"] == "synthetic") == 20


def test_benchmark_corpus_for_optimization_ignores_legacy_split_labels() -> None:
    legacy_rows = _rows(30)
    for i, row in enumerate(legacy_rows):
        row["split"] = "held_out" if i < 5 else "train"

    corpus = benchmark_corpus_for_optimization(legacy_rows)

    assert len(corpus) == 30
    assert {row["split"] for row in corpus} == {"full"}
    assert [row["id"] for row in corpus] == [row["id"] for row in legacy_rows]


def test_build_eval_records_normalizes_legacy_split_labels() -> None:
    legacy_rows = _rows(30)
    for i, row in enumerate(legacy_rows):
        row["split"] = "held_out" if i < 5 else "train"

    records = build_eval_records(legacy_rows)

    assert len(records) == 30
    assert {record["expectations"]["split"] for record in records} == {"full"}


def test_list_backed_benchmark_loader_normalizes_legacy_split_labels() -> None:
    legacy_rows = _rows(30)
    for i, row in enumerate(legacy_rows):
        row["split"] = "held_out" if i < 5 else "train"

    loaded = load_benchmarks_from_dataset(legacy_rows, "cat.sch", "sales")

    assert len(loaded) == 30
    assert {row["split"] for row in loaded} == {"full"}


def test_assign_splits_small_corpus_still_uses_full_scope() -> None:
    rows = assign_splits(_rows(10), seed=42)

    assert len(rows) == 10
    assert {row["split"] for row in rows} == {"full"}


def test_legacy_held_out_scope_is_full_corpus_alias() -> None:
    from genie_space_optimizer.optimization.evaluation import filter_benchmarks_by_scope

    legacy_rows = _rows(30)
    for i, row in enumerate(legacy_rows):
        row["split"] = "held_out" if i < 5 else "train"

    scoped = filter_benchmarks_by_scope(legacy_rows, "held_out")

    assert len(scoped) == 30
    assert [row["id"] for row in scoped] == [row["id"] for row in legacy_rows]


def test_truncate_benchmarks_caps_to_thirty_and_prefers_user_rows() -> None:
    from genie_space_optimizer.optimization.evaluation import _truncate_benchmarks

    rows = [
        {"id": f"user-{i}", "question": f"User {i}", "provenance": "curated"}
        for i in range(25)
    ] + [
        {"id": f"synthetic-{i}", "question": f"Synthetic {i}", "provenance": "synthetic"}
        for i in range(15)
    ]

    truncated = _truncate_benchmarks(rows, 30)

    assert len(truncated) == 30
    assert sum(1 for row in truncated if row["provenance"] == "curated") == 25
    assert sum(1 for row in truncated if row["provenance"] == "synthetic") == 5


def test_synthetic_target_is_zero_when_existing_rows_fill_target() -> None:
    from genie_space_optimizer.optimization.evaluation import _compute_synthetic_target

    assert _compute_synthetic_target(target_count=30, curated_count=30, existing_count=0) == 0
    assert _compute_synthetic_target(target_count=30, curated_count=12, existing_count=8) == 10
    assert _compute_synthetic_target(target_count=30, curated_count=0, existing_count=0) == 30


def test_cap_keeps_evaluation_corpus_at_thirty() -> None:
    from genie_space_optimizer.optimization.evaluation import _truncate_benchmarks

    rows = [
        {"id": f"q{i:03d}", "question": f"Question {i}", "provenance": "synthetic"}
        for i in range(45)
    ]

    assert len(_truncate_benchmarks(rows, 30)) == 30
