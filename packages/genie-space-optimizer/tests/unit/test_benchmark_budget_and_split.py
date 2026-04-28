from __future__ import annotations

from genie_space_optimizer.optimization.benchmarks import assign_splits


def _rows(n: int) -> list[dict]:
    return [
        {
            "id": f"q{i:03d}",
            "question": f"Question {i}",
            "provenance": "curated" if i < 10 else "synthetic",
        }
        for i in range(n)
    ]


def test_assign_splits_is_random_across_curated_and_synthetic_rows() -> None:
    # Curated rows must no longer be pinned to train. With deterministic
    # random sampling, a seed that yields a mixed held_out proves the gate
    # is open to both provenances; pinning would always send the 10 curated
    # rows to train regardless of seed.
    rows = assign_splits(_rows(30), seed=0)

    held_out = [row for row in rows if row["split"] == "held_out"]
    train = [row for row in rows if row["split"] == "train"]

    assert len(train) == 25
    assert len(held_out) == 5
    assert any(row["provenance"] == "curated" for row in held_out)
    assert any(row["provenance"] == "synthetic" for row in held_out)


def test_assign_splits_is_deterministic_for_same_seed() -> None:
    first = assign_splits(_rows(30), seed=42)
    second = assign_splits(_rows(30), seed=42)

    first_held_out = [row["id"] for row in first if row["split"] == "held_out"]
    second_held_out = [row["id"] for row in second if row["split"] == "held_out"]

    assert first_held_out == second_held_out


def test_assign_splits_best_effort_for_small_corpus() -> None:
    rows = assign_splits(_rows(10), seed=42)

    assert sum(1 for row in rows if row["split"] == "held_out") >= 1
    assert sum(1 for row in rows if row["split"] == "train") >= 1


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
