"""Phase 1 / Track 6 — canonical row-shape adapter coverage.

For each of the five hydration fixture rows (one per documented shape
variant), assert that :mod:`eval_row_access` returns non-empty values for
the canonical accessors and that :func:`iter_asi_metadata` yields at
least one judge whose metadata covers every ``ASI_SURFACE_KEYS`` entry.

This pins the foundation for the Stage 1 hydration wire-in (Phase 2).
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from genie_space_optimizer.optimization.eval_row_access import (
    ASI_SURFACE_KEYS,
    iter_asi_metadata,
    row_expected_sql,
    row_generated_sql,
    row_qid,
    row_question,
)

FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "production_eval_rows.json"
)


@pytest.fixture(scope="module")
def hydration_rows() -> list[dict]:
    with FIXTURE_PATH.open() as f:
        data = json.load(f)
    return [dict(r) for r in data["hydration_rows"]]


def test_fixture_covers_five_documented_shape_variants(
    hydration_rows: list[dict],
) -> None:
    shapes = {r["_shape"] for r in hydration_rows}
    assert shapes == {
        "top_level",
        "mlflow_slash_flattened",
        "dotted_flat_keys",
        "nested_inputs_dict",
        "request_kwargs_json_string",
    }


@pytest.mark.parametrize(
    "expected_qid",
    ["gs_009", "gs_021", "gs_024", "gs_026", "gs_004"],
)
def test_row_qid_resolves_canonical_qid_across_shapes(
    hydration_rows: list[dict], expected_qid: str
) -> None:
    row = next(r for r in hydration_rows if r["_expected_qid"] == expected_qid)
    assert row_qid(row) == expected_qid


@pytest.mark.parametrize(
    "expected_qid",
    ["gs_009", "gs_021", "gs_024", "gs_026", "gs_004"],
)
def test_row_question_non_empty_across_shapes(
    hydration_rows: list[dict], expected_qid: str
) -> None:
    row = next(r for r in hydration_rows if r["_expected_qid"] == expected_qid)
    assert row_question(row).strip() != ""


@pytest.mark.parametrize(
    "expected_qid",
    ["gs_009", "gs_021", "gs_024", "gs_026", "gs_004"],
)
def test_row_expected_sql_non_empty_across_shapes(
    hydration_rows: list[dict], expected_qid: str
) -> None:
    row = next(r for r in hydration_rows if r["_expected_qid"] == expected_qid)
    assert row_expected_sql(row).strip() != ""


@pytest.mark.parametrize(
    "expected_qid",
    ["gs_009", "gs_021", "gs_024", "gs_026", "gs_004"],
)
def test_row_generated_sql_non_empty_across_shapes(
    hydration_rows: list[dict], expected_qid: str
) -> None:
    row = next(r for r in hydration_rows if r["_expected_qid"] == expected_qid)
    assert row_generated_sql(row).strip() != ""


@pytest.mark.parametrize(
    "expected_qid",
    ["gs_009", "gs_021", "gs_024", "gs_026", "gs_004"],
)
def test_iter_asi_metadata_yields_judge_with_all_surface_keys(
    hydration_rows: list[dict], expected_qid: str
) -> None:
    row = next(r for r in hydration_rows if r["_expected_qid"] == expected_qid)
    judges = list(iter_asi_metadata(row))
    assert judges, f"no ASI metadata judges yielded for {expected_qid}"
    has_full_judge = any(
        all(metadata.get(key) for key in ASI_SURFACE_KEYS)
        for _, metadata in judges
    )
    assert has_full_judge, (
        f"no judge for {expected_qid} populates all ASI_SURFACE_KEYS; "
        f"keys missing per judge: "
        + ", ".join(
            f"{judge}=[{', '.join(k for k in ASI_SURFACE_KEYS if not metadata.get(k))}]"
            for judge, metadata in judges
        )
    )
