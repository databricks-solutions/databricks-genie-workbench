"""Phase 1 (Trial 13) — real production row access tests.

Captures the production row shapes that Trial 12 missed:

* ``row["request"]["question"]`` (top-level question, not under
  ``request.kwargs.question``).
* ``row["{judge}/rationale"]`` per-judge rationales (no top-level
  ``judge_rationale``).
* ``row["{judge}/metadata"]`` (dict) and ``row["metadata/{judge}/{field}"]``
  (flat) ASI metadata.

Fixture: ``tests/unit/fixtures/production_eval_rows_real.json`` — seven
real rows pulled from Trial 12 / earlier postmortem evidence bundles.

These tests are intentionally red against the current
:mod:`eval_row_access` ladder (pre Phase 2); they go green together with
Phase 2's adapter extensions.
"""

from __future__ import annotations

import json
import pathlib
from collections.abc import Iterator

import pytest

from genie_space_optimizer.optimization.eval_row_access import (
    _collect_blame_set_from_asi,
    row_expected_sql,
    row_generated_sql,
    row_qid,
    row_question,
)

FIXTURE_PATH = (
    pathlib.Path(__file__).parent
    / "fixtures"
    / "production_eval_rows_real.json"
)


def _load_real_rows() -> list[tuple[str, str, dict]]:
    payload = json.loads(FIXTURE_PATH.read_text())
    rows: list[tuple[str, str, dict]] = []
    for bucket_key in (
        "production_rows_98ec8950",
        "production_rows_98ec8950_iter03",
        "production_rows_dc89d1a9",
    ):
        bucket = payload.get(bucket_key, [])
        for entry in bucket:
            rows.append(
                (
                    bucket_key,
                    str(entry["namespaced_qid"]),
                    dict(entry["row"]),
                )
            )
    assert len(rows) == 7, f"fixture must hold 7 real rows; got {len(rows)}"
    return rows


REAL_ROWS = _load_real_rows()


@pytest.mark.parametrize(
    "namespaced_qid,row",
    [(nsq, row) for _bucket, nsq, row in REAL_ROWS],
    ids=[nsq for _bucket, nsq, _row in REAL_ROWS],
)
def test_row_qid_returns_namespaced_qid_for_real_rows(
    namespaced_qid: str, row: dict
) -> None:
    """``row_qid`` MUST surface the canonical namespaced QID for the row.

    The QID lives at ``row["request"]["kwargs"]["question_id"]`` in
    production; this is the path already covered by Trial 12's
    consolidation through ``_qid_extraction``.
    """
    actual = row_qid(row)
    assert actual == namespaced_qid, (
        f"expected {namespaced_qid!r}, got {actual!r}"
    )


@pytest.mark.parametrize(
    "namespaced_qid,row",
    [(nsq, row) for _bucket, nsq, row in REAL_ROWS],
    ids=[nsq for _bucket, nsq, _row in REAL_ROWS],
)
def test_row_question_non_empty_for_real_rows(
    namespaced_qid: str, row: dict
) -> None:
    """``row_question`` MUST hydrate from ``row["request"]["question"]``.

    Red before Phase 2: today's ladder reads only
    ``inputs/question`` and ``kwargs.question``; production rows have
    the question at ``row["request"]["question"]`` (sibling to
    ``request.kwargs``), so this returns ``""``.
    """
    actual = row_question(row)
    assert actual.strip() != "", (
        f"row_question returned empty for {namespaced_qid}; "
        f"production rows carry question at row['request']['question']"
    )


@pytest.mark.parametrize(
    "namespaced_qid,row",
    [(nsq, row) for _bucket, nsq, row in REAL_ROWS],
    ids=[nsq for _bucket, nsq, _row in REAL_ROWS],
)
def test_row_expected_sql_non_empty_for_real_rows(
    namespaced_qid: str, row: dict
) -> None:
    """Sanity: ``row_expected_sql`` MUST find the ground-truth SQL.

    Production rows carry the ground truth at
    ``row["expected_response"]["value"]`` (already covered by the
    Trial 12 ladder via ``expected_response/value``) and at
    ``row["request"]["expected_sql"]`` for some shapes.
    """
    actual = row_expected_sql(row)
    assert actual.strip() != "", (
        f"row_expected_sql returned empty for {namespaced_qid}; "
        f"check expected_response/value and request.expected_sql"
    )


@pytest.mark.parametrize(
    "namespaced_qid,row",
    [(nsq, row) for _bucket, nsq, row in REAL_ROWS],
    ids=[nsq for _bucket, nsq, _row in REAL_ROWS],
)
def test_row_generated_sql_non_empty_for_real_rows(
    namespaced_qid: str, row: dict
) -> None:
    """Sanity: ``row_generated_sql`` MUST find the Genie response SQL.

    Production rows carry generated SQL at ``row["response"]["response"]``
    (already covered by the Trial 12 ladder via ``response.response``).
    """
    actual = row_generated_sql(row)
    assert actual.strip() != "", (
        f"row_generated_sql returned empty for {namespaced_qid}; "
        f"check response.response and outputs/response"
    )


@pytest.mark.parametrize(
    "namespaced_qid,row",
    [(nsq, row) for _bucket, nsq, row in REAL_ROWS],
    ids=[nsq for _bucket, nsq, _row in REAL_ROWS],
)
def test_collect_blame_set_from_asi_yields_at_least_one_for_real_rows(
    namespaced_qid: str, row: dict
) -> None:
    """``_collect_blame_set_from_asi`` MUST return ≥1 entry for the
    real rows.

    Red before Phase 2 for rows whose blame set lives in the flat-key
    surface ``metadata/<judge>/blame_set`` as a bracketed *string*
    rather than a list. Today's helper iterates ``<judge>/metadata``
    dicts and treats the bracketed string as a single opaque entry,
    which is semantically wrong; Phase 2 parses bracketed lists and
    also walks the flat-key surface.
    """
    blame = _collect_blame_set_from_asi(row)
    assert len(blame) >= 1, (
        f"_collect_blame_set_from_asi returned empty for {namespaced_qid}; "
        f"check metadata/<judge>/blame_set parsing and bracketed-list "
        f"normalization"
    )
    for entry in blame:
        assert "[" not in entry and "]" not in entry, (
            f"blame entry {entry!r} for {namespaced_qid} still carries "
            f"bracket characters; bracketed-string lists must be parsed"
        )


def test_fixture_has_seven_real_rows() -> None:
    """Sanity check: fixture must contain exactly 7 real production rows."""
    assert len(REAL_ROWS) == 7

    namespaced = {nsq for _bucket, nsq, _row in REAL_ROWS}
    expected = {
        "airline_ticketing_and_fare_analysis_gs_009",
        "airline_ticketing_and_fare_analysis_gs_024",
        "airline_ticketing_and_fare_analysis_gs_016",
        "7now_delivery_analytics_space_gs_001",
        "airline_ticketing_and_fare_analysis_gs_013",
        "7now_delivery_analytics_space_gs_021",
        "7now_delivery_analytics_space_gs_026",
    }
    assert namespaced == expected, (
        f"fixture QIDs drifted: extra={namespaced - expected}, "
        f"missing={expected - namespaced}"
    )


def _iter_collected_keys_in_row(row: dict) -> Iterator[str]:
    """Smoke helper used only by ``test_real_row_shape_assumptions``."""
    for key in row:
        if isinstance(key, str):
            yield key


def test_real_row_shape_assumptions_match_plan() -> None:
    """Verify the production-row shape claims in the Trial 13 plan.

    Pinning these as a test guards against future fixture-refresh
    operations silently breaking the Phase 2 ladder assumptions.
    """
    row = REAL_ROWS[0][2]
    request = row.get("request")
    assert isinstance(request, dict)
    assert request.get("question"), (
        "row['request']['question'] must be populated (Phase 2 target path)"
    )

    keys = set(_iter_collected_keys_in_row(row))
    assert "arbiter/rationale" in keys
    assert "metadata/arbiter/blame_set" in keys
    assert "metadata/arbiter/failure_type" in keys
    assert "arbiter/metadata" in keys

    arbiter_md = row.get("arbiter/metadata")
    assert isinstance(arbiter_md, dict)
    assert "blame_set" in arbiter_md


# ── Trial 14 — structured blame surfacing on a real production row ──


def test_real_row_accepts_structured_blame_injection() -> None:
    """Trial 14 — when a real production row is enriched with a typed
    ``blame_set_structured`` field on any judge, the readers MUST
    prefer the structured payload over the legacy free-text mirror.

    This is the local equivalent of the live re-run gate: the
    Trial 14 readers don't regress on the legacy path AND they pick
    up the new field as soon as judges emit it.
    """
    import copy
    import json

    from genie_space_optimizer.optimization.eval_row_access import (
        _collect_blame_entries_from_asi,
        _collect_blame_set_from_asi,
    )

    row = copy.deepcopy(REAL_ROWS[0][2])

    # Inject a clean structured payload on the schema_accuracy judge's
    # nested ``<judge>/metadata`` surface and a matching JSON-encoded
    # flat key. The structured payload mixes a schema-resolvable
    # column entry with a non-schema filter entry — the production
    # shape the Trial 14 prompts are designed to produce.
    structured = [
        {"kind": "column", "ref": "main.airline.fact_flights.dest_airport_cd"},
        {"kind": "filter", "ref": None, "description": "year = 2023"},
    ]
    schema_md = row.setdefault("schema_accuracy/metadata", {})
    if not isinstance(schema_md, dict):
        schema_md = {}
        row["schema_accuracy/metadata"] = schema_md
    schema_md["blame_set_structured"] = structured
    row["metadata/schema_accuracy/blame_set_structured"] = json.dumps(structured)

    entries = _collect_blame_entries_from_asi(row)
    assert [e.kind for e in entries] == ["column", "filter"]
    # The seed list MUST come from the structured ref, not the
    # legacy bracketed string still sitting on the row.
    seeds = _collect_blame_set_from_asi(row)
    assert "main.airline.fact_flights.dest_airport_cd" in seeds
