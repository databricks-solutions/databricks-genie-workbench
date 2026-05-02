"""One-shot reconstruction of the airline_real_v1 replay fixture from cycle 7.

Cycle 7's ``_baseline_row_qid`` accepted MLflow ``client_request_id`` (a
trace ID like ``tr-f74a86401aa0b8e292f602e0069d867d``) as a benchmark
question ID. The captured fixture has structurally complete ``eval_rows``,
but the ``question_id`` values are trace IDs — useless for replay because
trace IDs are minted fresh on every run.

This script substitutes each trace ID with its canonical benchmark qid
(e.g., ``airline_ticketing_and_fare_analysis_gs_024``) using two existing
data sources we already paid for:

1. **MLflow traces** — every predict_fn invocation tagged the trace with
   ``tags["question_id"] = canonical_qid``. ``mlflow.search_traces``
   returns a DataFrame from which we can build ``{trace_id: canonical_qid}``
   per iteration. See ``evaluation.py:_qid_trace_map_from_search_traces_df``.

2. **Delta table fallback** — ``genie_opt_iterations.rows_json`` persists
   per-question detail rows that include canonical qids in known shapes.
   Used only when MLflow strategies (1-3 in evaluation.py) all return empty.

Run inside a Databricks notebook attached to the workspace where cycle 7
ran. See
``docs/2026-05-02-track-a-fixture-reconstruction-and-qid-extractor-fix-plan.md``
for the full procedure.
"""
from __future__ import annotations

import json
import pathlib
from typing import Any


def substitute_trace_ids_with_canonical_qids(
    raw_iter: dict[str, Any],
    trace_to_canonical: dict[str, str],
) -> dict[str, Any]:
    """Return a copy of ``raw_iter`` with eval_rows[*].question_id rewritten.

    Args:
        raw_iter: One iteration dict from the cycle 7 raw fixture.
        trace_to_canonical: Map from trace ID (``tr-...``) to canonical
            benchmark qid (``airline_..._gs_NNN``). May be empty.

    Returns:
        A new iteration dict (does not mutate input). Non-eval_rows fields
        pass through unchanged. eval_rows are rebuilt: rows whose
        ``question_id`` already matches a canonical pattern (no ``tr-``
        prefix) pass through; rows with ``tr-`` prefix get substituted.

    Raises:
        KeyError: when an eval_row has a ``tr-`` prefixed question_id that
            is not in ``trace_to_canonical``. Hard failure — silently
            dropping rows would re-introduce the cycle 1-3 empty-fixture
            symptom without operator visibility.
    """
    new_eval_rows: list[dict[str, Any]] = []
    for row in raw_iter.get("eval_rows") or []:
        qid = row.get("question_id", "")
        if isinstance(qid, str) and qid.startswith("tr-"):
            if qid not in trace_to_canonical:
                raise KeyError(
                    f"trace_id {qid!r} (iteration {raw_iter.get('iteration')}) "
                    f"missing from trace_to_canonical map; cannot substitute"
                )
            canonical = trace_to_canonical[qid]
            new_eval_rows.append({**row, "question_id": canonical})
        else:
            new_eval_rows.append(dict(row))
    return {**raw_iter, "eval_rows": new_eval_rows}


def reconstruct_fixture(
    raw_fixture: dict[str, Any],
    trace_maps_by_iter: dict[int, dict[str, str]],
) -> dict[str, Any]:
    """Apply per-iteration trace_id->canonical_qid maps to a raw fixture.

    Args:
        raw_fixture: The cycle 7 fixture as parsed from JSON. Must have
            ``fixture_id`` and ``iterations`` keys.
        trace_maps_by_iter: ``{iteration_int: {trace_id: canonical_qid}}``.
            Must contain an entry for every iteration in ``raw_fixture``.

    Returns:
        A new fixture dict (does not mutate input) with eval_rows rewritten.

    Raises:
        KeyError: when an iteration in raw_fixture has no entry in
            trace_maps_by_iter, OR when any eval_row's trace ID isn't in
            its iteration's map.
    """
    new_iterations: list[dict[str, Any]] = []
    for raw_iter in raw_fixture.get("iterations") or []:
        iter_num = raw_iter.get("iteration")
        if iter_num not in trace_maps_by_iter:
            raise KeyError(
                f"iteration {iter_num} not in trace_maps_by_iter "
                f"(have iterations: {sorted(trace_maps_by_iter.keys())})"
            )
        new_iterations.append(
            substitute_trace_ids_with_canonical_qids(
                raw_iter, trace_maps_by_iter[iter_num]
            )
        )
    return {**raw_fixture, "iterations": new_iterations}


def load_fixture(path: pathlib.Path | str) -> dict[str, Any]:
    """Read a fixture JSON from disk."""
    return json.loads(pathlib.Path(path).read_text())


def save_fixture(fixture: dict[str, Any], path: pathlib.Path | str) -> None:
    """Write a fixture JSON to disk in compact form (matches stderr emission)."""
    pathlib.Path(path).write_text(json.dumps(fixture, separators=(",", ":")))


def fetch_trace_map_for_iteration(
    *,
    experiment_id: str,
    optimization_run_id: str,
    iteration: int,
    expected_count: int = 24,
) -> dict[str, str]:
    """Build ``{trace_id: canonical_qid}`` for one iteration via MLflow tags.

    Mirrors ``evaluation.py:_recover_trace_map_via_tags`` but inverts the
    map (the production helper returns ``{qid: trace_id}``; we want the
    other direction so we can substitute trace IDs in eval_rows).

    The lever-loop tags every predict_fn span with:
      - ``genie.optimization_run_id`` = the GSO run UUID
      - ``genie.iteration`` = the iteration number (string)
      - ``question_id`` = the canonical benchmark qid

    Args:
        experiment_id: MLflow experiment that the lever-loop wrote to. From
            ``$MLFLOW_EXPERIMENT_ID`` in the deploy `.env`.
        optimization_run_id: GSO run UUID (e.g. cycle 7 was
            ``78557321-4e43-4bc6-9b4c-906771bd2f8d``). Find via the
            fixture's ``fixture_id`` field, which has format
            ``airline_real_v1_run_<run_id>``.
        iteration: 1-indexed iteration number.
        expected_count: How many traces we expect (24 for the airline
            corpus). Used as a sanity check, not a filter.

    Returns:
        ``{trace_id: canonical_qid}``. Empty if the search returns no rows
        (caller should fall back to Delta).
    """
    import mlflow  # type: ignore[import-not-found]

    filter_string = (
        f"tags.`genie.optimization_run_id` = '{optimization_run_id}' "
        f"AND tags.`genie.iteration` = '{iteration}'"
    )
    traces_df = mlflow.search_traces(
        locations=[experiment_id],
        filter_string=filter_string,
        max_results=max(500, expected_count * 2),
    )
    inverted: dict[str, str] = {}
    if traces_df is None or len(traces_df) == 0:
        return inverted
    for _, row in traces_df.iterrows():
        tid = row.get("trace_id")
        tags = row.get("tags") or {}
        qid = tags.get("question_id", "") if isinstance(tags, dict) else ""
        if tid and qid:
            inverted[str(tid)] = str(qid)
    return inverted


def fetch_trace_map_for_iteration_via_delta(
    *,
    spark: Any,
    catalog: str,
    schema: str,
    optimization_run_id: str,
    iteration: int,
) -> dict[str, str]:
    """Fallback: build ``{trace_id: canonical_qid}`` from Delta iteration rows.

    Reads ``<catalog>.<schema>.genie_opt_iterations.rows_json`` for the
    given run + iteration and parses the JSON list. Each row in the
    persisted JSON has both a canonical ``question_id`` field and (in
    most shapes) a ``client_request_id`` / ``request_id`` / ``trace_id``
    that matches the eval_rows entry in the raw fixture.

    Args:
        spark: A SparkSession (notebook's `spark` global, or a Databricks
            Connect session).
        catalog: UC catalog where GSO tables live (``$GSO_CATALOG``).
        schema: UC schema (``$GSO_SCHEMA``).
        optimization_run_id: GSO run UUID (must match Delta
            ``run_id`` partition).
        iteration: 1-indexed iteration number.

    Returns:
        ``{trace_id: canonical_qid}``. Empty if the row has no parseable
        rows_json or no rows carry both fields.
    """
    df = spark.sql(
        f"""
        SELECT rows_json
        FROM {catalog}.{schema}.genie_opt_iterations
        WHERE run_id = '{optimization_run_id}'
          AND iteration = {int(iteration)}
        ORDER BY timestamp DESC
        LIMIT 1
        """
    )
    rows = df.collect()
    if not rows:
        return {}
    rows_json_str = rows[0]["rows_json"] or ""
    if not rows_json_str:
        return {}
    try:
        rows_payload = json.loads(rows_json_str)
    except (TypeError, ValueError):
        return {}
    if not isinstance(rows_payload, list):
        return {}
    inverted: dict[str, str] = {}
    for row in rows_payload:
        if not isinstance(row, dict):
            continue
        canonical = row.get("question_id") or row.get("id")
        if not canonical and isinstance(row.get("inputs"), dict):
            canonical = row["inputs"].get("question_id")
        trace_id = (
            row.get("trace_id")
            or row.get("client_request_id")
            or row.get("request_id")
        )
        if canonical and trace_id:
            inverted[str(trace_id)] = str(canonical)
    return inverted


def assert_canonical_overlap(fixture: dict[str, Any]) -> None:
    """Validate that every iteration's eval_qids and cluster_qids share a namespace.

    Two checks:
      1. No eval_row has a ``tr-`` prefixed qid.
      2. Every cluster qid (hard + soft) is present in that iteration's
         eval_rows. (Strict subset, not equal — eval_rows is the full
         24-question corpus; clusters select a few that need fixing.)

    Raises AssertionError on the first violation.
    """
    for it in fixture.get("iterations") or []:
        eval_qids = {r.get("question_id") for r in (it.get("eval_rows") or [])}
        trace_id_prefixed = {q for q in eval_qids if isinstance(q, str) and q.startswith("tr-")}
        if trace_id_prefixed:
            raise AssertionError(
                f"iter {it.get('iteration')}: {len(trace_id_prefixed)} eval_rows "
                f"have trace-id-prefixed qids (e.g. {next(iter(trace_id_prefixed))})"
            )
        cluster_qids: set[str] = set()
        for c in (it.get("clusters") or []):
            cluster_qids.update(c.get("question_ids") or [])
        for c in (it.get("soft_clusters") or []):
            cluster_qids.update(c.get("question_ids") or [])
        missing = cluster_qids - eval_qids
        if missing:
            raise AssertionError(
                f"iter {it.get('iteration')}: cluster qids not present in eval_rows: "
                f"{sorted(missing)}"
            )


def main(
    *,
    raw_fixture_path: str,
    out_fixture_path: str,
    experiment_id: str,
    optimization_run_id: str,
    catalog: str,
    schema: str,
    spark: Any | None = None,
) -> None:
    """End-to-end reconstruction. Run inside a Databricks notebook.

    Step 1. Load raw fixture.
    Step 2. For each iteration, build trace_id->canonical_qid via MLflow
            (primary) or Delta (fallback).
    Step 3. Apply substitution.
    Step 4. Assert canonical overlap.
    Step 5. Save the corrected fixture.

    Hard-fails (raises) at any step that does not produce expected data —
    we explicitly do not silently emit a partially-correct fixture.
    """
    print(f"[reconstruct] loading raw fixture from {raw_fixture_path}")
    raw = load_fixture(raw_fixture_path)
    iterations = raw.get("iterations") or []
    print(f"[reconstruct] found {len(iterations)} iterations in raw fixture")

    trace_maps_by_iter: dict[int, dict[str, str]] = {}
    for it in iterations:
        iter_num = int(it["iteration"])
        print(f"[reconstruct] iter {iter_num}: fetching trace map via MLflow tags")
        tmap = fetch_trace_map_for_iteration(
            experiment_id=experiment_id,
            optimization_run_id=optimization_run_id,
            iteration=iter_num,
        )
        if not tmap and spark is not None:
            print(f"[reconstruct] iter {iter_num}: MLflow returned 0 traces, falling back to Delta")
            tmap = fetch_trace_map_for_iteration_via_delta(
                spark=spark,
                catalog=catalog,
                schema=schema,
                optimization_run_id=optimization_run_id,
                iteration=iter_num,
            )
        if not tmap:
            raise RuntimeError(
                f"iter {iter_num}: both MLflow and Delta returned empty trace maps; "
                f"check experiment_id={experiment_id!r}, run_id={optimization_run_id!r}"
            )
        print(f"[reconstruct] iter {iter_num}: recovered {len(tmap)} trace_id->qid pairs")
        trace_maps_by_iter[iter_num] = tmap

    print("[reconstruct] applying substitution across all iterations")
    corrected = reconstruct_fixture(raw, trace_maps_by_iter)

    print("[reconstruct] running canonical-overlap assertions")
    assert_canonical_overlap(corrected)

    print(f"[reconstruct] writing corrected fixture to {out_fixture_path}")
    save_fixture(corrected, out_fixture_path)
    print("[reconstruct] DONE")
