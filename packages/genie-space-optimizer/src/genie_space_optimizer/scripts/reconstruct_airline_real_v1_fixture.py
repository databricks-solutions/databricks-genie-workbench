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
