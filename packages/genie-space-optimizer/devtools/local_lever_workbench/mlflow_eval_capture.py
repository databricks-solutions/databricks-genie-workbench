"""MLflow → workbench eval-row capture (PR-1).

For a given (experiment, source-task-run) pair, fetch every
``genie_predict_fn`` trace plus its assessments and translate them into
production-shaped eval-row dicts that the existing workbench loaders
(``input_bundle.from_run_analysis_dir``) already understand.

The output is a single JSON file shaped::

    {
      "_schema_version": "workbench_eval_capture_v1",
      "_provenance": {
        "experiment_id": "...",
        "experiment_name": "...",
        "source_job_run_id": "...",
        "source_task_run_id": "...",
        "task_key": "...",
        "captured_at_utc": "...",
        "trace_count": N
      },
      "eval_rows": [<row>, <row>, ...]
    }

Written to the location ``from_run_analysis_dir`` already searches::

    docs/runid_analysis/<optimization_run_id>/evidence/
        replay_fixture_from_latest_export_<task_run_id>.json

so no other workbench code needs to change to consume the fixture.

Why one task run id at a time? Each production task ("enrichment",
"lever_loop[/attempt_N]") emits its own ``genie_predict_fn`` trace
batch, and the workbench wants exactly one "eval pass" worth of rows.
Mixing batches would force the SM to dedupe across iterations, which
is not what this entrypoint is for.

Production traces are filtered client-side on
``trace_metadata["mlflow.source.name"]`` because MLflow's filter DSL
does not accept that key as a server-side filter (returns 400).
"""
from __future__ import annotations

import datetime as _dt
import json
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


# ── Trace → row translation ───────────────────────────────────────────


def _parse_json_blob(value: Any) -> Any:
    """Decode an MLflow string-encoded JSON blob, tolerantly.

    Span attributes round-trip through JSON-stringification (a
    ``"question_id"`` attribute is stored as the literal string
    ``"\\"airline_..._gs_029\\""``). Bare values come back as the raw
    string. We attempt one ``json.loads`` and fall back to the value
    unchanged.
    """
    if value is None:
        return None
    if isinstance(value, (dict, list, int, float, bool)):
        return value
    if not isinstance(value, str):
        return value
    text = value.strip()
    if not text:
        return ""
    try:
        return json.loads(text)
    except (ValueError, TypeError):
        return value


def _get_span_attr(span: Mapping[str, Any], key: str) -> Any:
    attrs = span.get("attributes") or {}
    if not isinstance(attrs, Mapping):
        return None
    return attrs.get(key)


def _root_span(trace: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Return the ``genie_predict_fn`` root span if present.

    Falls back to the first span when the name is missing — defensive
    only; production always names the root span.
    """
    data = trace.get("data") or {}
    spans = data.get("spans") if isinstance(data, Mapping) else None
    if not isinstance(spans, list) or not spans:
        return None
    for span in spans:
        if isinstance(span, Mapping) and span.get("name") == "genie_predict_fn":
            return span
    first = spans[0]
    return first if isinstance(first, Mapping) else None


def _flatten_assessment_metadata(
    row: dict[str, Any],
    assessment_name: str,
    metadata: Mapping[str, Any],
) -> None:
    """Mirror MLflow's flat ``metadata/<judge>/<field>`` key shape.

    Production eval rows carry both ``<judge>/metadata`` (nested) and
    ``metadata/<judge>/<field>`` (flat) for every judge metadata
    field; the Stage-1 evidence-card reader walks BOTH paths to handle
    fixture drift, so we emit both for fidelity.

    Trial 14 — ``blame_set_structured`` and ``blame_rationale`` are
    new flat fields. ``blame_set_structured`` is a ``list[dict]``;
    naive ``str(value)`` would produce a Python-repr blob the
    structured reader cannot round-trip. JSON-encode it so the
    Trial 14 ``_collect_blame_entries_from_asi`` reader (which calls
    ``coerce_blame_entries`` with a JSON-string path) parses it back
    cleanly. ``blame_rationale`` is a plain string and falls through
    the default ``str(value)`` path.
    """
    if not isinstance(metadata, Mapping):
        return
    for field_name, field_value in metadata.items():
        if not isinstance(field_name, str):
            continue
        if field_name.startswith("mlflow."):
            continue
        flat_key = f"metadata/{assessment_name}/{field_name}"
        # Production stores all flat-key values as strings.
        # Trial 14 — preserve list/dict fidelity for the structured
        # blame field via JSON encoding; everything else stringifies.
        if isinstance(field_value, str):
            serialized: str = field_value
        elif field_name == "blame_set_structured" and isinstance(
            field_value, (list, tuple, dict)
        ):
            try:
                serialized = json.dumps(field_value, ensure_ascii=False)
            except (TypeError, ValueError):
                serialized = str(field_value)
        else:
            serialized = str(field_value)
        row[flat_key] = serialized


def _ingest_assessment(row: dict[str, Any], assessment: Mapping[str, Any]) -> None:
    name = assessment.get("assessment_name")
    if not isinstance(name, str) or not name:
        return
    metadata = assessment.get("metadata") or {}
    if not isinstance(metadata, Mapping):
        metadata = {}

    # ``feedback`` (LLM_JUDGE/CODE outputs) and ``expectation`` (HUMAN
    # ground-truth) are mutually exclusive in practice; we coalesce
    # into the production ``<name>/value`` key the same way.
    feedback = assessment.get("feedback") or {}
    expectation = assessment.get("expectation") or {}
    value: Any = None
    if isinstance(feedback, Mapping) and "value" in feedback:
        value = feedback.get("value")
    elif isinstance(expectation, Mapping) and "value" in expectation:
        value = expectation.get("value")
    if value is not None:
        row[f"{name}/value"] = value

    rationale = assessment.get("rationale")
    if isinstance(rationale, str) and rationale.strip():
        row[f"{name}/rationale"] = rationale

    # Always populate the nested-dict shape so eval_row_access.iter_asi_metadata
    # (which keys on '<judge>/metadata') can walk it.
    if metadata:
        row[f"{name}/metadata"] = dict(metadata)
        _flatten_assessment_metadata(row, name, metadata)


def trace_to_eval_row(trace: Mapping[str, Any]) -> dict[str, Any] | None:
    """Translate one MLflow trace into a production-shaped eval row.

    Returns ``None`` when the trace does not carry the structure the
    workbench expects (no root span, no question_id). The caller
    should skip and log; we never raise on an individual trace because
    a single corrupt trace must not abort a multi-QID capture.
    """
    info = trace.get("info") or {}
    if not isinstance(info, Mapping):
        return None
    root = _root_span(trace)
    if root is None:
        return None

    qid_raw = _parse_json_blob(_get_span_attr(root, "question_id"))
    qid = str(qid_raw or "").strip()
    if not qid:
        return None

    span_inputs = _parse_json_blob(_get_span_attr(root, "mlflow.spanInputs"))
    span_outputs = _parse_json_blob(_get_span_attr(root, "mlflow.spanOutputs"))

    # Production rows store request/response as JSON-encoded strings.
    # Mirror that exactly so ``eval_row_access`` paths
    # (``request.question``, ``request.kwargs.expected_sql``) resolve.
    request_payload: dict[str, Any] = {}
    if isinstance(span_inputs, Mapping):
        if "question" in span_inputs:
            request_payload["question"] = span_inputs.get("question")
        if "expected_sql" in span_inputs:
            request_payload["expected_sql"] = span_inputs.get("expected_sql")
        request_payload["kwargs"] = {
            "question_id": qid,
            **(
                {"expected_sql": span_inputs["expected_sql"]}
                if "expected_sql" in span_inputs
                else {}
            ),
        }
    else:
        request_payload["kwargs"] = {"question_id": qid}

    response_payload: Any = span_outputs if span_outputs is not None else {}

    row: dict[str, Any] = {
        "request": json.dumps(request_payload, ensure_ascii=False),
        "response": (
            json.dumps(response_payload, ensure_ascii=False)
            if not isinstance(response_payload, str)
            else response_payload
        ),
        "client_request_id": info.get("client_request_id") or "",
        "state": info.get("state") or "",
        "request_time": info.get("request_time") or "",
        "execution_duration": info.get("execution_duration_ms") or 0,
        # _asi_source is the row-level QID fallback the canonical
        # extractor reads when ``request.kwargs.question_id`` is
        # missing. Production rows always set it; mirror that so
        # downstream extraction has the same path priority.
        "_asi_source": qid,
        # Trace identity for postmortem cross-reference.
        "trace_id": info.get("trace_id") or "",
    }

    assessments = info.get("assessments") or []
    if isinstance(assessments, list):
        for assessment in assessments:
            if isinstance(assessment, Mapping):
                _ingest_assessment(row, assessment)

    return row


# ── MLflow trace fetching ─────────────────────────────────────────────


@dataclass(frozen=True)
class CaptureSpec:
    """Parameters identifying a single production eval batch to capture."""

    experiment_id: str
    experiment_name: str
    optimization_run_id: str
    job_id: str
    task_run_id: str
    task_key: str
    # Inclusive lower / exclusive upper bound (epoch ms) for the
    # MLflow trace search. Set from the task's actual execution window
    # so we don't scan years of traces when the experiment is busy.
    task_start_ms: int = 0
    task_end_ms: int = 0
    # Trial 13j — the Genie Space the eval rows were generated against.
    # Used at capture time to fetch the serialized_space via the Genie
    # API and populate schema_columns into the v2 bundle payload. Empty
    # string means the resolver could not infer it from the job-run
    # parameters; the capture proceeds without schema_columns injection
    # and the loader falls back to the Trial 13i derivation chain.
    genie_space_id: str = ""


def _resolve_capture_spec(
    *,
    job_run_id: str,
    task_key: str,
    profile: str | None,
) -> CaptureSpec:
    """Look up the experiment + task-run details for a production job run.

    Uses the Databricks SDK to fetch the parent job-run payload, picks
    the latest attempt of the requested task_key, and resolves the
    experiment id via MLflow. Optimisation_run_id is read from the
    job-run parameters (``optimization_run_id`` value).
    """
    from databricks.sdk import WorkspaceClient

    if profile:
        w = WorkspaceClient(profile=profile)
    else:
        w = WorkspaceClient()
    run = w.jobs.get_run(run_id=int(job_run_id))
    if run is None or run.tasks is None:
        raise RuntimeError(f"job run {job_run_id} not found or has no tasks")

    job_id = str(run.job_id) if run.job_id is not None else ""

    # Resolve parameters off the parent run — the task notebook reads
    # ``experiment_name`` from there via dbutils.widgets.
    params: dict[str, str] = {}
    overriding = getattr(run, "overriding_parameters", None) or []
    job_params = getattr(run, "job_parameters", None) or []
    for param_list in (overriding, job_params):
        for param in param_list:
            name = getattr(param, "name", None)
            value = getattr(param, "value", None) or getattr(param, "default", None)
            if name and value:
                params.setdefault(str(name), str(value))
    experiment_name = params.get("experiment_name", "")
    # Production stores the optimization run id under the parameter
    # name ``run_id``; the ``optimization_run_id`` alias is the
    # MLflow tag set later, not the job parameter name.
    optimization_run_id = params.get("optimization_run_id") or params.get("run_id", "")
    # Trial 13j — the Genie Space id is the input to ``fetch_space_config``
    # at capture time. Production stores it under one of these parameter
    # names; tolerate absence so a missing param does not block capture.
    genie_space_id = (
        params.get("space_id")
        or params.get("genie_space_id")
        or params.get("genie_room_id")
        or ""
    )
    if not experiment_name:
        raise RuntimeError(
            f"job run {job_run_id} carries no 'experiment_name' parameter; "
            "cannot resolve MLflow experiment"
        )

    # Pick the latest successful attempt of the requested task. We allow
    # FAILED attempts as a fallback when a task only ever failed.
    matching = [t for t in run.tasks if t.task_key == task_key]
    if not matching:
        raise RuntimeError(
            f"task_key={task_key!r} not present in job run {job_run_id}; "
            f"available={sorted({t.task_key for t in run.tasks if t.task_key})}"
        )
    matching.sort(key=lambda t: t.attempt_number or 0, reverse=True)
    chosen = next(
        (
            t
            for t in matching
            if t.state and t.state.result_state and t.state.result_state.value == "SUCCESS"
        ),
        matching[0],
    )
    task_run_id = str(chosen.run_id) if chosen.run_id is not None else ""
    if not task_run_id:
        raise RuntimeError(
            f"task {task_key!r} on run {job_run_id} has no run_id; "
            "cannot scope traces"
        )
    task_start_ms = int(getattr(chosen, "start_time", 0) or 0)
    task_end_ms = int(getattr(chosen, "end_time", 0) or 0)

    import mlflow

    mlflow.set_tracking_uri("databricks")
    exp = mlflow.get_experiment_by_name(experiment_name)
    if exp is None:
        raise RuntimeError(
            f"MLflow experiment {experiment_name!r} not found on the "
            f"current tracking server"
        )

    return CaptureSpec(
        experiment_id=str(exp.experiment_id),
        experiment_name=experiment_name,
        optimization_run_id=optimization_run_id,
        job_id=job_id,
        task_run_id=task_run_id,
        task_key=task_key,
        task_start_ms=task_start_ms,
        task_end_ms=task_end_ms,
        genie_space_id=str(genie_space_id),
    )


def _source_name(job_id: str, task_run_id: str) -> str:
    return f"jobs/{job_id}/run/{task_run_id}"


# ── Trial 13l — Genie Space schema_columns fetch (production-owned) ───
#
# These helpers originally lived here (Trial 13j) but Trial 13l promoted
# them into ``genie_space_optimizer.optimization.schema_columns`` so the
# production lever-loop harness can call them too. Workbench keeps a thin
# re-export so existing callers, ``__all__``, and the
# ``tests/workbench/test_capture_schema_columns_fetch.py`` wiring test
# stay valid.
#
# Two callers, one fetch implementation:
#
# * Production hot path (``harness.py``, per iteration) uses the
#   high-level injector ``inject_schema_columns_into_metadata_snapshot``
#   which mutates ``metadata_snapshot["schema_columns"]`` in place and
#   emits a ``GSO_PLAN11_SCHEMA_COLUMNS_INJECTION_V1`` marker. It does
#   not need the raw ``serialized_space`` payload because the SM and
#   Stage 1 transformers only consume the FQN list.
# * Workbench offline capture (this module) uses the lower-level
#   ``_fetch_schema_columns_for_space`` helper because it needs the
#   ``serialized_space`` dict to persist into the
#   ``workbench_eval_capture_v2`` JSON bundle (so the loader's typed-
#   evidence-union derivation has access to the parsed Genie config).
#   The two callers share the underlying fetch + extract logic;
#   they only differ in how they consume the result.
from genie_space_optimizer.optimization.schema_columns import (
    _extract_fqn_columns,
    _fetch_schema_columns_for_space,
)


def _fetch_predict_traces(
    experiment_id: str,
    source_name: str,
    *,
    task_start_ms: int = 0,
    task_end_ms: int = 0,
    page_size: int = 500,
    max_pages: int = 20,
) -> list[Mapping[str, Any]]:
    """Return all ``genie_predict_fn`` traces matching ``source_name``.

    MLflow's trace search does not accept ``metadata.mlflow.source.name``
    as a server-side filter (returns 0 in our tests), so we page through
    traces in the experiment and filter client-side. To avoid scanning
    the entire (multi-thousand) trace history of a hot experiment, we
    pass a ``timestamp_ms`` window derived from the task's actual
    execution start/end (with small slack to cover clock skew). This
    keeps a single capture bounded to the traces the task emitted.

    ``search_traces`` already returns full ``Trace`` objects (spans +
    assessments) so we DO NOT call ``get_trace`` per-id — doing so
    saturates the connection pool when an experiment is busy.
    """
    from mlflow import MlflowClient

    client = MlflowClient()
    filter_parts: list[str] = []
    if task_start_ms:
        # 60 s pre-roll covers MLflow's request_time = "span open ms"
        # whereas job task start = notebook launch.
        filter_parts.append(f"timestamp_ms > {max(0, task_start_ms - 60_000)}")
    if task_end_ms:
        # 5 minute post-roll covers slow trace flush at task end.
        filter_parts.append(f"timestamp_ms < {task_end_ms + 300_000}")
    filter_string = " AND ".join(filter_parts) or None

    matches: list[Mapping[str, Any]] = []
    token: str | None = None
    pages = 0
    seen_trace_ids: set[str] = set()
    while pages < max_pages:
        result = client.search_traces(
            experiment_ids=[experiment_id],
            filter_string=filter_string,
            max_results=page_size,
            page_token=token,
        )
        if not result:
            break
        for trace in result:
            info = getattr(trace, "info", None)
            if info is None:
                continue
            md = getattr(info, "trace_metadata", None) or {}
            tags = getattr(info, "tags", None) or {}
            if md.get("mlflow.source.name") != source_name:
                continue
            if tags.get("mlflow.traceName") != "genie_predict_fn":
                continue
            trace_id = getattr(info, "trace_id", None) or getattr(
                info, "request_id", None
            )
            if not trace_id or trace_id in seen_trace_ids:
                continue
            seen_trace_ids.add(str(trace_id))
            matches.append(_trace_to_dict(trace))
        token = getattr(result, "token", None)
        pages += 1
        if not token:
            break
    return matches


def _trace_to_dict(trace: Any) -> dict[str, Any]:
    """Normalize an mlflow.entities.Trace into a plain JSON-able dict."""
    if isinstance(trace, Mapping):
        return dict(trace)
    info_obj = getattr(trace, "info", None)
    data_obj = getattr(trace, "data", None)
    if info_obj is None and data_obj is None:
        return {}

    def _to_jsonable(value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, Mapping):
            return {str(k): _to_jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [_to_jsonable(v) for v in value]
        # Pydantic / dataclass fallback
        if hasattr(value, "to_dict"):
            try:
                return _to_jsonable(value.to_dict())
            except Exception:
                pass
        if hasattr(value, "model_dump"):
            try:
                return _to_jsonable(value.model_dump())
            except Exception:
                pass
        if hasattr(value, "__dict__"):
            return _to_jsonable({k: v for k, v in vars(value).items() if not k.startswith("_")})
        return str(value)

    return {
        "info": _to_jsonable(info_obj) if info_obj is not None else {},
        "data": _to_jsonable(data_obj) if data_obj is not None else {},
    }


# ── Public entrypoints ────────────────────────────────────────────────


def capture_eval_rows(
    *,
    job_run_id: str,
    task_key: str = "enrichment",
    profile: str | None = None,
) -> tuple[CaptureSpec, list[dict[str, Any]], tuple[str, ...], dict, str]:
    """Resolve a ``CaptureSpec`` and return rows + schema_columns payload.

    Pure with respect to the local filesystem; the caller decides where
    to persist the output. Splits resolution from persistence so the
    same code path can be exercised in unit tests with a stubbed SDK.

    Trial 13j — also fetches the Genie Space's ``serialized_space``
    via :func:`fetch_space_config` and projects its
    ``data_sources.tables[*].columns[*]`` to 4-part column FQNs. The
    fetch is best-effort: failures are logged and the capture proceeds
    with empty ``schema_columns`` so the resulting v1-shaped bundle
    still works under Trial 13i's pre-flight contract.

    Returns ``(spec, rows, schema_columns, serialized_space,
    schema_columns_source)`` — pass them through to :func:`write_capture`
    to produce a v2 payload (or v1 when ``schema_columns`` is empty).
    """
    from databricks.sdk import WorkspaceClient

    spec = _resolve_capture_spec(
        job_run_id=str(job_run_id), task_key=task_key, profile=profile
    )
    raw_traces = _fetch_predict_traces(
        experiment_id=spec.experiment_id,
        source_name=_source_name(spec.job_id, spec.task_run_id),
        task_start_ms=spec.task_start_ms,
        task_end_ms=spec.task_end_ms,
    )
    rows: list[dict[str, Any]] = []
    for trace in raw_traces:
        row = trace_to_eval_row(trace)
        if row is None:
            continue
        rows.append(row)

    if profile:
        w = WorkspaceClient(profile=profile)
    else:
        w = WorkspaceClient()
    schema_columns, serialized_space, schema_columns_source = (
        _fetch_schema_columns_for_space(w, spec.genie_space_id)
    )
    return spec, rows, schema_columns, serialized_space, schema_columns_source


def write_capture(
    *,
    spec: CaptureSpec,
    rows: Iterable[Mapping[str, Any]],
    output_path: Path,
    serialized_space: Mapping[str, Any] | None = None,
    schema_columns: Iterable[str] | None = None,
    schema_columns_source: str = "",
) -> Path:
    """Serialize a capture to ``output_path``.

    When ``schema_columns`` is non-empty the payload is written in the
    Trial 14 ``workbench_eval_capture_v2.1`` shape with the columns,
    the full ``serialized_space``, and the typed ASI
    ``metadata/<judge>/blame_set_structured`` /
    ``metadata/<judge>/blame_rationale`` flat keys at top-level so
    ``input_bundle.from_run_analysis_dir`` can lift them into
    ``metadata_snapshot["schema_columns"]`` and the Stage 1 reader
    can ``coerce_blame_entries`` the structured field directly.

    Backward compatibility — v2.1 is strictly additive over v2:

    * the new structured flat keys are extra columns on each row;
      v2 readers that don't know about them simply ignore them.
    * ``input_bundle.from_run_analysis_dir`` accepts both v2 and v2.1
      tokens (Trial 14 update there is the same one-line addition).

    When ``schema_columns`` is empty the v1 schema is preserved for
    backward compat — older bundles continue to load through the
    Trial 13i derivation chain.
    """
    rows_list = [dict(r) for r in rows]
    schema_columns_tuple = tuple(str(c) for c in (schema_columns or ()))
    is_v2 = bool(schema_columns_tuple)
    payload: dict[str, Any] = {
        "_schema_version": (
            # Trial 14 bumps the v2 token to v2.1 to signal that the
            # rows carry typed ``blame_set_structured`` flat keys. The
            # bundle loader accepts both tokens.
            "workbench_eval_capture_v2.1" if is_v2 else "workbench_eval_capture_v1"
        ),
        "_provenance": {
            "experiment_id": spec.experiment_id,
            "experiment_name": spec.experiment_name,
            "optimization_run_id": spec.optimization_run_id,
            "source_job_id": spec.job_id,
            "source_task_run_id": spec.task_run_id,
            "task_key": spec.task_key,
            "captured_at_utc": _dt.datetime.now(_dt.timezone.utc).isoformat(),
            "trace_count": len(rows_list),
            "genie_space_id": spec.genie_space_id,
            "schema_columns_source": schema_columns_source or "",
            "schema_columns_count": len(schema_columns_tuple),
        },
        "eval_rows": rows_list,
    }
    if is_v2:
        payload["serialized_space"] = (
            dict(serialized_space) if isinstance(serialized_space, Mapping) else {}
        )
        payload["schema_columns"] = list(schema_columns_tuple)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    return output_path


def default_output_path(
    *,
    docs_root: Path,
    optimization_run_id: str,
    task_run_id: str,
) -> Path:
    """Return the path ``from_run_analysis_dir`` searches first.

    This mirrors the artifact location committed alongside existing
    postmortems so the rest of the workbench picks the capture up with
    no other code change.
    """
    return (
        docs_root
        / "runid_analysis"
        / optimization_run_id
        / "evidence"
        / f"replay_fixture_from_latest_export_{task_run_id}.json"
    )


__all__ = [
    "CaptureSpec",
    "_extract_fqn_columns",
    "_fetch_schema_columns_for_space",
    "capture_eval_rows",
    "default_output_path",
    "trace_to_eval_row",
    "write_capture",
]
