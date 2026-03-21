---
name: Fix labeling sessions
overview: "Fix three issues preventing labeling sessions from working: (1) session name exceeds 64-character API limit, (2) trace_map is always empty because trace context is lost during Spark Connect calls, and (3) all traces (pass + fail) are added, overwhelming reviewers."
todos:
  - id: fix-session-name
    content: Truncate session name to 64 chars in labeling.py line 165
    status: completed
  - id: recover-trace-map
    content: Add _recover_trace_map helper in evaluation.py that searches experiment traces by tags when trace_map is empty
    status: completed
  - id: add-failure-qids-labeling
    content: Add failure_question_ids parameter to create_review_session and _populate_session_traces in labeling.py
    status: completed
  - id: add-extract-qid-helper
    content: Add _extract_question_id helper and question-ID-based trace filtering logic in _populate_session_traces
    status: completed
  - id: remove-backfill
    content: Remove backfill logic when failure_question_ids filtering is active
    status: completed
  - id: accumulate-qids-harness
    content: Accumulate all_failure_question_ids in harness.py lever loop and pass to both call sites
    status: completed
isProject: false
---

# Fix Labeling Sessions: Name Length, Trace Recovery, and Failed-Only Filtering

## Problem 1: Session Name Too Long

The MLflow API enforces a 64-character limit on `labeling_session.name`. The current name format on [labeling.py line 165](src/genie_space_optimizer/optimization/labeling.py) produces names like:

```
genie_opt_review_new_travel_booking_and_payment_analytics_66a936b1_20260305_112141
```

That is 82 characters (18 over the limit), causing a `BAD_REQUEST` error.

**Fix:** Shorten the prefix and truncate the domain portion to fit within 64 characters while keeping the timestamp and run_id suffix (which ensure uniqueness):

```python
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
suffix = f"_{run_id[:8]}_{ts}"          # 25 chars (e.g. _66a936b1_20260305_112141)
prefix = f"opt_{domain}"                 # e.g. opt_new_travel_booking_and_payment_analytics
max_prefix = 64 - len(suffix)
if len(prefix) > max_prefix:
    prefix = prefix[:max_prefix]
session_name = f"{prefix}{suffix}"
```

This produces e.g. `opt_new_travel_booking_and_payment_a_66a936b1_20260305_112141` (64 chars).

---

## Problem 2: trace_map Is Always Empty (0 Trace IDs)

The evaluation always produces 0 trace IDs:

```
Evaluation genie_eval_iter0_20260305_110911 produced 0 trace IDs from 54 rows
(trace context may have been lost during Genie API calls)
```

**Root cause chain:**

- `genie_predict_fn` is decorated with `@mlflow.trace` (line 1206) and sets a `question_id` tag on each trace (line 1219)
- During execution, Spark Connect gRPC calls (`spark.sql()` for EXPLAIN, GT SQL) corrupt/break the MLflow trace context
- When `mlflow.genai.evaluate()` finishes, `eval_item.trace` is `None` for all rows
- The safety monkey-patch `_safe_batch_link_traces_to_run` (line 2187) filters out None-trace results, so no traces get linked to the eval run
- The `trace_map` loop (lines 3298-3310) finds no `trace_id` values in the result rows

**However, the traces DO exist** in the MLflow tracking store (the `@mlflow.trace` decorator logs them), and each trace has tags including `question_id`, `genie.optimization_run_id`, and `genie.iteration` (set at lines 1218-1227). They are just not linked to the eval run.

**Fix:** Add a `_recover_trace_map` helper in [evaluation.py](src/genie_space_optimizer/optimization/evaluation.py) that recovers the mapping after evaluation by searching experiment traces by tags:

```python
def _recover_trace_map(
    experiment_id: str,
    optimization_run_id: str,
    iteration: int,
    expected_qids: list[str],
) -> dict[str, str]:
    """Recover question_id -> trace_id mapping by searching experiment traces.

    When mlflow.genai.evaluate() loses trace context (e.g. due to Spark
    Connect gRPC calls), traces still exist in the tracking store with
    tags set by genie_predict_fn. This function finds them.
    """
    if not experiment_id or not optimization_run_id:
        return {}

    try:
        filter_parts = [
            f"tags.`genie.optimization_run_id` = '{optimization_run_id}'",
            f"tags.`genie.iteration` = '{iteration}'",
        ]
        traces_df = mlflow.search_traces(
            experiment_ids=[experiment_id],
            filter_string=" AND ".join(filter_parts),
            max_results=500,
        )
        if traces_df is None or len(traces_df) == 0:
            return {}

        recovered: dict[str, str] = {}
        for _, row in traces_df.iterrows():
            tid = row.get("trace_id")
            tags = row.get("tags")
            if isinstance(tags, dict):
                qid = tags.get("question_id", "")
            else:
                qid = ""
            if tid and qid:
                recovered[qid] = str(tid)

        return recovered
    except Exception:
        logger.debug("Trace recovery via tag search failed", exc_info=True)
        return {}
```

Insert recovery call at line ~3312 of `run_evaluation()`, right after the "0 trace IDs" warning:

```python
if not trace_map and exp:
    trace_map = _recover_trace_map(
        experiment_id=exp.experiment_id,
        optimization_run_id=optimization_run_id,
        iteration=iteration,
        expected_qids=[...],  # from rows_for_output
    )
    if trace_map:
        logger.info(
            "Recovered %d/%d trace IDs from experiment via tag search",
            len(trace_map), len(rows_for_output),
        )
        print(f"[Eval] Recovered {len(trace_map)} trace IDs via tag search")
```

**Impact:** This fixes ALL downstream consumers of `trace_map`:

- `failure_trace_ids` will be populated in harness.py
- `regression_trace_ids` will be populated in harness.py
- `log_expectations_on_traces()` will work
- `log_gate_feedback_on_traces()` will work
- `log_asi_failures_on_traces()` will work
- Labeling session priority filtering by trace ID will work

Also apply the same recovery to `run_repeatability_evaluation()` (line ~3564) which has an identical `trace_map` building pattern.

---

## Problem 3: All Traces Added to Session (Pass + Fail)

Currently `_populate_session_traces` adds ALL traces from the eval runs. Even with trace recovery working, the user wants only failed question traces in the labeling session to avoid overwhelming reviewers.

**Fix:** Thread `failure_question_ids` through the pipeline as a secondary filter (defense-in-depth alongside trace IDs).

### Changes in [harness.py](src/genie_space_optimizer/optimization/harness.py)

**Baseline call (~line 410):** Pass `_bl_failures` to `create_review_session`:

```python
_bl_session_info = create_review_session(
    ...,
    failure_question_ids=list(_bl_failures),   # NEW
)
```

**Lever loop end-of-run call (~line 3830):** Accumulate `all_failure_question_ids`:

- Add `all_failure_question_ids: list[str] = []` at ~line 3114
- At ~line 3685 (gate fail): `all_failure_question_ids.extend(_fail_qids)`
- At ~line 3728 (accept): `all_failure_question_ids.extend(_full_failures)`
- Pass to `create_review_session`

### Changes in [labeling.py](src/genie_space_optimizer/optimization/labeling.py)

`**create_review_session`:** Add `failure_question_ids: list[str] | None = None` parameter, pass through.

`**_populate_session_traces`:** Add `failure_question_ids` parameter. After collecting all traces, if `failure_question_ids` is non-empty, filter the DataFrame by matching question IDs in the trace `request` JSON:

```python
def _extract_question_id(request_val) -> str:
    if not request_val:
        return ""
    try:
        req = json.loads(request_val) if isinstance(request_val, str) else request_val
        if isinstance(req, dict):
            return (req.get("question_id")
                    or req.get("kwargs", {}).get("question_id", ""))
    except Exception:
        pass
    return ""
```

When `failure_question_ids` is provided, filter to only those traces and skip the backfill logic entirely. When not provided, fall back to existing priority/backfill behavior.

---

## Summary of File Changes

- **[evaluation.py](src/genie_space_optimizer/optimization/evaluation.py):** Add `_recover_trace_map()` helper; call it after `trace_map` is empty in both `run_evaluation()` (~~line 3312) and `run_repeatability_evaluation()` (~~line 3564)
- **[labeling.py](src/genie_space_optimizer/optimization/labeling.py) line 165:** Truncate session name to 64 characters
- **[labeling.py](src/genie_space_optimizer/optimization/labeling.py) lines 128-320:** Add `failure_question_ids` parameter, `_extract_question_id` helper, question-ID-based filtering, skip backfill when filtering
- **[harness.py](src/genie_space_optimizer/optimization/harness.py) ~line 3114:** Add `all_failure_question_ids` accumulator
- **[harness.py](src/genie_space_optimizer/optimization/harness.py) ~lines 3685, 3728:** Accumulate failure question IDs
- **[harness.py](src/genie_space_optimizer/optimization/harness.py) ~lines 410, 3830:** Pass `failure_question_ids` in both call sites

