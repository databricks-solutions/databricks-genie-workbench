# Trace-ID Propagation Loss — Investigation (2026-04-24)

Investigation-only writeup for the partial trace-map recovery observed on
run `baseline_eval_20260424_124548`. Scope: enumerate plausible causes and
propose a fix direction. No code changes here — implementation is a
follow-up plan.

---

## 1. Observed symptom

From the run's stdout:

```
Evaluation baseline_eval_20260424_124548 produced 0 trace IDs from 21 rows
(trace context may have been lost during Genie API calls)
[Eval] Recovered 10/21 trace IDs via fallback strategies
```

- **All 21 rows** in `rows_for_output` had `trace_id=None` when the eval
  summary was assembled — so the primary link (predict-fn span →
  eval row) was lost for every question.
- After falling through to `_recover_trace_map`, **10 of 21** were
  recovered via a fallback strategy.
- **11 of 21** stayed unrecovered. Downstream trace-linked operations
  (`log_expectations_on_traces`, `log_judge_verdicts_on_traces`,
  `log_gate_feedback`, and any drill-down UI keyed on `trace_id`) are
  blind on those 11 rows.

The run itself completed successfully (`overall_accuracy=100%`,
`thresholds_met=NO`). Trace loss is an observability regression, not a
scoring regression.

---

## 2. How the recovery path works today

Reference: `src/genie_space_optimizer/optimization/evaluation.py`.

### 2.1 Primary path — `trace_id` on each eval row

`run_evaluation` reads `trace_id` straight off each row:

```python
for _row in rows_for_output:
    _qid = _row.get("question_id") or _row.get("inputs/question_id") or ...
    _tid = _row.get("trace_id")
    if _qid and _tid:
        trace_map[_qid] = str(_tid)
    elif _qid:
        _rows_without_tid += 1
```

When all 21 rows have no `trace_id`, MLflow's `genai.evaluate()` did not
populate that column — meaning either the predict function didn't produce
a linked trace, or the trace existed but wasn't associated back to the
eval row.

### 2.2 Fallback — `_recover_trace_map`

Three strategies, tried in order, **short-circuited** the moment one
returns a non-empty map:

| Strategy | Mechanism | Requires |
|---|---|---|
| 1. `tags` | `mlflow.search_traces(locations=..., filter_string="tags.\`genie.optimization_run_id\`='<id>' AND tags.\`genie.iteration\`='<n>'")` | Both tags survived on the trace |
| 2. `time_window` | `mlflow.search_traces(locations=..., filter_string="attributes.timestamp_ms >= <start_ms>")` plus client-side filter on `tags.question_id` | Only `question_id` tag survived; `start_ms` captured before predict_fn ran |
| 3. `eval_results` | Read `eval_result.tables['eval_results']['trace_id']` | MLflow populated `trace_id` in the eval_results DataFrame (MLflow ≥ 2.18) |

### 2.3 Where the tags come from — the predict function

`make_predict_fn` (`evaluation.py:1843`) wraps every predict with:

```python
_trace_tags = {
    "question_id": kwargs.get("question_id", ""),
    "space_id": space_id,
}
if optimization_run_id:
    _trace_tags["genie.optimization_run_id"] = optimization_run_id
if iteration is not None:
    _trace_tags["genie.iteration"] = str(iteration)
...
mlflow.update_current_trace(tags=_trace_tags, metadata=_trace_metadata)
```

The whole block is wrapped in `try: ... except Exception: pass`, so a
`update_current_trace` failure is silently swallowed.

---

## 3. Hypotheses

### H1. `update_current_trace` silently fails when no trace is active

**Evidence for**

- The call is wrapped in a bare `except Exception: pass`. If
  `mlflow.update_current_trace()` is called when no trace is active
  (the autologging span hasn't opened yet, or already closed), it
  raises and the tags are silently dropped.
- In the observed run, **every** row has `trace_id=None`. A consistent
  failure across 21 rows is more consistent with a structural issue
  (no active trace when `update_current_trace` fires) than with
  intermittent loss.

**Evidence against**

- The `time_window` strategy recovered 10 traces, which means
  **some** traces WERE created with `question_id` tags. So at least
  half the time the trace context DID exist when
  `update_current_trace` ran.

**Verdict:** Partially true but doesn't explain the full symptom.
Likely coexists with another hypothesis.

### H2. SparkConnect context is not threaded into the predict function

**Evidence for**

- `genie_predict_fn` executes Genie HTTP calls (`run_genie_query`) AND
  Spark SQL calls (`spark.sql(...)`). Under SparkConnect, the Spark
  call goes off-box to a remote driver. MLflow's active-run /
  active-trace state is thread-local; RPC boundaries don't carry it.
- If MLflow autologging tries to attach the predict-fn span via a
  code path that hits a remote worker, the parent context is lost
  and the span is either created unparented or not at all.

**Evidence against**

- `evaluate()` should open the predict-fn trace on the driver before
  dispatching to Spark. Mid-call RPCs shouldn't disturb the already-open
  span.

**Verdict:** Plausible secondary cause for the 11 unrecovered traces.
Would explain why exactly the Spark-heavy calls (those that run the
GT SQL in the comparison branch) lose context while the Genie-only
paths survive.

### H3. `mlflow.genai.evaluate` writes `trace_id` to `eval_results` after `rows_for_output` is built

**Evidence for**

- `_recover_trace_map_via_eval_results` reads `eval_result.tables['eval_results']['trace_id']`.
- If `rows_for_output` is built from a different source (e.g., an
  earlier snapshot of eval rows before `trace_id` is backfilled),
  `trace_id` would be missing from our `rows_for_output` but
  present in `eval_result.tables["eval_results"]`.

**Evidence against**

- Strategy 3 is in the short-circuit chain. If it ran and returned a
  full map, we'd never see the "Recovered 10/21" message — we'd see
  "Recovered 21/21".

**Verdict:** Unlikely to be the primary cause, but would explain
why the strategies are short-circuited incorrectly (see H5).

### H4. The `genie.iteration` / `genie.optimization_run_id` tags are
being dropped selectively

**Evidence for**

- If strategy 1 (tags) hit for only a subset of traces, that's
  consistent with: the trace was created, `question_id` tag was
  applied on one `update_current_trace` call, then on a LATER
  `update_current_trace` call the `genie.iteration` tag failed to
  land (span already closed, or MLflow's tag-update call raced).
- MLflow's `update_current_trace` is sometimes only honored BEFORE
  the span ends. If `evaluate()` ends the span before all our
  tags are written, partial tagging happens.

**Evidence against**

- The predict_fn sets all tags in a single `update_current_trace`
  call (line 1906). There's no race between multiple calls in the
  same predict invocation.

**Verdict:** Unlikely for the observed symptom (all-or-nothing is
what we'd see with one update call), but a class of races to keep
in mind for future changes.

### H5. **Short-circuit in `_recover_trace_map` is too aggressive** (new)

**Evidence for**

- Code (`evaluation.py:3206-3237`):

  ```python
  winning_map: dict[str, str] = {}
  for name, fn in strategies:
      if winning_map:
          _log_trace_map_recovery_metric(name, 0)
          continue
      result = fn() or {}
      _log_trace_map_recovery_metric(name, len(result))
      if result:
          logger.info(...)
          winning_map = result
  ```

  If strategy 1 returns **10** trace IDs for 10 of the 21 questions,
  `winning_map` becomes non-empty, and strategies 2 and 3 are skipped
  even though they could cover the remaining 11.

- The symptom — "Recovered 10/21" — is **exactly** what you'd see if
  strategy 1 found a partial match and later strategies were skipped.

**Verdict:** High-confidence root cause for the 11-trace gap. The
short-circuit logic should UNION strategy results (fill missing qids
from later strategies) rather than pick the first non-empty one
verbatim.

---

## 4. Recommended fix direction

### 4.1 Primary fix — UNION instead of short-circuit

Replace the "first non-empty wins" logic in `_recover_trace_map` with
a fill-in-missing loop:

```python
recovered: dict[str, str] = {}
for name, fn in strategies:
    partial = fn() or {}
    _log_trace_map_recovery_metric(name, len(partial))
    for qid, tid in partial.items():
        recovered.setdefault(qid, tid)  # first-writer-wins per qid
    if len(recovered) >= expected_count:
        # All qids accounted for — skip remaining strategies.
        for remaining_name, _ in strategies[strategies.index((name, fn)) + 1:]:
            _log_trace_map_recovery_metric(remaining_name, 0)
        break
```

Properties:
- Preserves the "strategies are ordered by preference" contract —
  strategy 1's trace_id wins if it provided one.
- No longer loses traces that only strategy 2 or 3 can find.
- Early-exit on full coverage keeps the happy-path cost unchanged.

### 4.2 Secondary fix — preserve context across Spark boundary (H2)

Lower priority; only worth doing if H5 fix doesn't close the gap
completely. Options:

- Explicit trace-context propagation: capture `mlflow.get_current_active_span().context`
  on the driver, threadlocal-push it before each `spark.sql` call.
- Fall back to a fourth recovery strategy that uses the Genie
  `statement_id` as a secondary key (already stored on the row),
  looking up traces via `tags.genie.statement_id`.

### 4.3 Defensive fix — surface `update_current_trace` failures (H1)

Replace the blanket `except Exception: pass` around
`mlflow.update_current_trace(tags=...)` with:

```python
except Exception:
    logger.debug("Failed to update trace tags", exc_info=True)
    mlflow.log_metric("predict_fn.trace_tag_update_failures", 1)
```

Gives us a signal in MLflow when tag-writes are being dropped, so the
next occurrence has a diagnostic breadcrumb instead of a silent miss.

---

## 5. Scope estimate for the fix PR

| Change | Files | Lines | Tests |
|---|---|---|---|
| Union trace recovery | `evaluation.py:3180-3245` | ~20 | Add 2 tests to `test_trace_map_recovery.py` — "union across strategies" and "early exit when complete" |
| Log tag-update failures | `evaluation.py:1907` | ~3 | One unit test asserting the debug log + metric are emitted on failure |
| Secondary `statement_id` strategy (optional) | `evaluation.py` + predict_fn | ~30 | 1–2 new tests |

Primary fix is a half-day. Secondary-plus-defensive: one day.

---

## 6. Verification plan for the fix

1. **Unit** — `test_trace_map_recovery_unions_partial_strategy_hits`:
   - Strategy 1 returns `{q1:t1, q2:t2}` (partial).
   - Strategy 2 returns `{q3:t3}`.
   - Strategy 3 returns `{q4:t4}`.
   - Expect `_recover_trace_map` to return all four.
2. **Unit** — `test_trace_map_recovery_first_writer_wins`:
   - Strategy 1 returns `{q1:t1-from-tags}`.
   - Strategy 2 returns `{q1:t1-from-time-window, q2:t2}`.
   - Expect the result's `q1` value to be `t1-from-tags` (preserves
     ordering contract), plus `q2:t2`.
3. **Unit** — `test_trace_map_recovery_early_exit_on_full_cover`:
   - Strategy 1 returns `{q1:t1, q2:t2}` and `expected_count=2`.
   - Strategies 2 and 3 must NOT be invoked.
4. **Integration (manual)** — rerun `baseline_eval_20260424_124548`
   with the fix and confirm:
   - `Recovered N/21 trace IDs` reports ≥ 20.
   - `log_expectations_on_traces` and `log_judge_verdicts_on_traces`
     succeed for all resolved questions.

---

## 7. Open questions for reviewers

- Should `statement_id` be added as a fourth recovery key, or is the
  3-strategy + union approach sufficient? The answer depends on how
  often we see traces that exist but have no `question_id` tag.
- The `log_metric` calls in `_log_trace_map_recovery_metric` name each
  strategy explicitly. With union semantics, a single trace can be
  "found by" multiple strategies — should we still log per-strategy
  counts, or a single combined `recovered` metric?
