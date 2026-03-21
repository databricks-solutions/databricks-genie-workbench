---
name: Fix Scorer Accuracy Blockers
overview: "Fix 8 compounding scorer and regression-gate issues that block the lever loop from making any progress: a broken asset_routing type-vs-name comparison, result_correctness penalizing cosmetic differences, join spec prose contamination, and zero-threshold judges triggering rollbacks."
todos:
  - id: normalize-expected-asset
    content: Add _normalize_expected_asset() helper in evaluation.py and apply at all benchmark creation/loading sites (lines 4058, 4339, 4541, 4567, 4611, and benchmarks.py:462)
    status: in_progress
  - id: improve-detect-asset-type
    content: Enhance detect_asset_type() in genie_client.py with optional mv_names parameter and tightened TVF regex
    status: pending
  - id: signature-match-as-pass
    content: In evaluation.py:1522, include sig_match in the match condition so signature matches count as correct
    status: pending
  - id: fix-asset-routing-scorer
    content: Update asset_routing.py scorer to normalize expected_asset through detect_asset_type when value is a table name, not a type
    status: pending
  - id: arbiter-adjusted-rc-score
    content: Add arbiter-adjusted result_correctness per-judge score computation in evaluation.py so detect_regressions sees true signal
    status: pending
  - id: sanitize-join-spec-sql
    content: Add _sanitize_join_sql() in optimizer.py to strip prose/cardinality labels from LLM-generated join conditions
    status: pending
  - id: exclude-zero-threshold-judges
    content: Add skip_judges parameter to detect_regressions() and pass informational judges (threshold=0.0) at both call sites in harness.py
    status: pending
  - id: deploy-and-trigger
    content: Deploy the fixes, trigger a new optimization run, and verify scorer improvements
    status: pending
isProject: false
---

# Fix Scorer Accuracy Blockers

## Problem Analysis

Two broken scorers plus a regression-gate blindspot block the entire lever loop. Every iteration rolls back despite real accuracy being 87-96%.

Evidence from iteration 2:

- Accuracy **improved** 87.5% -> 95.8%, arbiter says 22/24 both_correct
- Rolled back anyway: `asset_routing 16.7->8.3 (+8.3), response_quality 77.3->69.6 (+7.7)`
- 13 high-quality patches (dim_host enrichment, join specs, instructions, example SQL) discarded

### Issue 1: `asset_routing` -- type-vs-name mismatch (8.3% score)

The scorer at [asset_routing.py](src/genie_space_optimizer/optimization/scorers/asset_routing.py) line 31 compares `actual_asset` (detected type: `MV`/`TVF`/`TABLE`) against `expected_asset` from benchmarks which contains **table names** (`BOOKING_ANALYTICS_METRICS`, `DIM_HOST`, `BRIDGE_PROPERTY_AMENITY`). So `"BOOKING_ANALYTICS_METRICS" == "MV"` always fails.

From iteration 2: Q1, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q14, Q15, Q17, Q19, Q22, Q23, Q24 all fail asset_routing despite exact/column_subset result matches and `both_correct` arbiter verdicts.

### Issue 2: `result_correctness` -- cosmetic mismatches (70.8% score)

[evaluation.py](src/genie_space_optimizer/optimization/evaluation.py) line 1522 marks `signature` matches as `match = False`. A `signature` match means same row count, same data patterns, different hash (timestamp format `2022-12` vs `2022-12-01T00:00:00.000Z`, boolean casing `True` vs `true`).

From iteration 2: Q10 (145 rows, same data, `DATE_TRUNC` vs `DATE_FORMAT` timestamp), Q3 (3742 rows, missing 2 extra columns), Q11, Q19 all fail result_correctness with `both_correct` arbiter.

### Issue 3: `response_quality` triggers rollback despite threshold=0.0 (NEW)

`detect_regressions()` at [optimizer.py](src/genie_space_optimizer/optimization/optimizer.py) line 5394 checks ALL judges blindly. `response_quality` has convergence threshold `0.0` in `DEFAULT_THRESHOLDS` (explicitly informational), yet its 7.7-point drop in iteration 2 contributed to the rollback decision. This happened because Genie non-deterministically returns follow-up questions instead of analysis (Q3: "Would you like to see host performance...", Q7: "Would you like to see the top 5 destinations by net revenue...").

### Issue 4: Join spec prose contamination (recurring)

Both iteration 2 AG3 and iteration 4 AG4 had valid join specs rejected:

```
Invalid join spec rejected: {'sql': ['fact_booking.property_key = dim_property.property_key AND dim_property.is_current = true, MANY_TO_ONE. Use this join...']}
```

The LLM embeds cardinality labels and prose descriptions into the `sql` field.

### Impact

- **All 5 iterations rolled back**, zero levers accepted
- Accuracy improved to 95.8% but was discarded
- Strategist wastes budget on phantom "asset routing errors" instead of real issues (booking_status hallucination Q8/Q23, missing temporal filters Q2)

---

## Fixes

### Fix 1: Normalize `expected_asset` to type category everywhere

**[evaluation.py](src/genie_space_optimizer/optimization/evaluation.py)** -- Add helper:

```python
_VALID_ASSET_TYPES = frozenset({"MV", "TVF", "TABLE"})

def _normalize_expected_asset(raw: str, expected_sql: str) -> str:
    upper = raw.strip().upper()
    if upper in _VALID_ASSET_TYPES:
        return upper
    return detect_asset_type(expected_sql)
```

Apply at all benchmark creation/loading sites (lines 4058, 4339, 4541, 4567, 4611) and in [benchmarks.py](src/genie_space_optimizer/optimization/benchmarks.py) line 462.

### Fix 2: Improve `detect_asset_type` heuristic

**[genie_client.py](src/genie_space_optimizer/common/genie_client.py)** line 254 -- Add optional `mv_names` parameter, tighten TVF regex to require `get`_ followed by `(`:

```python
def detect_asset_type(sql: str, mv_names: list[str] | None = None) -> str:
    sql_lower = sql.lower()
    if "measure(" in sql_lower:
        return "MV"
    if mv_names and any(name.lower() in sql_lower for name in mv_names):
        return "MV"
    if "mv_" in sql_lower:
        return "MV"
    if re.search(r'\bget_\w+\s*\(', sql_lower):
        return "TVF"
    return "TABLE"
```

### Fix 3: Treat `signature` match as correct

**[evaluation.py](src/genie_space_optimizer/optimization/evaluation.py)** line 1522 -- Include `sig_match`:

```python
"match": exact_match or hash_match or subset_match or approx_match or tied_subset or sig_match,
```

This also unlocks the `schema_accuracy` result-match override (schema_accuracy.py:126) for signature-matched questions like Q10.

### Fix 4: Make `asset_routing` scorer robust to both type and name values

**[asset_routing.py](src/genie_space_optimizer/optimization/scorers/asset_routing.py)** -- Normalize before comparison:

```python
from genie_space_optimizer.common.genie_client import detect_asset_type

VALID_TYPES = {"MV", "TVF", "TABLE"}
expected_sql = expectations.get("expected_response", "")
expected_type = expected_asset if expected_asset in VALID_TYPES else detect_asset_type(expected_sql)
correct = actual_asset == expected_type
```

### Fix 5: Arbiter-adjusted `result_correctness` per-judge score

**[evaluation.py](src/genie_space_optimizer/optimization/evaluation.py)** -- After computing `per_judge` from raw MLflow metrics (around line 2996), recompute `result_correctness` using arbiter verdicts:

```python
if rows_for_output:
    rc_total = rc_correct = 0
    for row in rows_for_output:
        rc_val = str(row.get("result_correctness/value", "")).lower()
        if rc_val == "excluded":
            continue
        rc_total += 1
        arbiter_val = str(row.get("arbiter/value", "skipped")).lower()
        if rc_val in ("yes", "true", "1", "1.0"):
            rc_correct += 1
        elif arbiter_val in ("genie_correct", "both_correct"):
            rc_correct += 1
    if rc_total > 0:
        per_judge["result_correctness"] = rc_correct / rc_total
```

This ensures `detect_regressions()` sees ~95.8% instead of ~66.7%.

### Fix 6: Sanitize join spec SQL field

**[optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)** -- After `_call_llm_for_join_discovery` returns results, sanitize each `sql` entry:

```python
def _sanitize_join_sql(sql_str: str) -> str:
    """Strip prose from join condition SQL. Keeps only the ON-clause expression."""
    # Split on comma followed by cardinality label or prose
    cleaned = re.split(r',\s*(?:MANY_TO_|ONE_TO_|Use this|This join|Always)', sql_str, flags=re.IGNORECASE)[0]
    # Split on period followed by space+uppercase (prose sentence boundary)
    cleaned = re.split(r'\.\s+[A-Z]', cleaned)[0]
    return cleaned.strip().rstrip(',').rstrip('.')
```

Apply to each entry in `join_spec["sql"]` before proposal validation.

### Fix 7: Exclude zero-threshold judges from regression detection

**[optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)** line 5394 -- Add `skip_judges` parameter:

```python
def detect_regressions(
    current_scores: dict[str, float],
    previous_scores: dict[str, float],
    threshold: float = REGRESSION_THRESHOLD,
    skip_judges: set[str] | None = None,
) -> list[dict]:
    regressions: list[dict] = []
    for key in previous_scores:
        if skip_judges and key in skip_judges:
            continue
        ...
```

**[harness.py](src/genie_space_optimizer/optimization/harness.py)** -- At both call sites (lines 1176 and 1288), compute and pass `skip_judges`:

```python
_informational_judges = {j for j, t in DEFAULT_THRESHOLDS.items() if t == 0.0}
regressions = detect_regressions(
    full_scores, best_scores, threshold=effective_regression_tol,
    skip_judges=_informational_judges,
)
```

This prevents `response_quality` (threshold=0.0) from triggering rollbacks. In iteration 2, this would have removed one of the two regression signals, and combined with the asset_routing fixes, the iteration would have been accepted.

---

## Expected Outcome

- `asset_routing` jumps from 8.3% to ~91% (only Q2 and Q16 legitimately fail)
- `result_correctness` improves from ~66-70% to ~87-96% via signature match + arbiter adjustment
- `response_quality` drops no longer trigger rollbacks (threshold=0.0 = informational only)
- Regression gate stops firing on phantom regressions; lever loop can actually make progress
- Join spec proposals like the dim_property-to-bridge_property_amenity join succeed
- `schema_accuracy` result-match override fires for signature-matched questions (e.g., Q10)
- Strategist focuses on real issues: booking_status hallucination (Q8/Q23), missing temporal filters (Q2)

