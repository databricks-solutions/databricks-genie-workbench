# airline_1105451933925748_iter01

| Field | Value |
|---|---|
| Workspace | airline |
| Lever-loop task ID | `1105451933925748` |
| Lever-loop attempt | 14 |
| Iteration | 1 |
| Run dir (postmortem) | `docs/runid_analysis/1099b152-8655-4f1e-ab43-1240a9400280/` |
| Captured at | <FILL ON FIRST CAPTURE: ISO-8601 timestamp> |
| Captured by | <PR author handle> |
| Defects this anchor pins | D-3 ext, D-4, D-5, D-6, D-7 |

## Why this iteration

Iter 1 of attempt 14 is the first in-production demonstration of
`accepted_with_attribution_drift` keep-the-win acceptance: 83.3% →
95.8% with target `gs_024` remaining still-hard. It exercises
Chunk D's full surface — acceptance gate, learning, bundle
assembly, run manifest — and reproduces the five Chunk-D defects
this cycle closes.

## Redactions applied

### `REDACTION_FIELDS` — replaced with `"<redacted>"`

| Field key | Reason |
|---|---|
| `question_text` | Customer question text |
| `generated_sql` | LLM-generated SQL body |
| `expected_sql` | Golden SQL body |
| `evidence` | Free-text RCA evidence |
| `sql_body` | Patch SQL body |
| `expression` | Metric / computed-column expression |
| `analysis_text` | LLM analysis free-text |
| `rationale` | LLM-generated rationale |
| `change_description` | Patch change description |
| `counterfactual_fix` | LLM counterfactual fix text |
| `counterfactual_fixes` | LLM counterfactual fixes (collection) |
| `sql` | Inline SQL string |
| `definition` | Column / metric definition text |
| `description` | Free-text description |
| `display_name` | Human-readable name (may contain customer data) |
| `alias` | Column alias (may carry customer naming) |
| `proposed_value` | LLM-proposed value |
| `actual_value` | Measured value (may carry customer figures) |
| `expected_value` | Expected value (may carry customer figures) |

### `DBX_ID_FIELDS` — last 4 chars preserved, rest replaced with `X`

| Field key | Reason |
|---|---|
| `databricks_job_id` | Workspace job identifier |
| `databricks_parent_run_id` | Parent MLflow run ID |
| `lever_loop_task_run_id` | Lever-loop task run ID |
| `experiment_id` | MLflow experiment ID |
| `client_request_id` | Databricks client request ID |
| `conversation_id` | Genie conversation ID |

These redactions preserve every field name, type, and structural
shape — just not the SQL/text bodies or workspace IDs. The capture
script enforces this list via a two-pass check (redact → fail-loud
guard for unknown long-text fields). Adding a new sensitive field
requires updating `REDACTION_FIELDS` in
`scripts/capture_stage_fixture.py` AND this README in the same
commit.
