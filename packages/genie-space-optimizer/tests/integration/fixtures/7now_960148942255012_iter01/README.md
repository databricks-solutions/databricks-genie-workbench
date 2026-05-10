# 7now_960148942255012_iter01

| Field | Value |
|---|---|
| Workspace | 7now |
| Lever-loop task ID | `960148942255012` |
| Lever-loop attempt | 12 |
| Iteration | 1 |
| Run dir (postmortem) | `docs/runid_analysis/3b050ec5-4032-457f-a785-2d1a3942a097/` |
| Captured at | <FILL ON FIRST CAPTURE: ISO-8601 timestamp> |
| Captured by | <PR author handle> |
| Defects this anchor pins | D-5, D-7, D-8 (Chunk-D side) + forbidden-AG no-op loop (Chunk B side) |

## Why this iteration

Iter 1 of attempt 12 produces a `+8.1pp` candidate that is correctly
rolled back because target `gs_026` is rendered as `soft_passing`
rather than fixed. Iters 2-5 then repeat AG1 with zero proposals —
this anchor is the canonical evidence for the forbidden-AG no-op
loop (Phase 3, Task 3.3) and the journey-validator drift (D-8,
Phase 1, Task 1.10).

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
