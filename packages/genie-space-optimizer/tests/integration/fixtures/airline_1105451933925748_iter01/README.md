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

| Field path | Redaction |
|---|---|
| `eval_rows[].question_text` | replaced with `"<redacted>"` |
| `eval_rows[].generated_sql` | replaced with `"<redacted>"` |
| `eval_rows[].expected_sql` | replaced with `"<redacted>"` |
| `eval_rows[].evidence` | replaced with `"<redacted>"` |
| `applied_patches[].patch_body.sql_body` | replaced with `"<redacted>"` |
| `applied_patches[].patch_body.expression` | replaced with `"<redacted>"` |
| `databricks_*_id` | last 4 chars preserved, rest replaced with `X` |

These redactions preserve every field name, type, and structural
shape — just not the SQL/text bodies. The capture script enforces
the redaction list; tests assert no field carries a non-redacted
SQL body.
