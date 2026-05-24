# Production Replay Cases

## Why this corpus exists

The pre-existing `tests/unit/fixtures/production_eval_rows.json::hydration_rows`
fixture is **shape-only**: every row carries `question` text plus SQL inline,
so any test that consumes it proves only "if a row has the question, the
builders hydrate it." That is true and worth pinning, but the **production
rows we actually saw for hard QIDs carry those fields at production-specific
paths** such as `request.question`, `expected_response/value`,
`response.response`, `<judge>/rationale`, and `metadata/<judge>/<field>`.
Trial 11 (run `98ec8950-d7d4-40b3-b5c0-36dcfb3fb610`) and Trial 12 (run
`dc89d1a9-2020-4f42-994d-1ae05b865398`) both produced zero applied patches
because the accessor layer did not cover the row shape while every harness
test stayed green.

This corpus closes that gap: each file here is a **sanitized snapshot of
exactly the upstream data the canonical Stage 1 card builder receives for a
hard QID in production**, so the harness can fail loudly the next time the
input contract drifts. Cases are extracted from `docs/runid_analysis/<run_id>/
evidence/` and committed as immutable per-(run, qid) JSON.

## Case file shape

Each file under `tests/integration/fixtures/production_replay/` describes a
single `(run, qid)` pair the canonical card builder must handle. Field
glossary:

```json
{
  "_schema_version": "production_case_v1",
  "_provenance": {
    "source_run_id": "<full run uuid, unsanitized>",
    "source_qid": "<full production qid, unsanitized>",
    "source_artifacts": [
      "docs/runid_analysis/<run_id>/evidence/replay_fixture_from_latest_export_<task_run_id>.json::iterations[*].eval_rows[*]",
      "docs/runid_analysis/<run_id>/evidence/analysis_inputs_<task_run_id>.json::stage1_input_card_sample"
    ],
    "field_sources_snapshot": {
      "blame_set_seed": "present|absent",
      "generated_sql": "present|absent",
      "ground_truth_sql": "present|absent",
      "judge_rationale": "present|absent",
      "question_text": "present|absent",
      "rca_evidence": "present|absent"
    },
    "violations_snapshot": ["question_text_empty", "..."]
  },
  "qid": "<sanitized qid used in tests>",
  "row": {
    "request": {
      "kwargs": {"question_id": "<sanitized qid>"},
      "question": "<production question text>"
    },
    "expected_response/value": "<production-shaped expected SQL>",
    "response": {"response": "<production-shaped generated SQL>"},
    "<judge>/rationale": "<per-judge rationale>",
    "metadata/<judge>/<field>": "<flat ASI metadata>"
  },
  "typed_evidence": {
    "qid": "<sanitized qid>",
    "observed_failure": "...",
    "generated_sql_issue": "...",
    "expected_sql_shape": "...",
    "blame_set": ["catalog.schema.table.column", "..."],
    "suggested_repair_family": "add_example_sql|add_sql_snippet_filter|...",
    "repair_hint_patch_type": "ADD_EXAMPLE_SQL|ADD_SQL_SNIPPET_FILTER|...",
    "confidence": "high|medium|low",
    "quoted_evidence": ["...", "..."]
  },
  "expected_card_violations": ["question_text_empty"]
}
```

### Field semantics

- **`row`** — the captured eval row shape the production hard-QID lane carries.
  Snapshot intentionally includes the exact production paths the accessors must
  cover (`request.question`, `expected_response/value`, `response.response`,
  per-judge rationale keys, and ASI metadata keys). Do NOT introduce
  `joined_row_fields`; a join shortcut can make the harness green while the
  deployed row shape still fails.
- **`typed_evidence`** — the upstream `PerQidRcaEvidence` carrier, fully
  populated for hard QIDs. Drives `blame_set_seed`, `rca_evidence.*`, and
  `judge_rationale` fallback in the card builder.
- **`expected_card_violations`** — the violations the canonical builder
  produces *today*, before any code-side ladder fix. Tests that consume the
  case pin this list. It is `[]` for the committed corpus because the row
  itself now carries the real question/SQL/rationale/metadata paths.

## Sanitization rules (immutable per case once committed)

These rules ensure the corpus contains no customer literals while preserving
the shape that drives the optimizer:

1. **Run tag**: `98ec` (airline) and `dc89` (7now-delivery) — derived from the
   first four hex chars of the source run UUID. Used as the case filename
   prefix.
2. **Domain prefix substitution**:
   - `airline_ticketing_and_fare_analysis_` → `domain_a_`
   - `7now_delivery_analytics_space_` → `domain_b_`
   - Applied consistently across the `qid` field, every `question_id` value,
     and every `target_qid` / `blame_set` entry that contains the domain
     prefix.
3. **SQL literals**: real table and column names from the customer schema are
   replaced with neutral fixtures (`orders`, `payments`, `stores`, `region`,
   `quarter`, `revenue`, `currency`). The **structural shape** of the SQL
   (filters, joins, aggregations, missing clauses) is preserved because the
   downstream patch generators key off shape, not literals.
4. **Blame set fully-qualified names**: `<catalog>.<schema>.<table>.<column>`,
   using the sanitized table/column names from rule 3. Catalog/schema are
   normalized to `main.public`.
5. **No PII, no customer names, no internal identifiers**: enforced by a CI
   greppable token list (see `replay_row_sanitizer.py` constants).
6. **Field-source parity**: the snapshot of `field_sources` and
   `violations` from the postmortem `analysis_inputs.*` artefact MUST match
   what the canonical card builder produces against the sanitized case. This
   is a contract the consuming test checks — if it diverges, either the
   sanitization is wrong or the builder has changed behaviour.

## Committed corpus (as of this PR)

| Case file | Source run | Source QID | Postmortem |
|---|---|---|---|
| `98ec__gs_009.json` | `98ec8950-d7d4-40b3-b5c0-36dcfb3fb610` | `airline_ticketing_and_fare_analysis_gs_009` | `docs/runid_analysis/98ec8950-.../postmortem.md` |
| `98ec__gs_016.json` | `98ec8950-d7d4-40b3-b5c0-36dcfb3fb610` | `airline_ticketing_and_fare_analysis_gs_016` | same |
| `98ec__gs_024.json` | `98ec8950-d7d4-40b3-b5c0-36dcfb3fb610` | `airline_ticketing_and_fare_analysis_gs_024` | same |
| `dc89__gs_001.json` | `dc89d1a9-2020-4f42-994d-1ae05b865398` | `7now_delivery_analytics_space_gs_001` | `docs/runid_analysis/dc89d1a9-.../postmortem.md` |
| `dc89__gs_013.json` | `dc89d1a9-2020-4f42-994d-1ae05b865398` | `7now_delivery_analytics_space_gs_013` | same |
| `dc89__gs_021.json` | `dc89d1a9-2020-4f42-994d-1ae05b865398` | `7now_delivery_analytics_space_gs_021` | same |
| `dc89__gs_026.json` | `dc89d1a9-2020-4f42-994d-1ae05b865398` | `7now_delivery_analytics_space_gs_026` | same |

## How to add a new case

1. Locate the source artefacts in `docs/runid_analysis/<run_id>/evidence/`.
   The minimum required pair is `replay_fixture_from_latest_export_*.json`
   (for the row shape) and `analysis_inputs_*.json` (for the
   `stage1_input_card_sample` field_sources/violations snapshot the loader
   pins against).
2. Apply the substitution rules above. Run the sanitization audit:
   `pytest tests/integration/test_production_replay_corpus_sanitization.py`.
3. Add an entry to the committed-corpus table in this file.
4. The new case is immutable from that point — fixing the underlying
   contract bug updates `expected_card_violations`, not the row/evidence
   payload. New runs go in new files, never in existing ones.
