# Ontology — Eval surfaced-scoping fix Goal-Mode driver (MV-D59 follow-on)

## Positioning (READ FIRST)

The eval harness is BUILT + live-wired + deploy-verified (`233da3cd`); each run persists
one `EvalReport` to `genie_ont_eval`. The **first live baseline caught a
measurement-scope bug**: P/R/F **and** structural health score over **all raw pre-gate
rows** (145 on the airline estate, incl. 128 suppressed dev/migration/demo clusters)
instead of the **surfaced** estate (17 — what users actually see). So precision reads
**17/145 = 0.117** and singleton/orphan rates are inflated. This tiny fix scopes the
report to surfaced domains so the baseline is meaningful.

This is the **first dogfood of `compare_reports`**: before (raw-scoped, already in
`genie_ont_eval`) vs after (surfaced-scoped) should show precision jump ~0.117 → ~1.0
and orphan/singleton rates drop, recall ~unchanged.

**Scope of the change:** wheel-only, in `eval_harness.py` (+ its tests). Filter to
surfaced at **`assemble_eval_report`** so the pure `compute_*` builders stay generic and
unit-testable. `materialize.py` is unchanged (the hook already passes all rows; the
harness now scopes internally) — only update materialize-test expectations if a fixture
carries unsurfaced rows.

---

## Spec / decisions

- **Spec:** `ontology-curation-redesign-build.md` §10 (MV-D59, incl. the recorded
  surfaced-scoping finding), §12 guardrails, §14 DoD.
- **Decisions:** MV-D59 (harness), MV-D56 (evidence is read, not re-derived — `surfaced`
  is an `evidence` field), MV-D43 (degrade-not-hang), MV-D45 (no dep), MV-D49 (additive;
  no DDL/grain change — this is pure logic), MV-D50. Do NOT edit the playbook.

---

## OWNS / OFF-LIMITS

- **OWNS:** `ontology/eval_harness.py` (the scoping filter in `assemble_eval_report`
  only), `tests/unit/test_ontology_eval_harness.py`, and — only if a fixture breaks —
  `tests/unit/test_ontology_materialize.py` expectations.
- **OFF-LIMITS:** all `backend/` + `frontend/`; `ddl.py` (no schema change); the engine
  stage logic and `alignment.py`; the pure `compute_*` builders' math (scope the INPUT,
  don't change the formulas); the apply path; `uv.lock` / `package-lock.json`.

---

## Driver prompt (paste verbatim)

```text
GOAL: Scope the eval harness (MV-D59) to the SURFACED estate so P/R/F + structural health
reflect what users see, not raw pre-gate clusters. Filter to surfaced domains at
assemble_eval_report; keep the pure compute_* builders unchanged (scope the INPUT, not the
math). Wheel-only, additive logic, no DDL, no dep. Offline code + green tests; STOP before
deploy. Branch: ontology.

SPEC: ontology-curation-redesign-build.md §10 (MV-D59) incl. the recorded surfaced-scoping
finding; §12/§14. DECISIONS: MV-D59, MV-D56 (surfaced is an evidence field: read
_load_evidence(row).get("surfaced")), MV-D43/D45/D49/D50. RULES: AGENTS.md. Read §10
first. eval_harness.py is BUILT — change ONLY the scoping.

CONTEXT: assemble_eval_report(run_id, metastore_id, timestamp, domain_rows, member_rows,
aligned_reference=None, llm_client=None) calls the 4 builds (compute_precision_recall_f1,
compute_structural_health, run_llm_sanity_monitor, build_spot_review_queue). Today it
passes ALL domain_rows/member_rows. On the airline estate that is 145 rows incl. 128
suppressed → precision 17/145 = 0.117. Each row carries evidence.surfaced (bool).

BUILD A — scoping at assemble_eval_report: add param surfaced_only: bool = True. When
true, BEFORE the 4 builds compute the surfaced domain set (rows whose
_load_evidence(row).get("surfaced") is truthy) and filter both domain_rows (to surfaced)
and member_rows (to members whose domain_id is in that surfaced set). Pass the filtered
rows to all four builds. The pure compute_* functions are UNCHANGED — they simply receive
the surfaced subset. surfaced_only=False preserves today's all-rows behavior (escape hatch
for tests). Degrade-not-hang (MV-D43): zero surfaced rows ⇒ P/R/F N/A + zeroed structural
health + empty queue, never raise.

BUILD B — tests (test_ontology_eval_harness): (1) a fixture with a mix of surfaced +
suppressed rows scores P/R/F over the SURFACED subset only — precision denominator is the
surfaced count (e.g. all-surfaced-aligned ⇒ precision 1.0), and suppressed rows never
appear as false positives; (2) structural health (singleton/orphan/depth/branching) is
computed over the surfaced subset; (3) surfaced_only=False reproduces the old all-rows
numbers (proves it's purely a scoping switch); (4) zero-surfaced ⇒ N/A/zeros, no raise.
Keep the existing pure compute_* unit tests as-is (they call the builders directly with
explicit rows). If a materialize-test fixture carries unsurfaced rows, update ONLY its
expected eval numbers to the surfaced-scoped values.

HARD GUARDRAILS: change ONLY the input scoping in assemble_eval_report — do NOT alter the
compute_* formulas, the report shape, report_to_dict/eval_report_to_row, or the DDL/table.
No new dependency (MV-D45); no backend/frontend; no apply-path; read-only. Deterministic.
Degrade-not-hang (MV-D43).

ACCEPTANCE (offline): ./scripts/test.sh + wheel suite green; the new scoping tests pass;
surfaced_only=False still yields the pre-fix numbers; uv.lock/package-lock.json untouched.

WORKFLOW: branch ontology. Do NOT deploy or run the job. When offline-green, STOP and
report the diff + test summary; a human runs the deploy-gated re-baseline + compare_reports.
```

---

## After the run (human-gated) — re-baseline + first `compare_reports`

**Before re-running, capture the current (raw-scoped) `genie_ont_eval.report` JSON** — the
table is metastore-keyed (one row per metastore), so the surfaced-scoped run **overwrites**
it. Then deploy `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless),
trigger the airline scope with `industry_alignment_enabled=true`,
`industry_alignment_reference_model=airline`, and read the new row: expect precision to
jump (~0.117 → ~1.0 for the fully-aligned surfaced set), orphan/singleton rates to drop,
recall roughly unchanged. Load both reports via `dict_to_report` and run
`compare_reports(before, after)` — the first live gate delta. Record the surfaced-scoped
report as the durable baseline (human logs it in the playbook — the agent never edits it).

> **Future (out of scope here):** `genie_ont_eval` keeps only the latest row per metastore.
> A run-keyed eval history (grain change) would let `compare_reports` trend across runs
> without a manual "before" capture — note for the backlog, not this fix.
