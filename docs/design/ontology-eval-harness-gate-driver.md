# Ontology — Eval harness GATE Goal-Mode driver (MV-D59, follow-on)

Copy-paste launcher to turn the **already-built but inert** ontology eval scorer into a
**runnable regression gate**. Run on the **`ontology`** branch. Wheel-only, additive,
**no new dependency / no new table**; ends offline-green and **STOPs before deploy**.

## Why this (state check, 2026-09)

The offline scorer **is built and committed** — `ontology/eval_harness.py`
(`assemble_eval_report` + `compare_reports` + PRF / structural-health / LLM-sanity /
spot-review), landed as commit `7120a6df`, **28 tests green**. But it is **inert**: it is
referenced only by its own test — **nothing reads a materialized run into it and nothing
runs it**, so it cannot yet fulfil §10's stated job ("gate every subsequent
signal/threshold change"). This follow-on adds the missing plumbing:

1. a **read-only reader** that pulls the latest run's `genie_ont_domains` +
   `genie_ont_members` rows into the shape the scorer expects,
2. a **runnable entrypoint** that emits + persists a report (JSON artifact — no new table),
3. **baseline-vs-current gating** (non-zero exit on regression) so a change is *measured*,
4. an optional **airline gold reference** to light up precision/recall/F (until §9/MV-D58
   materializes the aligned reference, PRF degrades to N/A and the gate rides structural
   health).

- **Spec:** `docs/design/ontology-curation-redesign-build.md` §10 (MV-D59) + §11/§12/§14.
- **Existing scorer driver (built):** `docs/design/ontology-eval-harness-driver.md`.
- **Project rules:** `AGENTS.md`.

## OWNS / OFF-LIMITS

- **OWNS:** `ontology/eval_harness.py` (append the reader — do NOT change existing scorer
  outputs), `jobs/run_ontology_eval.py` (new entrypoint), a gold-reference fixture + loader,
  `tests/unit/test_ontology_eval_gate.py` (new).
- **OFF-LIMITS:** all `backend/`, all `frontend/`, `ontology/ddl.py` + `materialize.py`
  (no new table/column/DDL — MV-D49), every other engine stage module, and
  `docs/design/mv-advisor-playbook.md`.

---

## Driver prompt (paste verbatim)

```text
GOAL: Turn the BUILT-but-inert ontology eval scorer (ontology/eval_harness.py, MV-D59, commit 7120a6df) into a RUNNABLE regression GATE. The scorer (assemble_eval_report + compare_reports) works offline but nothing reads a materialized run into it or runs it. Add a read-only reader, a runnable entrypoint that emits+persists a report, baseline-vs-current gating, and an optional gold reference to light up P/R/F. Offline code + green tests; STOP before deploy. Branch: ontology.

SPEC: docs/design/ontology-curation-redesign-build.md §10 (MV-D59) + §11/§12/§14. Existing scorer driver (built): ontology-eval-harness-driver.md. DECISIONS: MV-D59; honor MV-D45 (no new dep, uv.lock untouched), MV-D49 (metastore grain, NO new table/column/DDL), MV-D50 (OBO-default reads), MV-D43 (degrade-not-hang), MV-D56 (evidence is read, not re-derived), MV-D65 (reuse the wheel LLM client). RULES: AGENTS.md.

CONTEXT (real symbols): assemble_eval_report(run_id, metastore_id, timestamp, domain_rows, member_rows, aligned_reference=None, llm_client=None) + compare_reports(before, after) exist in ontology/eval_harness.py. Snapshot tables (ddl.py): genie_ont_domains (TABLE_ONT_DOMAINS; cols incl. domain_id, parent_id, name, evidence JSON, score, run_id, as_of, metastore_id) and genie_ont_members (TABLE_ONT_MEMBERS; cols incl. domain_id, asset_fqn, run_id, as_of, metastore_id). compute_precision_recall_f1 expects each domain to carry a `members` list, so the reader MUST group members by domain_id and attach it.

BUILD A — READER (offline-testable, INJECTED fetcher): add read_latest_run(fetch, metastore_id) to eval_harness.py taking an injected row-fetcher (NO warehouse/SDK import in the wheel). Pull the latest run_id's genie_ont_domains + genie_ont_members rows (metastore-scoped), group members -> domain["members"], return (domain_rows, member_rows). Degrade-not-hang: empty/failed fetch -> empty rows.

BUILD B — ENTRYPOINT + GATE: add jobs/run_ontology_eval.py that reads the way jobs/run_ontology_materialize.py does (SQL over the warehouse via WorkspaceClient; OBO-default MV-D50), calls assemble_eval_report, prints the human summary, and PERSISTS the report as a JSON artifact to a known workspace/volume path (NO new table — MV-D49). With --baseline PATH: load a prior report JSON, run compare_reports, print deltas + regression_warnings, exit NON-ZERO when a metric regresses past a fixed tolerance (any structural-health worsening, or P/R/F drop > 0.01) — the gate.

BUILD C — GOLD REFERENCE (lights up P/R/F): add a small airline gold-taxonomy fixture + load_aligned_reference(path) returning the alignment-map shape compute_precision_recall_f1 already consumes. Offline test uses the fixture; live passes --reference PATH until §9/MV-D58 materializes it. Absent -> P/R/F N/A (degrade); other sections + structural-health gate still run.

HARD GUARDRAILS: READ-ONLY (no snapshot/apply writes, no governed-tag SET/UNSET, no manage_uc_tags, no web_search). NO new table/column/DDL (MV-D49) — report rides a JSON artifact. NO new dependency (MV-D45), uv.lock untouched. NO backend route, NO frontend. Wheel logic deterministic + offline (fetcher injected; LLM monitor injectable/skippable). FROZEN scorer: extend, never change assemble_eval_report/compare_reports outputs.

ACCEPTANCE (offline): test_ontology_eval_gate — (A) read_latest_run over a fake fetcher picks the latest run_id, groups members correctly, degrades to empty on fetcher error; (B) the gate exits non-zero when compare_reports shows a regression (P/R/F drop or orphan/singleton increase) and zero when clean; (C) with the gold fixture P/R/F populate + match hand values, absent -> N/A + structural gate still works. ./scripts/test.sh + wheel suite green; uv.lock/package-lock.json untouched.

WORKFLOW: reader -> entrypoint+gate -> gold fixture+loader (+tests). Do NOT deploy/run the job. When offline-green, STOP + report diff + test summary; a human runs the deploy-gated live baseline capture.
```

---

## After the run (human-gated)

```bash
git diff --stat            # expect: ontology/eval_harness.py, jobs/run_ontology_eval.py,
                           #   gold fixture + tests/unit/test_ontology_eval_gate.py
./scripts/test.sh          # re-confirm green
uv lock --check            # UNCHANGED (MV-D45)
git add packages/genie-space-optimizer && git commit -m "ontology(MV-D59): eval harness reader + runnable regression gate (+ optional gold reference)"
git push origin ontology
```

Then **you** run the deploy-gated **baseline capture**:

```bash
SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update    # wheel-only change
# Run run_ontology_eval against the materialized estate (serverless_stable_6t92c3_catalog)
#   as the OBO admin. §9 alignment (MV-D58) is still spec-only, so P/R/F report N/A —
#   structural health + LLM sanity + spot-review render. SAVE this report JSON as the
#   BASELINE. Every later signal/threshold change re-runs with --baseline <that file>;
#   a non-zero exit means the change regressed quality (§10's gate role). This is the
#   scoreboard P6 (Signal Authority) is gated behind.
```
