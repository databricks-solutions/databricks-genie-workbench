# Ontology — Eval-harness LIVE-WIRING Goal-Mode driver (MV-D59, follow-on)

## Positioning (READ FIRST)

The offline harness is **already BUILT + landed** (`eval_harness.py`, commit
`7120a6df`): `assemble_eval_report` / `compute_precision_recall_f1` /
`compute_structural_health` / `run_llm_sanity_monitor` / `build_spot_review_queue` /
`compare_reports` / `report_to_dict` — 25 tests green. **§9 alignment is BUILT +
deploy-verified** (`de65f480`): each run produces `result.aligned_reference` in the
harness's exact shape (`{domains, alignments}`), but that object is **report/log-only,
not persisted**.

This follow-on wires the built harness into the live run so we capture a **queryable,
diff-able baseline** the gate (`compare_reports`) can use. It **supersedes** the
library-era eval-harness driver's "no materialize.py / no DDL" guardrails for THIS
task only: a *minimal additive* materialize hook + one *additive* table are explicitly
in scope (MV-D49 additive-only still holds).

**Decision of record (owner, 2026-09-15):** invoke via a **post-materialize hook**
using the in-memory `aligned_reference` (no reconstruction), and persist to an
**additive `genie_ont_eval` table** (metastore-keyed, CDF-on).

---

## Spec / decisions

- **Spec:** `ontology-curation-redesign-build.md` §10 (MV-D59), §4 (harness scores each
  run against the aligned reference), §11 (additive tables OK — metastore-keyed, CDF-on,
  no retired cols), §12 guardrails, §14 DoD. **Also** the STATUS/After-the-run sections
  of `ontology-eval-harness-driver.md` (the library build this completes).
- **Decisions:** MV-D59 (harness), MV-D58 (supplies `aligned_reference`), MV-D56
  (evidence is read, not re-derived), MV-D65 (reuse the wheel-native LLM client — no new
  dep), MV-D43 (degrade-not-hang), MV-D45 (no dep), MV-D49 (metastore grain; additive
  DDL), MV-D50 (batch reads as the run identity). Do NOT edit the playbook.

---

## OWNS / OFF-LIMITS

- **OWNS:** `ontology/ddl.py` (add the `genie_ont_eval` table + constant + register it),
  `ontology/materialize.py` (the post-run eval hook only), `eval_harness.py` (only if a
  tiny `report → eval_row` helper is cleaner there), and the two test files
  (`test_ontology_eval_harness.py`, `test_ontology_materialize.py`).
- **OFF-LIMITS:** all `backend/` + `frontend/`; the engine stage logic
  (`cluster`/`rank`/`pages`/`er`/`alignment`) — the harness SCORES their output, it must
  not change it; the apply path (`apply.py`); `uv.lock` / `package-lock.json`.

---

## Driver prompt (paste verbatim)

```text
GOAL: Wire the already-built offline eval harness (MV-D59) into the live materialize run
so each run persists ONE queryable EvalReport the gate can diff. Post-materialize HOOK
using the in-memory §9 aligned_reference (no reconstruction) + a NEW additive table
genie_ont_eval. Read-only w.r.t. the estate (only writes the new table). Offline code +
green tests; STOP before deploy. Branch: ontology.

SPEC: ontology-curation-redesign-build.md §10 (MV-D59), §4/§11/§12/§14; and the
STATUS/After-the-run of ontology-eval-harness-driver.md. DECISIONS: MV-D59, MV-D58
(aligned_reference), MV-D56, MV-D65 (reuse wheel LLM client, NO new dep), MV-D43/D45/
D49/D50. RULES: AGENTS.md. Read §10 first. eval_harness.py is BUILT — reuse it, do NOT
re-implement scoring.

CONTEXT: materialize.py already: runs align() (result.aligned_reference exists in memory
when industry_reference is set; else {}), builds expanded[domain_rows]/[member_rows], and
MERGEs genie_ont_graph_snapshot as a metastore-keyed single-row-per-run snapshot right
before writing the genie_ont_runs row. Mirror that snapshot table + merge exactly.

BUILD A — DDL (ddl.py): add genie_ont_eval mirroring genie_ont_graph_snapshot —
metastore_id (derived PK, one per metastore), workspace_id (not a key), report STRING
(report_to_dict JSON), precision/recall/f1 DOUBLE (NULL when no aligned reference),
singleton_rate/orphan_rate DOUBLE, max_depth INT, branching_factor DOUBLE, run_id STRING
(FK to genie_ont_runs.run_id), as_of TIMESTAMP; USING DELTA + CDF on. Add TABLE_ONT_EVAL
constant + EVAL_KEYS=["metastore_id"] and register it in the Phase-3 table list.

BUILD B — hook (materialize.py, right after the graph_snapshot MERGE, additive-last like
ranking/layout): call eval_harness.assemble_eval_report(run_id, metastore_id, as_of,
expanded[domain_rows], expanded[member_rows], aligned_reference=(aligned_reference or
None), llm_client=<the wheel-native LLM client if the run already built one, else None>).
Serialize with report_to_dict; build ONE eval_row (top-level metrics lifted from the
report; NULLs when P/R/F is N/A); writer.merge(TABLE_ONT_EVAL, [eval_row], EVAL_KEYS,
metastore_id). Runs EVERY materialize (alignment off ⇒ aligned_reference None ⇒ P/R/F
NULL, structural health still computed). MV-D43: a harness/merge error records a failed/
empty eval row (or logs) and NEVER corrupts the snapshots already committed or fails the
run.

BUILD C — tests: (materialize) a fixture run WITH aligned_reference writes an eval_row
with correct precision/recall/f1 + structural metrics; WITHOUT it, P/R/F are NULL and
structural metrics still present; a boobytrapped assemble_eval_report ⇒ run still SUCCESS,
eval marked failed, other snapshots intact. (harness) any tiny report→row helper is
unit-tested. Assert genie_ont_eval is registered in the table list.

HARD GUARDRAILS: additive ONLY — new table + hook; touch NO existing column/table/grain
(MV-D49); read-only w.r.t. the estate (only genie_ont_eval is written). NO new dependency
(MV-D45) — reuse the wheel LLM client (MV-D65) or pass None. NO backend/frontend edit, NO
apply-path, NO governed-tag write, no manage_uc_tags, no web_search. Deterministic except
the injectable LLM monitor (report-only; never changes P/R/F or structural numbers). Off
by default stays byte-identical for the domain/page snapshots (the eval row is an additive
orthogonal write). Degrade-not-hang (MV-D43). Do NOT edit the playbook.

ACCEPTANCE (offline): ./scripts/test.sh + wheel suite green incl. the new materialize
tests; genie_ont_eval registered; uv.lock/package-lock.json untouched.

WORKFLOW: branch ontology. Do NOT deploy or run the job. When offline-green, STOP and
report the diff + test summary; a human runs the deploy-gated baseline pass.
```

---

## After the run (human-gated) — capture the baseline

Deploy `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless), trigger
the materialize job scoped to `["serverless_stable_6t92c3_catalog"]` with
`industry_alignment_enabled=true`, `industry_alignment_reference_model=airline`, then
query `genie_ont_eval` for the row: confirm non-NULL precision/recall/f1 (§9's 16/16
correspondences feed BUILD A), structural-health metrics populated, and the report JSON
round-trips via `dict_to_report`. Record this as the **baseline**; future signal/threshold
changes are gated by `compare_reports(before, after)` deltas. (Human logs it in the
playbook Ontology Build Queue — the agent never edits the playbook.)
