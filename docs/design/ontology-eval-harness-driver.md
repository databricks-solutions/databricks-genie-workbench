# Ontology — Evaluation & trust harness Goal-Mode driver (MV-D59)

## ✅ STATUS (2026-09-15) — the offline harness is BUILT; the build prompt below is HISTORICAL

Read this first — it supersedes the lane header and build prompt that follow.

- **The offline harness is BUILT + landed (commit `7120a6df`, Sep 8), offline-green.**
  `packages/genie-space-optimizer/src/genie_space_optimizer/ontology/eval_harness.py`
  already implements all four builds — `compute_precision_recall_f1` (A),
  `compute_structural_health` (B), `run_llm_sanity_monitor` (C, injectable),
  `build_spot_review_queue` (D) — plus `assemble_eval_report`, `compare_reports`
  (the before/after gate), and `report_to_dict` / `dict_to_report`. Tested in
  `test_ontology_eval_harness.py`. **Do NOT re-run the build prompt below — it would
  re-create an existing module.** The Wave-2 parallel-lane header is historical (that
  wave merged).
- **§9 industry alignment (MV-D58) is now BUILT + deploy-verified (commit `de65f480`,
  2026-09-15)** — it emits `aligned_reference` in *this harness's exact shape*
  (`alignment._aligned_reference`, `{domains, alignments}`). So the driver's earlier
  "§9 is still spec-only → the first live pass is degraded (P/R/F = N/A)" language is
  **stale**: full discovered-vs-reference scoring is now possible.
- **The ONE genuine remaining gap = a live entrypoint + the aligned-reference glue.**
  The harness is a pure *library*; nothing invokes it on a real run yet (no job,
  reader, or route calls `assemble_eval_report`). And §9's `aligned_reference` is
  **report/log-only — it is NOT persisted** (only the per-domain
  `evidence.rank.alignment` relations land in `genie_ont_domains`). So a live P/R/F
  pass must first *obtain* the aligned reference by one of:
  1. **capture the materialize run report** (it already carries `aligned_reference`), or
  2. **reconstruct it read-only** — pull each surfaced domain's
     `evidence.rank.alignment` (`domain_id` → `reference_id`) from `genie_ont_domains`
     and join `alignment.load_reference_model(<model>)` for the reference `domains`
     (names + members), then call `compute_precision_recall_f1`.
  Option 2 stays inside the harness's read-only/no-DDL contract (MV-D49/D50). This
  small glue (a thin reader + a run entrypoint) is the only thing left to write; the
  scoring logic itself is done.

**So "up to date" means:** the *build* is complete; what remains is the **deploy-gated
live baseline pass** — wire the thin reader/entrypoint, run it against the airline
estate (which now has §9's persisted correspondences), and capture the first real
P/R/F + structural-health report as the regression baseline (§10's stated role).

---

## ⚙️ Parallel-build lane header — Lane 1 of Wave 2 (HISTORICAL — harness already merged)

You run in an **isolated git worktree** off the `ontology` HEAD (`isolation: worktree`,
`worktree.baseRef: "head"`). Two sibling lanes edit the repo concurrently. This lane owns a
whole disjoint subtree (the wheel eval module), so merges are conflict-free — keep it that way:

- **OWNS (create/edit freely):**
  `packages/genie-space-optimizer/src/genie_space_optimizer/ontology/eval_harness.py` (new),
  `packages/genie-space-optimizer/tests/unit/test_ontology_eval_harness.py` (new).
- **SHARED:** none. Keep the typed report a **local dataclass in `eval_harness.py`** — do not
  edit any existing model surface.
- **OFF-LIMITS (do NOT touch):** all of `backend/`, all of `frontend/`, **every other wheel
  module** (`pages.py`/`rank.py`/`cluster.py`/`er.py`/`materialize.py`/`ddl.py` — reuse the
  wheel-native LLM client **by import only**, MV-D65), and `docs/design/mv-advisor-playbook.md`.
- **MERGE-ORDER:** independent — no cross-lane file overlap; merge in any order (nominally first).
- **Launch:** via the `ontology-lane-builder` subagent — see `ontology-wave2-launcher.md`.

---

Copy-paste launcher for building the **offline evaluation & trust harness** (MV-D59)
for the ontology curation engine, with a long-running agent (Claude Code / Cursor Goal
Mode). Run it on the **`ontology`** branch, on top of the shipped signals-first stages
(1 → 4.1x — all LANDED + deploy-verified) and the metastore re-grain (MV-D49). It is a
**self-contained, wheel-only slice** (Lane 1 above); it competes with no sibling for files.

The harness is a **read-only, offline scorer**. It consumes a materialized run's snapshot
tables and emits one comparable report per run. Its purpose is to **gate every subsequent
signal/threshold change** — you run it before and after any change to the Stage-1/2/3
signal weights or gate thresholds and confirm the change did not regress discovered-domain
quality or structural health. It writes nothing back and applies no governed tags.

- **Spec (source of truth):** `docs/design/ontology-curation-redesign-build.md` **§10**
  (Evaluation & trust harness · MV-D59). Also honor §4 (architecture — the harness scores
  each run against the **aligned reference**), §11 (data-model impact — prefer the existing
  `evidence` JSON; no DDL churn), §12 (guardrails / invariants), §13 (phasing — offline code
  + tests first, then a deploy-gated live run), and §14 (Definition of Done).
- **Decisions register:** `docs/design/mv-advisor-playbook.md` — **MV-D59** (this harness).
  Honor the decisions §10 depends on **as stated in build.md** (do **not** open the playbook
  to build this driver): **MV-D58** (§9 industry-reference alignment — supplies the *aligned
  reference* the precision/recall/F is measured against), **MV-D56** (every stage attaches
  `evidence`; the harness reads that evidence, never re-derives it), the shipped **MV-D65**
  (single wheel-native LLM client with injected identity — the sanity monitor **reuses** it,
  adding no dependency), and the inherited invariants **MV-D45** (no new dependency,
  `uv.lock` untouched), **MV-D49** (metastore grain, no DDL/column change), **MV-D50**
  (OBO-default reads), **MV-D43** (degrade-not-hang).
- **Design context:** `docs/design/ontology-engine-architecture.md` (the discover → rank →
  align → merge → serve pipeline whose output this harness scores).
- **Project rules:** `AGENTS.md`.

The harness is **pure offline wheel logic** — deterministic except for the optional LLM
sanity monitor, which must be injectable and skippable and never feeds the deterministic
metrics. Acceptance is **offline** (pytest on fixture runs, LLM monitor mocked); running the
harness against a live materialized estate is **deploy-gated** and human-run. Adds **no
dependency** (MV-D45), so `uv.lock` is untouched.

---

## OWNS / OFF-LIMITS

- **OWNS (create / edit freely):**
  - `packages/genie-space-optimizer/src/genie_space_optimizer/ontology/eval_harness.py`
    — **new module.** Pure functions that take a materialized run's rows and return a typed
    report; a thin read-only reader that pulls those rows from the snapshot tables; the
    injectable LLM-sanity hook.
  - `packages/genie-space-optimizer/tests/unit/test_ontology_eval_harness.py` — **new**
    unit tests over fixture runs (LLM monitor mocked, no I/O).
- **SHARED — append only, never reflow existing lines:** if a typed report model is worth
  keeping alongside the other ontology models, append it to the wheel's existing ontology
  model surface; do **not** touch existing model definitions. Prefer keeping the report a
  local dataclass in `eval_harness.py` if that avoids a shared edit.
- **OFF-LIMITS (do NOT touch):** all of `backend/`, all of `frontend/`, the engine stage
  modules (`grouping`/`subdomains`/`gates`/`rank`/`pages`/`align` logic — the harness
  **scores** their output, it must not change how it is produced), `ontology/ddl.py` and
  `ontology/materialize.py` (**no new table, column, or DDL** — MV-D49), and
  **`docs/design/mv-advisor-playbook.md`** (never edit the playbook).

---

## Driver prompt (HISTORICAL — already executed in `7120a6df`; do NOT re-run)

> This prompt built the now-landed `eval_harness.py`. It is kept for provenance only.
> Re-running it would re-create an existing module. For the remaining work, see
> **STATUS** and **After the run** above (the thin reader/entrypoint + live baseline pass).

```text
GOAL: Build the OFFLINE evaluation & trust harness (MV-D59) for the ontology curation engine.
It reads ONE materialized run's snapshot output and emits ONE comparable report with four
sections: (1) gold-standard precision/recall/F of discovered domains vs the aligned reference,
(2) structural health, (3) a cheap reference-free LLM sanity monitor, (4) a human spot-review
queue. Read-only. Deterministic (except the LLM monitor, which is injectable, skippable, and
never feeds the deterministic metrics). Purpose: gate every subsequent signal/threshold
change. Offline code + green tests; STOP before deploy. Branch: ontology.

SPEC (source of truth): docs/design/ontology-curation-redesign-build.md §10 (MV-D59); honor
§4, §11, §12, §13, §14. DECISIONS: mv-advisor-playbook.md MV-D59; honor MV-D58 (aligned
reference), MV-D56 (evidence is read, not re-derived), MV-D65 (reuse the wheel-native LLM
client for the sanity monitor — NO new dependency), and inherited MV-D45/D49/D50/D43. RULES:
AGENTS.md. Read §10 and the honored sections first. §9 industry alignment (MV-D58) is itself
still spec-only — when its aligned-reference output is absent, DEGRADE (see BUILD A).

CONTEXT: The engine's discover -> rank -> align -> merge -> serve pipeline materializes
metastore-keyed snapshot tables for domains, sub-domains, pages, and (optionally, when §9
lands) alignment relations. Every row carries an `evidence` JSON column (MV-D56). This harness
consumes those rows read-only; it never re-runs the engine and never writes back.

BUILD A — discovered-vs-reference precision/recall/F (deterministic): given the run's
discovered domain set and the ALIGNED REFERENCE set (the §9/MV-D58 industry-reference
alignment relations, or a supplied gold reference file for offline fixtures), compute
set-level precision, recall, and F over domain matches, using the alignment relations as the
match map (never re-deriving them). Report per-domain match status so a reviewer can see which
discovered domains matched, which reference domains were missed, and which discovered domains
had no reference. When NO aligned reference is available (§9 not yet materialized, or no gold
supplied), mark P/R/F as N/A with a plain reason and still emit the other three sections
(degrade-not-hang, MV-D43) — never raise.

BUILD B — structural health (deterministic): from the discovered domain/sub-domain/page
hierarchy compute the metrics the Vibe reference repo itself reports — singleton rate, orphan
rate, tree depth, and branching factor — plus the counts they are computed from. These are the
regression signals the gate compares across runs. Pure counting over the snapshot rows; no
LLM.

BUILD C — reference-free LLM sanity monitor (cheap, injectable, report-only): a lightweight,
reference-free check that flags obviously-wrong groupings/labels using the SHIPPED wheel-native
LLM client (MV-D65) with its injected identity — NO new dependency (MV-D45). The LLM client is
passed in (injected), so tests mock it and it can be skipped entirely (monitor emits
"skipped"). Its output is advisory: it appears in the report but MUST NOT alter the BUILD A/B
numbers or the gate verdict.

BUILD D — human spot-review queue (deterministic): emit a bounded, deterministically-sampled
queue of assignments/domains for a human to eyeball — each queue item carries its plain
`evidence`-derived reason and honest confidence (read from evidence, MV-D56), so the queue is a
review artifact, not a new judgement. Fixed sampling (seeded) so the queue is reproducible
across identical runs.

REPORT + GATE: assemble A-D into one typed, serializable report (a stable dict / dataclass) so
two runs are directly diff-able. Provide a comparator that, given a BEFORE and AFTER report,
reports deltas in P/R/F and structural-health metrics — this is the object a human/CI inspects
to confirm a signal/threshold change did not regress quality. The harness itself renders a
verdict input; it does NOT auto-block.

HARD GUARDRAILS: READ-ONLY — no writes to any snapshot/apply table, no governed-tag SET/UNSET,
no manage_uc_tags, no web_search. Additive — NO new table/column/DDL (MV-D49); report fields
ride the return object, not the schema. NO backend route, NO frontend. NO new dependency
(MV-D45) — reuse the wheel LLM client (MV-D65) for the ONLY LLM call. OBO-default reads
(MV-D50). Deterministic + offline except the injectable LLM monitor; fixed seed for sampling.
Degrade-not-hang (MV-D43) — a missing reference, missing evidence field, or an LLM error yields
a marked-partial report, never a crash.

ACCEPTANCE (offline): test_ontology_eval_harness — (A) on a fixture run WITH an aligned
reference, P/R/F match hand-computed values and per-domain match status is correct; WITHOUT a
reference, P/R/F are N/A and the other sections still render; (B) singleton/orphan rate, depth,
and branching factor match hand-computed values on a fixture hierarchy; (C) with a MOCKED LLM
client the sanity section is populated and the deterministic A/B numbers are byte-identical to a
run with the monitor skipped (proving report-only); (D) the spot-review queue is bounded,
seeded-reproducible across two identical runs, and every item carries its evidence reason +
confidence; (comparator) BEFORE/AFTER deltas are computed correctly. `./scripts/test.sh` +
the wheel suite green; `uv.lock` / `package-lock.json` untouched (MV-D45).

WORKFLOW: branch ontology. Do NOT deploy or run the job. When offline-green, STOP and report
the diff + test summary; a human runs the deploy-gated live pass.
```

---

## After the run (human-gated) — UPDATED 2026-09-15

**§9 is now landed + deploy-verified**, so the first live pass is **no longer degraded** —
full precision/recall/F is available once the aligned reference is obtained (see STATUS
option 1/2). The remaining live-pass steps:

1. Write the thin glue (harness's read-only contract): a reader that reconstructs
   `aligned_reference` from persisted `evidence.rank.alignment` + `load_reference_model`
   (STATUS option 2), or capture it from the materialize run report (option 1); and a
   small entrypoint that pulls the domain/sub-domain/page rows and calls
   `assemble_eval_report`.
2. Deploy `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless) and run
   the harness against the materialized estate (`serverless_stable_6t92c3_catalog`,
   `reference_model=airline`) as the OBO admin. The airline snapshot already carries §9's
   correspondences (16/16 surfaced domains, verified 2026-09-15), so P/R/F, structural
   health, the LLM sanity monitor, and the spot-review queue all render.
3. Capture the report as the **baseline** so subsequent signal/threshold changes can be
   gated against it via `compare_reports` (§10's stated role). Record the live pass in the
   playbook's Ontology Build Queue / live-evidence log (human edit — the agent never
   touches the playbook).
