# Ontology — Evaluation & trust harness Goal-Mode driver (MV-D59)

Copy-paste launcher for building the **offline evaluation & trust harness** (MV-D59)
for the ontology curation engine, with a long-running agent (Claude Code / Cursor Goal
Mode). Run it on the **`ontology`** branch, on top of the shipped signals-first stages
(1 → 4.1x — all LANDED + deploy-verified) and the metastore re-grain (MV-D49). This is
**future work** — it is **not** a lane in the current parallel batch, so it carries **no
lane header**; build it as a single self-contained slice.

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

## Driver prompt (paste verbatim)

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

## After the run (human-gated)

Deploy `SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update` (fevm-serverless) and run the
harness against a materialized estate (e.g. `serverless_stable_6t92c3_catalog`) as the OBO
admin. Because §9 industry alignment (MV-D58) is still spec-only, the **first** live pass
exercises the **degraded** path — structural health, the LLM sanity monitor, and the spot-review
queue render, and precision/recall/F report **N/A** with a plain reason. Full
discovered-vs-reference scoring lights up once §9 materializes the aligned reference. Capture
the report as the **baseline** so subsequent signal/threshold changes can be gated against it
(§10's stated role). Record the live pass in the playbook's Ontology Build Queue / live-evidence
log (human edit — the agent never touches the playbook).
