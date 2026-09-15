# Ontology — Curation Redesign · Stage 4.1e build spec (the real Step-1 timeout fix: bound LLM naming to gate-survivors + concurrency + timeout headroom)

Follow-up to **Stage 4.1d Step 1**. Step 1 bounded batch **page drafting** to a capped
super-sure set — correct, but it did **not** fix the timeout. The 4.1d-Step-1 deploy-verify
(run `737477992104319`) still ran **60.2 min → TIMEDOUT**, identical to 4.1c. Cutting page
drafts 641→50 barely moved the wall, which proves **page drafting was never the dominant
cost**. The real hog is **LLM domain naming**: `cluster._cluster_name` calls the injected
namer once per **non-tag-bound ("create") cluster** — and it runs **inside `cluster.cluster`,
before the legitimacy / diffuseness gate** (`rank.mark_surfaced`). So every one of the
hundreds of *noise* domains that the gate will later prune still costs one **slow
`databricks-claude-opus-4-6`** call. This stage moves naming **after** the gate so only the
~dozen **surfaced** domains are LLM-named; everything else keeps its deterministic anchor
name. Adds bounded concurrency to the remaining LLM loops and a modest task-timeout bump as
insurance.

- **Spec umbrella:** `ontology-curation-redesign-build.md` (§8 Pages; §11–§14). This is the
  Step-1 completion — MV-D66's "bounded batch LLM" principle, **extended to naming**.
- **Decisions:** `mv-advisor-playbook.md` — new **MV-D67** (bounded naming). Honor
  MV-D43/D49/D50/D57/D65/D66.
- **Grain / auth:** Metastore (MV-D49); batch `run_as` (MV-D50); identity injected (MV-D65).

## 1. Problem — naming, not drafting, blows the 60-min task cap

`materialize.run_materialize` order today (materialize.py):
1. `cluster.cluster(..., namer=namer)` — **names every create-cluster via the LLM** (462).
2. `build_domain_rows` → MERGE domains/members (466–471).
3. `mine_pages(...)` → MERGE pages (487–500) — **already bounded** to ≤50 drafts (4.1d).
4. `rank.score_proposals` + `rank.mark_surfaced` — compute `surfaced` (517–527).
5. re-MERGE domains/pages with `score` + `surfaced` (529).

Naming (step 1) fires for **all** non-tag-bound clusters — hundreds on the airline estate —
each a sequential opus call, before step 4 decides only a handful survive. Tag-bound
(reuse/reassign) clusters already skip the LLM (`_cluster_name` returns the governed
vocabulary). Evidence: 4.1c (641 drafts + all names) and 4.1d-Step-1 (50 drafts + all names)
**both** hit exactly the 3600s task cap → the constant is the naming/ER tail, not drafts.

## 2. Goals

- **4.1e-namegate:** LLM-name **only domains that pass the gate** (`surfaced=true`,
  non-tag-bound). Cluster with deterministic anchor names first; LLM-rename survivors after
  `mark_surfaced`, before the final re-MERGE. Naming LLM calls drop from *O(raw clusters)* to
  *O(surfaced domains)* — hundreds → dozens.
- **4.1e-concurrency:** run the remaining bounded LLM loops — the surfaced-domain renames and
  the ≤N super-sure page drafts — under a **small bounded worker pool** (stdlib
  `concurrent.futures`, default 4), cutting wall-clock without unbounded fan-out.
- **4.1e-headroom:** raise the `ontology_materialize` task `timeout_seconds` 3600 → **5400**
  (90 min) as insurance — the goal remains completing well under **3600**.

Non-goals: no change to *which* domains surface (gate logic untouched); ER adjudication stays
as-is (near-ties are few — concurrency covers it); no deterministic-only naming or app rename
route (that was the declined alternative); Genie-history stays dormant.

## 3. Design

### 3.1 Defer + gate naming (`materialize.run_materialize` + `cluster`)

- **Cluster deterministically.** Call `cluster.cluster(..., namer=None)` so `_cluster_name`
  yields the deterministic anchor name (tag-bound clusters still take the governed
  vocabulary). No LLM in the clustering pass.
- **Rename survivors after the gate.** After `rank.mark_surfaced` marks `surfaced` on
  `domain_rows`, and **before** the final re-MERGE (materialize.py:529), call a new
  `cluster.rename_surfaced(proposals, domain_rows, *, namer, company, oracle, max_workers)`:
  for each `domain_row` with `surfaced is True` whose proposal is **non-tag-bound**, call the
  namer on the proposal's sorted member identifiers + anchor + company, validate with the
  existing `name_leaks` / LeakageOracle, and update `domain_row["name"]` (and the proposal)
  on success; on empty/raise keep the deterministic name (MV-D43). Tag-bound and
  non-surfaced rows are never LLM-named. Runs under a bounded pool (§3.2). The final re-MERGE
  then persists the upgraded names — the first domain MERGE (step 2) already wrote the
  deterministic names, so a rename failure never leaves a blank.
- Keep the `namer` param on `run_materialize` (injected, MV-D65); it is now consumed by
  `rename_surfaced`, not `cluster.cluster`. `namer=None` ⇒ fully deterministic names
  (offline default).

### 3.2 Bounded concurrency (`cluster.rename_surfaced`, `pages.mine_pages` Pass C)

Wrap the two LLM-bound loops in a `ThreadPoolExecutor(max_workers=k)` (k default 4, a config
knob): the surfaced renames, and the ≤`page_autodraft_max_pages` super-sure drafts (4.1d Pass
C). Selection/ordering stays deterministic (compute the eligible/selected set first, then
fan the drafter/namer calls out); results are collected and applied deterministically so the
written snapshot is identical regardless of worker count. Per-item errors are captured and
degrade to the deterministic body/name (MV-D43). No new dependency (stdlib only). `k=1`
reproduces sequential behavior for tests.

### 3.3 Timeout headroom (`databricks.yml`)

Bump the `ontology_materialize` task `timeout_seconds` from 3600 to **5400**. This is
insurance, not the fix — with §3.1 the naming tail collapses and the run should finish well
under 3600.

## 4. Data-model impact
None. No new tables/columns/DDL. `max_workers` knobs are settings/job-params/widgets
(MV-D57 pattern), in-code default 4. `domain_row["name"]` already exists.

## 5. Guardrails / invariants
- **Naming is gate-bounded.** The LLM namer is invoked at most once per **surfaced,
  non-tag-bound** domain — never per raw cluster. Non-surfaced/pruned domains cost **zero**
  LLM.
- **Deterministic-first.** `namer=None` / `drafter=None` ⇒ a fully deterministic run
  (anchor names, stub bodies) — offline tests need no LLM. Snapshot is worker-count-invariant.
- **Bounded fan-out.** All LLM loops run under a small fixed worker cap; no unbounded threads.
- **Reuse, don't fork.** Same namer + `name_leaks`/oracle; same page gates; tag-bound naming
  and the gate logic are untouched.
- **Degrade-not-hang (MV-D43).** A missing/raising namer or drafter ⇒ deterministic
  name/stub; the run still `succeeded`.
- **Grain + identity.** Metastore keys; batch `run_as`; identity injected, never resolved in
  the wheel. Exact pins; no new dependency.

## 6. Acceptance (offline — the agent's job; stops before deploy)
- `test_ontology_cluster`: `cluster(..., namer=None)` yields deterministic anchor names (no
  LLM). `rename_surfaced` with a stub namer renames **only** surfaced non-tag-bound proposals;
  tag-bound and non-surfaced keep their names; a raising namer keeps the deterministic name;
  the namer is invoked exactly `#surfaced-non-tag-bound` times; `max_workers=1` and `k>1`
  produce identical names.
- `test_ontology_pages`: Pass-C concurrency drafts the same ≤N set as sequential, never more
  than `max_workers` in flight (instrumented stub), snapshot identical to `k=1`.
- `test_ontology_materialize`: end-to-end, the injected namer is called `O(#surfaced)` times,
  not `O(#raw clusters)`; deterministic names are written first, upgraded on re-MERGE.
- `./scripts/test.sh` + wheel unit suite green.

## 7. Deploy-verify (human-gated, after offline green)
Deploy (`SKIP_FRONTEND_BUILD=1 ./scripts/deploy.sh --update`, fevm-serverless), trigger scoped
to `["serverless_stable_6t92c3_catalog"]`, then confirm:
- Job **completes — ideally well under 3600s** (the 5400s cap is headroom, not the target).
- Snapshot **written** (a `succeeded` run row; `genie_ont_domains`/`_pages` refreshed).
- `certify=true` count `> 0` (4.1d deterministic certify — the ~591 corroborated Pages).
- `body_source="llm_auto"` count in `(0, page_autodraft_max_pages]`.
- Surfaced domains carry **LLM names**; pruned/non-surfaced never triggered a name call
  (spot-check run duration ≈ minutes, not the hour-long tail).
- No regression: Taxonomy `> 0`, **0** surfaced-but-unattached.

## 8. Risks / mitigations
- **Concurrency reorders/duplicates writes** → fan out only the pure LLM calls; compute
  selection and apply results deterministically; `k=1` parity test pins it.
- **A surfaced domain still LLM-names slowly** → dozens of calls under a worker pool is
  minutes; 5400s headroom absorbs variance.
- **Opus latency dominates even for dozens** → out of scope here; if it recurs, point the
  batch enrichers at a faster endpoint (a config change, no code).
