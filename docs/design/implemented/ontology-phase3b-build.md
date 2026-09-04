# Ontology — Phase 3b: domain / sub-domain clustering (build spec)

**Status:** build-ready (offline slice) · **Owner directive:** MV-D39 (in-job
Leiden via `leidenalg` over `python-igraph` — multiplex layers, soft-seeded
(`initial_membership`) with human-adjudicated reassignment, CPM objective),
inheriting MV-D35 / MV-D37 / MV-D40 / MV-D45 (see `mv-advisor-playbook.md`, Prompt
17e). **Design source of truth:** `ontology-engine-architecture.md` §5 — the **L4
domain / sub-domain clustering engine** subsection (the four building blocks: fuse
edges → soft-seeded detection → recursive split → centrality + naming + reuse /
create / reassign binding). This doc is a
*buildable slice* of that design, not a new design. **Builds on:**
`ontology-phase3a-build.md` (the completed signal graph + ER identity map, already
shipped on the `ontology` branch).

> **⚠ Grain note (MV-D49, added after this phase shipped):** the code described below
> keys `genie_ont_domains` / `genie_ont_members` by `workspace_id`. Per **MV-D49** the
> correct grain is the **metastore** (Domains/Sub-Domains are metastore-scoped governed
> tags). The dedicated re-grain phase (`ontology-regrain-build.md`) reconciles this
> shipped code to `metastore_id` (with `workspace_id` demoted to provenance) **before**
> 17f. Read `workspace_id` below as "the scope key" — the re-grain swaps it for
> `metastore_id` without changing this phase's clustering logic.

This is the **fourth** Goal-Mode deliverable and the **first phase to populate the
proposal tables**. It is deliberately narrow: **cluster the canonical entities from
17d's identity map into a two-level Domain → Sub-Domain tree, bind each cluster to a
REUSE-vs-CREATE governed-tag decision, and emit those as proposal rows into the
(already-DDL'd, still-empty) `genie_ont_domains` / `genie_ont_members` tables +
the Lakebase mirror.** It adds **no serving route and no frontend** — the `17.0d`
Domain draft is *served* in 17g, not here — and it writes **proposal rows only, never
`SET TAG`**.

> **The one-line contract:** Phase 3a decided *which entities are the same*. Phase 3b
> decides *which entities belong together* — it runs community detection over the
> deduped graph to propose Domains and Sub-Domains (with evidence + a reuse-or-create
> tag decision), writing them as proposals to Delta + the mirror. Nothing is served
> to the UI and nothing is applied to Unity Catalog until later phases.

---

## 1. Scope

### In (Phase 3b)

- **L4 clustering engine** (`ontology/cluster.py`) over the fused L2 graph (built in
  17d) and the 17d identity map:
  1. **Assemble weighted layers (multiplex)** — weights are *not* equal: `lineage`
     is the backbone, `join_key`/`co_query` reinforce, `semantic_sim` is weak glue,
     **`tag_assignment` is the strongest prior**, `agent_scope` is a seed. Keep each
     signal as its own `leidenalg` **layer** (per-layer weights) rather than
     hand-collapsing to one scalar edge. (`cost` is a ranking weight for L6, **not**
     a clustering layer — excluded here.)
  2. **Soft-seeded community detection (Domains)** — **Leiden** via `leidenalg` (over
     `python-igraph`) on the multiplex layers with the **CPM** objective, **soft-seeded**
     so existing governed tags seed the partition via `initial_membership` — a
     **strong prior, not a hard pin** (don't re-derive `Finance` if a `Finance` tag
     already anchors assets, but *let strong graph evidence move a mis-tagged asset*),
     Agent scopes seed, catalog/schema is the thin-signal fallback prior. Leiden (not
     Louvain) so every community is guaranteed connected under the recursive split.
  3. **Recursive split (Sub-Domains)** — re-run detection at a finer CPM `γ` on
     each Domain's subgraph; the sub-communities become Sub-Domains.
  4. **Centrality + naming + binding** — betweenness/degree on the **lineage
     subgraph** picks each cluster's anchor (the MV-D35 headline chip); the **LLM
     names** the cluster (company prior + member identifiers only — Context Pack is
     Phase 4); each cluster is bound to 17d's identity verdict → **REUSE** an existing
     governed tag, **CREATE** a new one, or — when the soft seed moved assets *away*
     from an existing tag beyond `REASSIGN_MARGIN` — **REASSIGN** (a conflict proposal
     carrying the contradicted tag + evidence, for human adjudication in 17g; **never**
     an auto-switch) (MV-D37 `tag_decision`).
- **Determinism / idempotency** → clustering is run with a **fixed seed** (and a
  stable tie-break) and each `domain_id` is a **derived fingerprint of the cluster's
  sorted canonical members** (not a run-scoped id), so a re-run over the same graph
  yields the **same** domains and members — a hard requirement for the MV-D26
  suppression ledger to work in 17g.
- **Proposal emission (L7)** → MERGE Domain/Sub-Domain rows into `genie_ont_domains`
  and their asset membership into `genie_ont_members` (both already DDL'd empty in
  Phase 2), plus a `domain_count` on the run ledger. Proposals carry **evidence**
  (anchor, shared spine, co-query count, tag prior; plus a `conflict` block —
  contradicted tag + moved members + margin — when `tag_decision == "reassign"`) and
  the `tag_decision`.
- **Dependency (MV-D39)** → add **`leidenalg`** (exact-pinned) — which pulls
  **`python-igraph`** (also exact-pin it per the dependency policy) — to the GSO
  wheel and refresh `uv.lock` + `requirements.txt`. Both ship manylinux wheels, so
  they install on the serverless job (`environment_version 4`) via pip with no
  cluster library. This is the **first** dependency addition in the ontology track
  (17d was lock-untouched).

### Out (deferred — see §12)

L5 Page miners → `genie_ont_pages` (17f); L6 rank/trust + **serving the `17.0d/e`
drafts** + the `/drafts` route + frontend + the suppression *filter* at serve time
(17g); external context / Context Pack vocabulary prior (Phase 4); the consented
`SET TAG` apply (Phase 5). Phase 3b **proposes** the tree; it serves nothing and
writes nothing to UC governance.

---

## 2. Decisions honored (and which sleep in Phase 3b)

| Decision | Phase-3b posture |
|---|---|
| MV-D35 evidence-first trust | **Active** — every domain/sub-domain proposal carries its signals (anchor, spine, co-query, tag prior); nothing is asserted without evidence. |
| MV-D36 standalone admin-gated estate page | **Active** — unchanged. |
| MV-D37 governed-tag substrate + reuse / create / reassign | **Active** — each cluster emits a `tag_decision` (`reuse` an existing governed tag, `create` a new one, or `reassign` when soft-seeded clustering contradicts an existing tag beyond `REASSIGN_MARGIN`), bound to 17d's identity verdict. `reassign` is a **conflict proposal for human adjudication in 17g** (applied only in Phase 5), never an auto-switch. Still **no** `CREATE/SET/UNSET TAG` — the decision is a *proposal field*. |
| MV-D38 external enrichment | **Dormant** — cluster naming uses the company prior + member identifiers only; the Context Pack vocabulary prior is Phase 4. |
| MV-D39 in-job Leiden clustering | **Active — this is the phase.** Leiden via `leidenalg` (multiplex layers + soft-seeded `initial_membership`, CPM objective) coarse pass = Domains + recursive finer-`γ` split = Sub-Domains; soft-seed disagreements → `reassign` proposals; `leidenalg` + `python-igraph` become wheel dependencies. |
| MV-D40 Lakebase Search similarity | **Consumed, not extended** — clustering runs over 17d's identity map; **Lakebase Search stays NOT enabled** (the in-process path from 17d is untouched; 17e adds no similarity code). |
| MV-D41 nightly batch + on-demand | **Active** — clustering runs **inside** the existing `ontology_materialize` job (reuse `GSO_ONT_JOB_ID`); **no new job**. |
| MV-D42 catalog allowlist | **Active** — the allowlist already scoped the graph the clustering consumes. |
| MV-D43 degrade-not-hang | **Active** — if the naming LLM is unavailable, clusters get a deterministic fallback name (anchor-derived) and the run still succeeds; clustering itself is deterministic and offline. |
| MV-D44 enrichment OFF by default | **Active (trivially)** — no enrichment path. |
| MV-D45 minimal install footprint | **Active with one addition** — reuses the existing job/graph/identity machinery; the **only** new artifacts are one module + **two pinned dependencies** (`leidenalg` + `python-igraph`, both shipping manylinux wheels so they install on the serverless job via pip, no cluster library / system build). Still no net-new managed service. |
| MV-D46 / MV-D47 web search / MCP registry | **Dormant.** |

**Load-bearing consequence:** the mirror now carries **proposal rows** for the first
time. The Phase-1 taxonomy tree (`genie_ont_taxonomy_snapshot`, derived from
*existing* governed tags) is **unchanged and still what the taxonomy route serves**;
the new `genie_ont_domains`/`_members` proposals are a **separate** surface that stays
unserved until 17g. No route contract changes.

---

## 3. Subsystem layout (extends Phase 3a in the GSO wheel; no backend touch)

Batch-side only, in the **GSO wheel**. **No backend change, no route, no model, no
frontend** — the proposals are written to Delta/mirror and served later (17g).

```
packages/genie-space-optimizer/
  pyproject.toml            # MODIFIED — + leidenalg==<exact> + igraph==<exact> (MV-D39); the new deps
  src/genie_space_optimizer/
    ontology/
      cluster.py            # NEW — L4: multiplex layers → seeded Leiden (CPM) → recursive split → centrality + naming + reuse/create
      materialize.py        # MODIFIED — call cluster after ER; MERGE genie_ont_domains + genie_ont_members
      ddl.py                # MODIFIED — move domains/members out of the "never written" guard set (schema already present)
      graph.py              # (unchanged — 17d already emits the fused weighted heterograph cluster.py consumes)
uv.lock                     # MODIFIED — leidenalg + igraph locked (root workspace lock)
requirements.txt            # MODIFIED — regenerated from uv.lock (pip reference; databricksignored)
backend/tests/test_ontology_firewall.py   # MODIFIED — domains/members now allowed writes; pages/consents/suppressions still forbidden
```

**Reuse, do not fork:**

- `genie_space_optimizer/ontology/graph.py` (`build_signal_graph`) — the fused
  weighted heterograph (nodes + `lineage`/`join_key`/`co_query`/`tag_assignment`/
  `agent_scope`/`semantic_sim` edges) is the clustering input; do **not** rebuild
  it. `cluster.py` **projects** those edge kinds into `leidenalg` multiplex layers
  (one layer per kind, per-layer weight) — it does not collapse them to a scalar.
- `genie_space_optimizer/ontology/er.py` + the `genie_ont_identity` map — clustering
  runs over **canonical** refs (map members to their `canonical_id` before detection).
- `genie_space_optimizer/ontology/ddl.py` (`build_snapshot_merge_sql`,
  `ensure_ontology_tables`) — the idempotent MERGE + the already-present
  `genie_ont_domains`/`_members` DDL; no new DDL.
- `genie_space_optimizer/ontology/materialize.py` (`run_materialize`,
  `SparkSnapshotWriter.merge`) — the reader→transforms→MERGE orchestration
  clustering slots into (it already builds the graph at line ~164 and currently
  discards it; feed it to `cluster.py` instead).
- `backend/services/llm_utils.call_serving_endpoint` (+ `model_catalog.validate_chat_model`)
  — the **only** LLM path, for cluster naming only (lazy-imported + degrades, the
  17d `default_adjudicator` precedent).
- `optimization/mv_scoring` centrality/degree helpers if present, else a thin
  betweenness/degree util (the architecture marks this "new, thin").

---

## 4. Backend contracts (`backend/ontology/models.py`)

**No new API response model, no route.** Every Phase-1/2/3a model is FROZEN. The
proposals live in Delta + the mirror and are **not** exposed through any route until
17g. Internal wheel-side types only (dataclasses, not Pydantic API models):

```python
# genie_space_optimizer/ontology/cluster.py
@dataclass(frozen=True)
class DomainProposal:
    domain_id: str            # derived: sug_<fingerprint of sorted canonical members>
    parent_id: str | None     # None = domain; set = sub-domain (self-ref)
    name: str                 # LLM-named (or anchor-derived fallback)
    description: str
    tag_decision: Literal["reuse", "create", "reassign"]
    tag_key: str | None       # existing tag (reuse), the tag being moved-from (reassign),
                              # or the proposed key (create)
    tag_value: str | None     # sub-domain value in the Domain/Sub `/` convention
    evidence: dict            # {anchor, shared_spine, co_query_count, tag_prior, seed,
                              #  conflict?}  # conflict = {existing_tag, moved_members, margin};
                              #  present ONLY when tag_decision == "reassign"
    members: tuple[str, ...]  # canonical member refs (→ genie_ont_members rows)
```

`REASSIGN_MARGIN` is a module-level tunable constant (architecture §5 "honest gap").
`tag_decision == "reassign"` is a **proposal field only** — 17e writes proposal rows;
17g renders the conflict for a human to approve/dismiss (recorded in
`genie_ont_consents`/`genie_ont_suppressions`); the physical `UNSET/SET TAG` happens
only in the Phase-5 consented apply. Below the margin the binder stays conservative
and emits `reuse` (honour the existing tag), keeping the 17g review queue small.

These map 1:1 onto the **existing** `genie_ont_domains` columns (§7) and expand into
`genie_ont_members` rows. `score` is written as `NULL`/`0.0` here — **L6 ranking is
17g**; 17e emits unranked proposals.

---

## 5. TypeScript mirrors (`frontend/src/ontology/types.ts`)

**None.** Phase 3b adds no API model and no route; the frontend is untouched (the
Domain draft frame `17.0d` is wired in 17g).

---

## 6. The clustering engine (determinism + seeding — the load-bearing rules)

- **Deterministic by construction.** Leiden is seeded with a **fixed RNG seed**
  (`leidenalg` `Optimiser.set_rng_seed(...)` or `find_partition(..., seed=...)`),
  **fixed `n_iterations` and `beta`**, and a **stable tie-break** (sort by canonical
  ref) so the same graph yields the same communities every run. This is
  non-negotiable: `domain_id` is a fingerprint of the cluster's sorted members, and
  17g's suppression ledger can only suppress a dismissed proposal if that id is
  **stable across runs**. (Leiden is stochastic by default — the seed + single-thread
  is what makes it reproducible.)
- **Leiden, not Louvain.** Use `leidenalg` Leiden. Louvain can leave a community
  internally **disconnected** — worst when applied iteratively, which is exactly the
  recursive Domain→Sub-Domain split — so a "Sub-Domain" could be two unrelated table
  sets. Leiden's refinement phase *guarantees* every community (and sub-community) is
  connected, at higher modularity and lower runtime.
- **Multiplex layers, not a fused scalar.** Feed `leidenalg` one **layer per edge
  kind** (`lineage`/`join_key`/`co_query`/`semantic_sim`, with `tag_assignment` and
  `agent_scope` as seed layers) with per-layer weights, rather than hand-collapsing
  to a single edge weight. This is the principled version of the architecture §5
  weight table and removes the tuned-coefficient guesswork.
- **Soft-seeded / semi-supervised via `initial_membership`.** Governed-tag assignment
  **seeds** the partition using `leidenalg`'s `initial_membership` — a `Finance` tag
  anchoring N assets *starts* those in one community rather than re-deriving the
  domain — but the seed is **soft, not fixed** (do **not** set `is_membership_fixed`):
  Leiden may move a node if the fused graph strongly disagrees. Agent scopes seed the
  same way; catalog/schema is the fallback only when graph signal is thin. This is the
  deliberate difference from a hard pin — it lets the engine *detect* a mis-tag rather
  than cementing it.
- **Two resolutions via CPM `γ`, one algorithm.** Coarse `γ` → Domains; per-Domain
  subgraph finer `γ` → Sub-Domains. Use the **CPM** objective (`objective_function =
  "CPM"`) so the modularity **resolution limit** does not bury small domains next to
  large ones; expose `γ` (coarse + fine) and the per-layer weights as constants with
  sane defaults (architecture §5 "honest gap"). The soft tag/Agent seeding plus the
  reassignment ledger is the stabiliser.
- **Reuse / create / reassign binding.** After naming, bind each cluster to 17d's
  identity verdict:
  - if the cluster's anchor/name resolves to an existing governed tag in the identity
    map and the seed held → `tag_decision="reuse"` with that `tag_key`;
  - if no existing tag matches → `create` with a proposed key in the `Domain/Sub`
    convention;
  - if the soft seed **moved assets out of** an existing governed tag and the
    disagreement exceeds `REASSIGN_MARGIN` (fraction of the tag's seeded members that
    the graph pulled into a different community) → `reassign`, carrying a `conflict`
    block (the contradicted `existing_tag`, the `moved_members`, and the `margin`).
  Never mint a duplicate of a tag 17d already flagged as the same concept.
- **Reassignment is a proposal, never an auto-switch.** A `reassign` row records a
  disagreement between the graph and an existing tag; 17e writes **only** the proposal
  row (into `genie_ont_domains`/`_members`) and mutates **no** governed tag. The human
  approves or dismisses it in 17g (recorded in the consent/suppression ledger; a
  dismissed reassign is suppressed on re-run via the stable `domain_id`), and the
  actual `UNSET/SET TAG` is the Phase-5 consented apply. Keep `REASSIGN_MARGIN`
  conservative so only strong contradictions reach the reviewer (the zero-burden
  ethos); everything below the margin quietly stays `reuse`.
- **Naming degrades.** LLM naming (company prior + member identifiers) is the only
  external call; on failure the cluster gets a deterministic **anchor-derived** name
  and the run still succeeds (MV-D43). Naming is validated structurally (non-empty,
  no invented identifiers leaked into the name — reuse the `LeakageOracle` discipline
  already extended for tag names in 17d).

---

## 7. Persistence / DDL

**No new DDL** — `genie_ont_domains` and `genie_ont_members` were created (empty) in
Phase 2 (`ontology/ddl.py`). Phase 3b **populates** them via the existing idempotent
MERGE.

- **Keys (idempotent MERGE, §7.2 precedent):**
  - `genie_ont_domains` → `(workspace_id, domain_id)`.
  - `genie_ont_members` → `(workspace_id, domain_id, asset_fqn)`.
  - Both use `WHEN NOT MATCHED BY SOURCE ... DELETE` scoped to `workspace_id`, so a
    cluster that no longer exists (or a member that moved) is removed — a re-run
    yields the same rows, never duplicates.
- **Run ledger** — populate `domain_count` (already a column) with the proposed
  domain/sub-domain count.
- **Firewall guard change** — in `ddl.py`, `PHASE3_TABLES` (the "never written in
  this phase" set the firewall test asserts) shrinks to
  `("genie_ont_pages", "genie_ont_consents", "genie_ont_suppressions")`;
  `genie_ont_domains`/`_members` become **written proposal tables**. Add them to a
  `PROPOSAL_TABLES` tuple so the firewall test can assert exactly what 17e writes.
- **Lakebase mirror** — `genie_ont_domains`/`_members` are already registered in
  `scripts/setup_synced_tables.py` (or add them there if not); they read
  Delta-via-warehouse through the Phase-2 `mirror.py` interface until the synced-table
  flip. No new read path.

---

## 8. Batch job implementation notes

No new job task — clustering runs **inside** `run_materialize` (the Phase-2/3a task),
after the ER step that produces the identity map:

1. Build the fused graph (already done at `materialize.py` ~line 164) and the
   identity map (17d). **Map graph nodes to their `canonical_id`** so clustering runs
   over deduped entities.
2. `cluster.py`: project edge kinds into multiplex layers → soft-seeded Leiden with CPM
   (coarse `γ` = Domains) → recursive finer-`γ` split (Sub-Domains) → lineage
   centrality anchors → LLM naming → reuse / create / reassign binding. Deterministic
   (fixed seed).
3. Expand `DomainProposal`s into `genie_ont_domains` + `genie_ont_members` rows and
   **`MERGE`** them (idempotent keys, §7). Set run-ledger `domain_count`.
4. Degrade paths (MV-D43): naming-LLM down → anchor-derived names; an empty/trivial
   graph → zero proposals (MERGE clears any stale rows), run still `succeeded`.

Clustering must **not** block or fail the tag/taxonomy/identity snapshot writes that
already happened earlier in the run — emit proposals as an additive final step; a
clustering error logs and records `failed` without corrupting the good snapshots.

---

## 9. Frontend wiring

**None.** Proposals are unserved until 17g. No component, no type, no route change.

---

## 10. Grants / deploy (DABs)

- **No new system-table grant** — clustering consumes signals already read.
- **No new job, no new env var** — runs in the existing `ontology_materialize` task
  (`GSO_ONT_JOB_ID` reused).
- **Dependency update (the one new deploy-relevant step, MV-D39/D45):** add
  `leidenalg==<exact>` and `igraph==<exact>` to
  `packages/genie-space-optimizer/pyproject.toml`, then
  `uv lock --upgrade-package leidenalg --upgrade-package igraph` and regenerate
  `requirements.txt`
  (`uv export --frozen --no-dev --no-hashes --format requirements-txt | grep -v '^-e ' > requirements.txt; echo '-e ./packages/genie-space-optimizer' >> requirements.txt`).
  Commit `pyproject.toml` + `uv.lock` + `requirements.txt` together (the dependency
  policy). `uv lock --check` and `npm ci --dry-run` must still pass.
- **Synced tables** — confirm `genie_ont_domains`/`_members` are in
  `scripts/setup_synced_tables.py` (add if missing).

---

## 11. Tests (offline, `backend/tests/` + GSO `tests/unit/`, run via `./scripts/test.sh`)

All acceptance is **offline** — clustering is deterministic and runs in-process; the
naming LLM is **mocked**; no cluster, no Lakebase Search.

- **Contract-frozen guard** — Phase-1/2/3a response models byte-identical; **no** new
  API model; taxonomy/tags routes unchanged.
- **Two-level tree on a fixture** — a seeded graph fixture (the architecture's worked
  example: `finance.sales.*` + `marketing.campaigns.*` with a `Commercial` tag prior)
  yields the expected **Domain → Sub-Domain** tree: one `Commercial` domain splitting
  into `Sales` and `Marketing` sub-domains, with `orders` as the centrality anchor.
- **Connectedness (Leiden guarantee)** — every emitted Domain and Sub-Domain community
  is an internally **connected** subgraph (the property Louvain does not guarantee);
  assert on a fixture crafted so Louvain would produce a disconnected community.
- **Multiplex + soft seed** — the layers are passed to `leidenalg` as separate layers
  (not a pre-fused scalar); a governed-tag node is passed via `initial_membership` and
  `is_membership_fixed` is **NOT** set (assert the call passes a soft seed, not a hard
  pin), so a node *can* move when the graph strongly disagrees.
- **Reuse / create / reassign** — a **tag-seeded** cluster whose seed holds emits
  `tag_decision="reuse"` with that `tag_key` (never a duplicate); a cluster with no
  matching tag emits `create` with a `Domain/Sub`-convention key.
- **Reassignment proposal (soft-seed conflict)** — a fixture where the fused graph
  strongly pulls a seeded member **out** of its governed tag (past `REASSIGN_MARGIN`)
  emits `tag_decision="reassign"` with a `conflict` block (`existing_tag`,
  `moved_members`, `margin`) and writes **no** tag; the same disagreement **below** the
  margin stays `reuse`. Assert the run mutates no governed tag (proposal row only).
- **Determinism / idempotency** — running the materializer twice over the same fixture
  yields the **same** `genie_ont_domains`/`_members` rows (stable `domain_id`, no
  dups); a member removed between runs is deleted (`NOT MATCHED BY SOURCE`); a fixed
  `leidenalg` seed (+ fixed `n_iterations`/`beta`) makes community assignment
  reproducible.
- **Naming degrade + firewall** — with the naming LLM mocked to fail, clusters get
  anchor-derived names and the run still succeeds; a naming result that echoes an
  invented identifier is rejected by the `LeakageOracle`.
- **Additive safety** — a clustering exception records the run `failed` but leaves the
  tag_graph/taxonomy/identity snapshots (written earlier in the run) intact.
- **Firewall (updated)** — `test_ontology_firewall.py`:
  - the write guard now allows `genie_ont_domains`/`_members` in `materialize.py`;
    `genie_ont_pages`/`_consents`/`_suppressions` are still **never** written.
  - still **no** `SET TAG` / `CREATE GOVERNED TAG` / `manage_uc_tags` / `web_search`
    anywhere; `lakebase_vector`/`lakebase_text` still confined to `similarity.py`
    (17e adds no similarity code).
- **Lockfile** — `uv.lock` includes `leidenalg` + `igraph`; `uv lock --check` passes
  (a CI-style assertion or a note in the DoD if no lock test exists).

---

## 12. Definition of done & explicit deferrals

**Offline done (the agent stops here) when:** `cluster.py` + the materializer wiring +
the `leidenalg` + `igraph` dependencies (locked) land in the GSO wheel;
`genie_ont_domains`/`_members` populate idempotently with reuse/create/reassign
proposals + evidence; the taxonomy/tags routes and all contracts are provably
unchanged; and `./scripts/test.sh` + `cd frontend && npm run lint` + `tsc` are all
green — including the two-level-tree, connectedness, multiplex/soft-seed,
reuse/create/reassign, determinism/idempotency, naming-degrade/firewall,
additive-safety, and updated-firewall tests. `uv lock --check` passes with `leidenalg` + `igraph` locked.

**Deploy-gated (human, after the offline run — the agent must NOT do these):** run the
`ontology_materialize` job once against the live workspace (via the "Refresh ontology"
button or a manual run) and confirm `genie_ont_domains`/`_members` populate in Delta +
the synced mirror with sensible Domain → Sub-Domain proposals and
reuse/create/reassign decisions. **Note:** there is **no UI change** in 17e — proposals are verified by
querying the tables / job output, not the page (the drafts render in 17g). The SP
reads, the `leidenalg`/`igraph` install on the serverless job, the Delta write, and the
mirror can only be validated in a deployed app — that boundary is why 17e's offline
slice stops before the live run.

**Explicitly deferred (do NOT pull forward):**

- **17f (Phase 3c)** — L5 Page miners → `genie_ont_pages` + MV/Agent advisories.
- **17g (Phase 3d)** — L6 rank/trust (`score` the proposals), the suppression *filter*
  at serve time, the `/drafts` route, and the `17.0d/e` frontend (STOP checkpoint).
  **17g also OWNS reassignment adjudication:** it renders 17e's `reassign`/conflict
  proposals, lets a human approve/dismiss them, and records the decision in
  `genie_ont_consents`/`genie_ont_suppressions`. **No `/drafts`, no ranking, no
  frontend, no adjudication in 17e** — 17e only *emits* the `reassign` proposal row.
- **Phase 4** — external Context Pack vocabulary prior for naming. **Phase 5 (17i)** —
  the consented apply (`genie_ont_applied`); this is the ONLY place an approved
  `reassign` is physically applied (`UNSET` the old tag + `SET` the new one). 17e/17g
  never mutate a governed tag.
