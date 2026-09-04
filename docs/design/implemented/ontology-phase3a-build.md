# Ontology — Phase 3a: signal graph + ER/dedupe (build spec)

**Status:** build-ready (offline slice) · **Owner directive:** MV-D40 (Lakebase
Search similarity behind one interface), inheriting MV-D35 / MV-D37 / MV-D39 /
MV-D45 (see `mv-advisor-playbook.md`, Prompt 17d). **Design source of truth:**
`ontology-engine-architecture.md` §3–§4 (the signal graph), the **L2 graph builder**
subsection under §4, and the **L3 ER / dedupe engine** subsection under §5. This doc
is a *buildable slice* of that design, not a new design. **Builds on:**
`ontology-phase2-build.md` (the batch path + Lakebase mirror, already shipped on the
`ontology` branch).

> **⚠ Grain note (MV-D49, added after this phase shipped):** the code described below
> keys `genie_ont_*` by `workspace_id`. Per **MV-D49** the correct grain is the
> **metastore** (governed tags + Pages + UC assets are metastore-scoped; the reads are
> already account-level). The dedicated re-grain phase (`ontology-regrain-build.md`)
> reconciles this shipped code to `metastore_id` (with `workspace_id` demoted to
> provenance) **before** 17f. Read `workspace_id` below as "the scope key" — the re-grain
> swaps it for `metastore_id` without changing this phase's logic.

This is the **third** Goal-Mode deliverable and the **first** proposal-engine phase.
It is deliberately narrow: **complete the fused signal graph, then stand up the
entity-resolution / dedupe engine (block → score → adjudicate → gate → PII firewall)
that resolves governed tags, measures, MVs, and Agent scopes into canonical entities,
and persist the resulting identity map + enriched dedupe verdicts to Delta + the
Lakebase mirror.** It writes **no domains, no sub-domains, no Pages** (those are 17e /
17f), adds **no new routes and no new API response models**, and does **not enable
Lakebase Search** — the similarity call sits behind one interface that degrades to
in-process cosine, and the irreversible enablement is the human deploy gate (§12).

> **The one-line contract:** Phase 2 = *materialize what already exists* (tags +
> taxonomy) to a fast mirror. Phase 3a = *resolve identity* — decide which tags /
> measures / MVs are the **same thing** (reuse) versus genuinely distinct — so every
> later phase (clustering, Pages, rank) forms over **canonical** entities, never
> duplicates. Runs **before any clustering** so we never grow the sprawl we are taming.

---

## 1. Scope

### In (Phase 3a)

- **L2 graph completion** → promote the Phase-2 `ontology/graph.py` scaffold (nodes +
  tag_assignment/lineage edges) to the **full weighted heterograph**: add the
  remaining edge kinds the design names (`co_query`, `agent_scope`, `cost` node
  attribute, and — populated by L3 below — `semantic_sim`), each edge carrying its
  per-edge `source` + `as_of` (the Provenanced discipline). **Still no clustering /
  no communities** (MV-D39 stays a scaffolded dependency; clustering is 17e).
- **L3 ER / dedupe engine** (`ontology/er.py`) → the standard three-step pipeline over
  {governed tags, measures, MV bodies, Agent scopes, page-name candidates}:
  1. **Block** — bucket candidates cheaply by name prefix / schema / node type
     (avoids O(n²)); only compare within a bucket.
  2. **Score** — two signals per pair: **string/keyword** (edit distance + BM25) and
     **embedding** (cosine), both **behind one `similarity` interface** (§6).
  3. **Adjudicate** — auto-merge high scores, auto-reject low scores, escalate only
     the **near-tie band** to `llm_utils.call_serving_endpoint` for a yes/no + reason.
  4. **Confidence gate** — `mv_scoring.dedup_gate`-pattern thresholds
     (auto-merge / auto-reject / escalate).
  5. **PII firewall** — `leakage.LeakageOracle` extended to **tag names** (tag names
     replicate globally in plaintext — a name that echoes a PII token is rejected).
- **Similarity interface (MV-D40 / MV-D45)** → one `similarity.py` seam with **two**
  backends: a **Lakebase Search** backend (`lakebase_vector` ANN + `lakebase_text`
  BM25 on the existing Lakebase) and an **in-process cosine + edit-distance** fallback.
  The offline slice tests the **fallback**; the Lakebase Search backend is authored
  but **not enabled** (enablement is irreversible — §12).
- **Embeddings** → reuse `leakage.get_embedding` (the `databricks-gte-large-en`
  FMAPI endpoint) and **L2-normalize GTE output** per the `mv_scoring` caveat, behind
  the same interface (so a fixture vector set drives offline tests).
- **Identity map persistence (L7)** → a **new** `genie_ont_identity` Delta table
  (`canonical_id → member`) written idempotently by the materializer, plus the
  **enriched `genie_ont_tag_graph.dedupe_verdicts`** (now embedding-backed, not just
  exact/fuzzy). Both flow to the existing Lakebase mirror via the Phase-2 reader.

### Out (deferred — see §12)

L4 clustering / Leiden (17e), the `genie_ont_domains` / `genie_ont_members` /
`genie_ont_pages` proposal writes (17e / 17f), the `17.0d/e` **drafts** + `/drafts`
route + frontend (17g), external enrichment / Context Pack (Phase 4), and the
`SET TAG` apply (Phase 5). Phase 3a resolves **identity**; it proposes **no new
domain/page** and writes **nothing to UC governance**.

---

## 2. Decisions honored (and which sleep in Phase 3a)

| Decision | Phase-3a posture |
|---|---|
| MV-D35 evidence-first trust | **Active** — every merge/collision verdict carries its signals; near-ties escalate, never silently merge. |
| MV-D36 standalone admin-gated estate page | **Active** — unchanged. |
| MV-D37 governed-tag substrate + Tags lens | **Active (read-only)** — dedupe verdicts get richer (embedding-backed) but the `TagLens` **shape is frozen**; still **no** `CREATE/SET TAG`. |
| MV-D38 external enrichment | **Dormant** — absent entirely. |
| MV-D39 in-job Leiden clustering | **Scaffolded** — the graph is completed here; **no clustering / no communities** (that is 17e). No `leidenalg`/`igraph` import added in 17d. |
| MV-D40 Lakebase Search similarity | **Active (interface + fallback) — this is the phase.** Both backends authored; **Lakebase Search NOT enabled** (beta, irreversible — §12); offline runs on in-process cosine. |
| MV-D41 nightly batch + on-demand | **Active** — L3 runs **inside** the existing `ontology_materialize` job; **no new job** (reuse `GSO_ONT_JOB_ID`). |
| MV-D42 catalog allowlist | **Active** — the allowlist scopes the ER candidate inventory. |
| MV-D43 degrade-not-hang | **Active** — if similarity/adjudication is unavailable the engine degrades (fallback backend / skip the near-tie escalation), never blocks the job or the page. |
| MV-D44 enrichment OFF by default | **Active (trivially)** — no enrichment path yet. |
| MV-D45 minimal install footprint | **Active** — reuses the **existing Lakebase, GTE endpoint, GSO job, LLM client**; the only new artifact is one Delta table + two wheel modules. Lakebase Search is an **extension of the Lakebase already installed**, not a new managed service. |
| MV-D46 / MV-D47 web search / MCP registry | **Dormant** — no web-search path. |

**Load-bearing consequence:** the mirror now carries an **identity map**. The Tags
lens (17.0c) keeps its exact Phase-1 response shape but its `collisions` are now
adjudicated with embedding evidence. No route contract changes.

---

## 3. Subsystem layout (extends Phase 2 in the GSO wheel; backend read-only touch)

Batch-side only, in the **GSO wheel** (runs on the job cluster; must not import
`backend.*`). The backend changes are limited to reading the richer verdicts through
the Phase-2 mirror — **no new routes, no new API models**.

```
packages/genie-space-optimizer/src/genie_space_optimizer/
  ontology/
    similarity.py     # NEW — one interface; LakebaseSearchBackend + InProcessCosineBackend (MV-D40/D45)
    er.py             # NEW — L3: block → score → adjudicate → confidence gate → PII firewall
    graph.py          # MODIFIED — complete the weighted heterograph (co_query, agent_scope, cost, semantic_sim)
    transforms.py     # MODIFIED — pure dedupe-verdict enrichment (identity map assembly; still side-effect-free)
    ddl.py            # MODIFIED — + genie_ont_identity table (idempotent MERGE builder reused)
    materialize.py    # MODIFIED — call er.py; MERGE genie_ont_identity + enriched tag_graph verdicts
                      #   (STILL never writes domains/members/pages/consents/suppressions)

backend/ontology/
  services/
    dedupe.py         # MODIFIED (optional) — surface mirror-backed enriched verdicts; TagLens shape UNCHANGED
```

**Reuse, do not fork:**

- `genie_space_optimizer/optimization/leakage.py` — `get_embedding` (GTE) for the
  embedding score, and `LeakageOracle` extended to tag-name PII.
- `genie_space_optimizer/optimization/mv_scoring.py` — the `EmbeddingClient`
  Protocol + GTE L2-normalize caveat (`similarity.py` takes a client of this shape),
  and the `dedup_gate` threshold pattern for the confidence gate.
- `backend/services/llm_utils.py` (`call_serving_endpoint`) +
  `backend/services/model_catalog.py` (`validate_chat_model`) — the **only** LLM path
  for near-tie adjudication (model override validated).
- `genie_space_optimizer/ontology/ddl.py` (Phase 2) — the Delta-DDL + idempotent
  `MERGE` builder the new `genie_ont_identity` table reuses verbatim.
- `genie_space_optimizer/ontology/materialize.py` (Phase 2) — the
  reader→transforms→MERGE orchestration L3 slots into (no new job task).
- `backend/services/gso_lakebase.py` + Phase-2 `mirror.py` — the identity map / enriched
  verdicts ride the **existing** mirror reader; no new read path.

---

## 4. Backend contracts (`backend/ontology/models.py`)

**No new API response model.** All Phase-1 + Phase-2 models
(`OntologyPreflight`, `OntologyInventory`, `OntologyTaxonomy`, `TagLens`,
`TagCollision`, `OntologySettings`, `OntologyRefreshStatus`) keep their **exact**
shape (the "contracts MUST NOT change" rule). Phase 3a enriches the *content* of
`TagLens.collisions` (now embedding-adjudicated) but not its *schema*.

Internal wheel-side types (NOT API models, not exported to the frontend):

```python
# genie_space_optimizer/ontology/er.py — dataclasses, not Pydantic API models
Verdict = Literal["merge", "reject", "escalate", "distinct"]

@dataclass(frozen=True)
class DedupeCandidate:
    ref: str            # canonical member ref (tag_key / measure fqn / mv fqn / agent id)
    kind: str           # 'tag' | 'measure' | 'metric_view' | 'agent' | 'page_name'
    name: str
    text: str           # name + comment used for embedding/BM25

@dataclass(frozen=True)
class DedupeVerdict:
    canonical_id: str   # derived id (dedupe_<fingerprint>)
    members: tuple[str, ...]
    verdict: Verdict
    method: Literal["exact", "string", "embedding", "llm"]
    score: float
    reason: str | None  # populated only for LLM-adjudicated near-ties
```

The identity map (`canonical_id → members`) is these verdicts, persisted to
`genie_ont_identity` (§7) and the mirror — it never becomes a route in 17d (drafts
are 17g).

---

## 5. TypeScript mirrors (`frontend/src/ontology/types.ts`)

**None.** Phase 3a adds no API model, so the frontend types are untouched. (The Tags
lens already renders `TagCollision`; its shape is frozen.)

---

## 6. The similarity interface (the MV-D40 / MV-D45 seam)

One interface, two backends, chosen by config — this is what keeps Lakebase Search
**optional and reversible** at the code level even though enabling it is not.

```python
# genie_space_optimizer/ontology/similarity.py
class SimilarityBackend(Protocol):
    def topk_embedding(self, query_vec, candidates, k) -> list[tuple[str, float]]: ...
    def topk_keyword(self, query_text, candidates, k) -> list[tuple[str, float]]: ...

class InProcessCosineBackend:      # DEFAULT (offline + when Lakebase Search off, MV-D45)
    # cosine over get_embedding() vectors (L2-normalized GTE) + edit-distance / token BM25 in-process
    ...

class LakebaseSearchBackend:       # authored; ACTIVE only after the §12 human enable
    # lakebase_vector ANN kNN + lakebase_text BM25 over a Lakebase-synced candidate table
    ...

def get_similarity_backend(settings) -> SimilarityBackend:
    # returns LakebaseSearchBackend iff settings.lakebase_search_enabled else InProcessCosineBackend
    ...
```

- The **default is the in-process backend** — the engine is fully functional (just
  slower on large estates) without Lakebase Search. This is the MV-D45 degrade path
  and the offline test target.
- `LakebaseSearchBackend` is written and unit-tested against a **fake** that mimics
  the `lakebase_vector` / `lakebase_text` SQL contract; it is **not run against a real
  Lakebase** until the §12 enable gate. `lakebase_vector` / `lakebase_text` tokens
  appear **only** in `similarity.py` (the firewall relaxation is scoped — §11).
- Embeddings always come through `leakage.get_embedding`; the backend never re-embeds.

---

## 7. Persistence / DDL

Same two tiers as Phase 2 (Delta = SoR, written by the job; Lakebase = synced-table
mirror, read by the page). Phase 3a adds **one** table and enriches one column.

### 7.1 Delta — `genie_space_optimizer/ontology/ddl.py` (MODIFIED)

```sql
-- Identity map (canonical entity -> members) — NEW in 17d --------------------
-- One row per (workspace_id, canonical_id, member_ref); the ER output that L4
-- clustering (17e) forms communities over. Written idempotently (MERGE).
CREATE TABLE IF NOT EXISTS ${GSO_CATALOG}.${GSO_SCHEMA}.genie_ont_identity (
  workspace_id   STRING,
  canonical_id   STRING,             -- derived id (dedupe_<fingerprint>)
  member_ref     STRING,             -- (workspace_id, canonical_id, member_ref) is the derived PK
  member_kind    STRING,             -- 'tag' | 'measure' | 'metric_view' | 'agent' | 'page_name'
  verdict        STRING,             -- 'merge' | 'reject' | 'escalate' | 'distinct'
  method         STRING,             -- 'exact' | 'string' | 'embedding' | 'llm'
  score          DOUBLE,
  reason         STRING,             -- LLM reason for near-tie adjudications only
  run_id         STRING,
  as_of          TIMESTAMP
) USING DELTA;
```

- `genie_ont_tag_graph.dedupe_verdicts` (JSON, created in Phase 2) is now populated
  with the **embedding-backed** collisions from L3 — same column, richer content.
- The Phase-2 empty tables (`genie_ont_domains`, `genie_ont_members`,
  `genie_ont_pages`, `genie_ont_consents`, `genie_ont_suppressions`) stay **empty** —
  17d writes **none** of them.

### 7.2 Idempotency

- `genie_ont_identity` is keyed on `(workspace_id, canonical_id, member_ref)` and
  written with `MERGE` + `WHEN NOT MATCHED BY SOURCE ... DELETE` scoped to
  `workspace_id` (the Phase-2 builder), so a re-run yields the **same** canonical
  groups — no duplicates, and a member that left a group is removed.
- `canonical_id` is a **derived fingerprint** of the sorted member set, so identity
  is stable across runs (a merge decided last night is the same id tonight).

### 7.3 Lakebase mirror

Register `genie_ont_identity` in `scripts/setup_synced_tables.py` alongside the
Phase-2 snapshots so it lights up on the synced-table flip; until then it reads
**Delta-via-warehouse** through the existing Phase-2 `mirror.py` interface (no new
read path — `_SYNCED_TABLES_ENABLED` reality unchanged).

---

## 8. Batch job implementation notes

No new job task — L3 runs **inside** `jobs/run_ontology_materialize.py` (the Phase-2
task), after the tag/taxonomy snapshot step:

1. Build the **completed L2 graph** (`graph.py`) from the signal frames.
2. Assemble `DedupeCandidate`s from the allowlist-scoped inventory (tags, measures,
   MV bodies, Agent scopes, page-name candidates).
3. Run `er.py`: **block → score** (via the configured `similarity` backend — cosine
   fallback offline) **→ adjudicate near-ties** (`call_serving_endpoint`, only on the
   escalate band) **→ confidence gate → PII firewall** on tag names.
4. **`MERGE`** the identity map into `genie_ont_identity` and the enriched verdicts
   into `genie_ont_tag_graph.dedupe_verdicts`. On any similarity/LLM failure →
   degrade (fallback backend / skip escalation), log, and still write the exact/string
   verdicts; the run does not fail on adjudication unavailability (MV-D43).
5. Flip `genie_ont_runs` to `succeeded` with counts (now incl. `identity_count`).

Concurrency + trigger are the Phase-2 `refresh.py` path unchanged (same
`GSO_ONT_JOB_ID`, same `run_now`).

---

## 9. Frontend wiring

**None new.** The Tags lens (17.0c) already renders collisions; it now shows
richer (embedding-adjudicated) groups through the **unchanged** `TagLens` contract —
no component change required beyond what Phase 1/2 shipped. No draft frames (17g).

---

## 10. Grants / deploy (DABs)

- **No new system-table grant** — the ER candidate inventory reads the same system
  tables Phase 1/2 already read (governed tags, information_schema, lineage) under the
  grants already in `deploy_lib/uc.py`.
- **No new job, no new env var** — L3 runs in the existing `ontology_materialize`
  task; `GSO_ONT_JOB_ID` is reused.
- **Lakebase Search enablement is NOT a deploy-script step** — it is a deliberate,
  human, irreversible action against the app's Autoscaling Lakebase (§12). The deploy
  script and installer are **not** modified to enable it.
- **Synced tables** — add `genie_ont_identity` to `scripts/setup_synced_tables.py`
  (+ the `deploy_lib/` notebook path), same as the Phase-2 snapshots.

---

## 11. Tests (offline, `backend/tests/` + GSO `tests/unit/`, run via `./scripts/test.sh`)

All acceptance is **offline** — L3 runs on the in-process cosine backend over fixture
vectors; the LLM adjudicator is **mocked**; no Lakebase Search, no cluster.

- **Contract-frozen guard** — the Phase-1 + Phase-2 response models are **byte-
  identical** (field set + types); Phase 3a adds **no** API model. `TagLens` /
  `TagCollision` shapes unchanged.
- **Blocking recall** — on a fixture inventory, the blocker puts all true-duplicate
  pairs in a shared bucket (recall == 1.0 on the fixture) while keeping bucket sizes
  sub-quadratic.
- **String vs embedding catch** — `order_revenue` vs `orders_revenue` collapses via
  the string/BM25 signal; `net revenue` vs `revenue after discount` collapses via the
  embedding signal; a **true-distinct** pair (`headcount` vs `revenue`) does **not**.
- **Adjudication band** — the LLM adjudicator is called **only** for near-tie pairs
  (assert call count + that high/low-confidence pairs never reach it); mock
  `call_serving_endpoint`; a mocked "no" keeps them distinct.
- **Backend selection / degrade** — `get_similarity_backend` returns the in-process
  backend when `lakebase_search_enabled` is false (the default); the engine produces
  identical verdicts whether the fake Lakebase backend or the in-process backend is
  injected (parity on the fixture).
- **PII firewall** — a proposed/observed tag name that echoes a PII token is rejected
  by the extended `LeakageOracle` and never enters the identity map.
- **Identity idempotency** — running the materializer twice over the same fixture
  yields the **same** `genie_ont_identity` rows (stable `canonical_id`, no dups); a
  member removed between runs is deleted (`NOT MATCHED BY SOURCE`).
- **Firewall (updated)** — `test_ontology_firewall.py`:
  - `_DEFERRED_TOKENS` drops `lakebase_vector` / `lakebase_text` (now allowed) but
    **keeps `web_search`**; assert those two tokens appear **only** in
    `similarity.py` (scoped, not sprayed across the package).
  - `test_wheel_writes_only_snapshot_tables_never_phase3` → the materializer may now
    also write `genie_ont_identity`; the **proposal** tables
    (`domains`/`members`/`pages`/`consents`/`suppressions`) are still **never**
    written; no `SET TAG` / `CREATE GOVERNED TAG` / `manage_uc_tags` anywhere.

---

## 12. Definition of done & explicit deferrals

**Offline done (the agent stops here) when:** `similarity.py` (both backends) + `er.py`
+ the completed `graph.py` + the `genie_ont_identity` DDL + the materializer wiring
land in the GSO wheel; the identity map + enriched verdicts write idempotently; the
Tags lens still renders through the frozen contract; and `./scripts/test.sh` +
`cd frontend && npm run lint` + `tsc` are all green — including the blocking,
string/embedding, adjudication-band, degrade-parity, PII-firewall, identity-
idempotency, and updated-firewall tests. `uv.lock` is untouched (no new dependency —
`igraph` is 17e).

**Deploy-gated (human, after the offline run — the agent must NOT do these):**

- **Enable Lakebase Search** (`lakebase_vector` + `lakebase_text`) on the app's
  Autoscaling Lakebase. **This is beta, IRREVERSIBLE, and restarts computes — it is
  the Phase-3 gate.** Do it deliberately, out of band, with the account team; the
  agent never runs it.
- Flip `settings.lakebase_search_enabled` on, run the `ontology_materialize` job once
  against the live workspace, and confirm the `LakebaseSearchBackend` returns ANN +
  BM25 neighbors, the identity map populates in Delta + the synced mirror, and the
  Tags lens collisions match the offline in-process verdicts (parity in production).
- The GTE embedding endpoint, the SP system-table reads, the Delta write, and the
  Lakebase Search index can only be validated in a deployed app — that boundary is why
  Phase 3a's offline slice stops before the enable.

**Explicitly deferred (do NOT pull forward):**

- **17e (Phase 3b)** — L4 **Leiden clustering** (via `leidenalg`/`igraph`) over the canonical
  entities this phase resolves; writes `genie_ont_domains` / `genie_ont_members`
  proposal rows with reuse-vs-create `tag_decision`.
- **17f (Phase 3c)** — L5 Page miners → `genie_ont_pages` + MV/Agent advisories.
- **17g (Phase 3d)** — L6 rank/trust + serve the `17.0d/e` drafts + the decision
  route + frontend (STOP checkpoint). **No `/drafts`, no `/apply` in 17d.**
- **Phase 4** — external context / Context Pack. **Phase 5** — the `SET TAG` apply.
