# Ontology — Phase 3c: Page miners (L5) — build spec

**Status:** build-ready (offline slice) · **Owner directive:** MV-D49 (grain =
metastore; Pages are account-level Discover artifacts), MV-D27 (Page-only — the advisor
writes Pages, never Agent instructions), MV-D35 (evidence-first), MV-D37 (governed-tag
substrate; MV/Agent advisories are first-class), inheriting the shipped 17d/17e spine
(see `mv-advisor-playbook.md`, Prompt 17f). **Design source of truth:**
`ontology-engine-architecture.md` §5 — the **L5 Page-miners** subsection (detect →
draft → validate → dedupe → rank/persist) — §7 (the **metastore grain**), plus
`page-archetypes.md` (the eight archetypes + Page standard) and
`genie-retrieval-notes.md` (the retrieval gates).
**Builds on:** `ontology-regrain-build.md` (the metastore re-grain, MV-D49 — 17f is
keyed by `metastore_id` from the start) **and** `ontology-phase3b-build.md` (the L4
Domain → Sub-Domain tree in `genie_ont_domains`/`_members`) **and**
`ontology-phase3a-build.md` (the **17d identity map** `genie_ont_identity` — the
canonical concepts 17f anchors on).

This is the **fifth** proposal-engine deliverable (after the re-grain) and the **first
phase to populate `genie_ont_pages`**. It is deliberately narrow: **for each CANONICAL
concept (17d) that surfaces across the estate, run deterministic per-archetype
detectors over the measures / coded columns / conflicts of ALL artifacts that resolve
to that concept, gate on CORROBORATION (how many independent artifacts back it), draft
each candidate's body with the LLM, validate it against the retrieval gates, and emit
archetype-tagged Page proposals (with copy-ready Related/Sources) into the
(already-DDL'd, still-empty) `genie_ont_pages` table + the Lakebase mirror.** It adds
**no serving route and no frontend** — the `17.0e` Page draft is *served* in 17g, not
here — and it writes **`genie_ont_pages` proposal rows only, never `SET TAG`, never an
Agent-instruction write (MV-D27).**

> **The one-line contract:** Phase 3b decided *which entities belong together*
> (Domains/Sub-Domains). Phase 3c decides *what governed knowledge each business
> CONCEPT needs* — Pages are **account-level (metastore-grain, MV-D49) artifacts keyed
> to a canonical concept (17d), not to a single artifact**, so one Page aggregates every
> measure/column/Agent across the estate that expresses the same concept, and its
> confidence/certify-eligibility is **gated on corroboration** (≥2 independent artifacts
> → full; a single artifact → low-confidence). Validated for retrieval quality, written
> as proposals to Delta + the mirror. Nothing is served to the UI and nothing is applied
> to Unity Catalog until 17g/17i.

---

## 1. Scope

### In (Phase 3c)

- **L5 Page-miner engine** (`ontology/pages.py`, new) over each **canonical concept**
  (17d `genie_ont_identity.canonical_id`) that surfaces in the metastore's Sub-Domain
  communities (`genie_ont_domains` where `parent_id IS NOT NULL`) and its member assets
  + the fused graph / metric-view measures:
  0. **Anchor + aggregate (estate-level, canonical-concept, MV-D49).** Detectors run
     **per canonical concept, not per artifact.** Resolve every candidate signal
     (measure / column / Agent) to its 17d `canonical_id`, then **aggregate all
     artifacts across the metastore that resolve to the same concept** — including
     artifacts that landed in **different** Sub-Domains (a concept legitimately spans
     sub-domains). The concept's **corroboration count** = the number of **independent
     artifacts** (distinct source assets / MVs / Agents) backing it. The Page's **home**
     `domain_id` is the Sub-Domain of the concept's strongest membership; the Page
     itself is concept-scoped, not artifact-scoped.
  1. **Detect (deterministic — one detector per archetype, LLM-free).** Each detector
     emits a `PageCandidate(archetype, canonical_id, evidence, corroboration,
     confidence)` from signals only, over the **aggregated** artifacts for the concept:
     - `[Routing]` — one candidate **per canonical measure concept** (all MV measures
       resolving to the same `canonical_id` collapse to ONE Page): NL question → the
       governed view + measure(s) (one canonical answer, all corroborating measures as
       Sources).
     - `[Disambiguation]` — one term → **several measures** across MVs: reuse the
       **CONFLICT / same-concept-different-expression** fingerprints (`mv_fingerprint`)
       and 17d's ER collisions; fires only when a real collision exists (inherently
       ≥2-artifact → corroborated).
     - `[Guardrail]` — **ratio / percentage-format** measures + **AVG-of-rate** shapes:
       never average a rate — recompute from numerator / denominator.
     - `[Taxonomy]` — **low-cardinality coded columns** (column comments + profiled
       distinct values) resolving to one canonical code list: decode codes / buckets /
       glossary.
     - `[Method]` / `[Cross-domain]` / `[Defaults]` / `[Rule]` — **low-confidence
       stubs**, emitted **only** when their signal is unambiguous (same-family measures;
       join-spine; standard filter; structural-break exclusion). No signal → nothing.
  1b. **Corroboration gate (estate-level, MV-D35).** A concept with **≥2 independent
     artifacts** is **full-confidence + certify-eligible**; a concept backed by a
     **single artifact** is emitted **low-confidence + `certify=false`** (a real concept
     but not yet corroborated across the estate) — never dropped, never silently
     promoted. `[Disambiguation]` requires a genuine collision (≥2 by construction).
  2. **Draft (LLM writes the body).** `llm_utils.call_serving_endpoint` writes the Page
     body (description, definition, rules, synonyms) from the candidate + its evidence,
     into the `page-archetypes.md` standard format. **Vocabulary is deterministic in
     17f** — synonyms/overlay come from member identifiers, column comments, and
     existing instruction text; the **Context Pack vocabulary prior is Phase 4 (17h)**.
     Degrades (MV-D43): LLM down → a deterministic evidence-derived stub body +
     `certify=false`, run still succeeds.
  3. **Validate (retrieval gates, validator-enforced — see §6).** Structural format +
     identifier gate (title prefix from the eight; required sections; backticked
     identifiers that **EXIST** in the members — an invented column fails), the four
     retrieval gates (synonyms / chunk-safe / specificity / corroboration), the
     **read-only contradiction gate**, and a certify recommendation. Routing Pages are
     optionally confirmed with `genie_client.run_genie_query` (the `ask_genie` path;
     degrades if Genie is unreachable — mark unvalidated, never block).
  4. **Dedupe (best-effort).** Pages have **no read API**, so Page-vs-Page dedupe is
     name/synonym heuristics only (reuse `ontology/similarity` + `er.pii_reject`) — the
     flagged asymmetry from architecture §5. Best-effort, never a silent merge.
  5. **Emit (L7).** MERGE Page proposals into `genie_ont_pages` (already DDL'd empty in
     Phase 2, re-grained to `metastore_id` by the re-grain phase), with **copy-ready**
     `related_fqns` / `source_fqns` pre-filled (MV-D27) — **all** corroborating
     artifacts for the concept: `[Routing]` → every MV + measure resolving to the
     concept as Sources, the Agent(s) as Related; `[Guardrail]` → the
     numerator/denominator measures as Sources. Plus a `page_count` on the ledger.
- **Determinism / idempotency** → detectors are deterministic; each `page_id` is a
  **derived fingerprint of deterministic, concept-level signals only** (`(canonical_id,
  archetype, sorted canonical key identifiers)` — **not** `domain_id`, so a concept that
  moves sub-domains keeps the same Page), **never** of the LLM prose — so a re-run
  yields the **same** `page_id`s (a hard requirement for the MV-D26 suppression ledger
  in 17g) even if the drafted body text varies or the home sub-domain shifts.
- **Firewall (extended)** → `LeakageOracle` (`optimization/leakage.py`) extended to
  scan Page bodies (the MV-D8 comment-echo rule transposed — same oracle, no second
  scanner); `genie_ont_pages` moves from the "never written" set into a written
  proposal table.

### Out (deferred — see §12)

- **MV / Agent advisories as a persisted surface** (MV-D37 metric-view gaps/dupes/
  quality; Agent domain-assignment/overlap). MV **dupe/quality** collisions are already
  surfaced by 17d's identity map, and Agent **overlap** by 17e's domain `seed`
  evidence; a *dedicated advisory surface* needs either a new table or a serving
  contract, so it is deferred to 17g (rank/serve) rather than smuggled into this
  no-new-DDL offline slice. **Flag for the owner** — see §12 and the launch note.
- L6 rank/trust (`score` the Pages), the suppression **filter** at serve time, the
  `/drafts` route, and the `17.0e` frontend (17g). External Context Pack vocabulary
  prior (Phase 4/17h). The consented apply (Phase 5/17i). Phase 3c **proposes** Pages;
  it serves nothing and writes nothing to UC governance or Agent instructions.

---

## 2. Decisions honored (and which sleep in Phase 3c)

| Decision | Phase-3c posture |
|---|---|
| MV-D49 grain = metastore | **Active — load-bearing.** Pages are keyed `(metastore_id, page_id)`; concepts aggregate across the whole metastore (including across sub-domains). `workspace_id` is provenance only. 17f is authored on the re-grained foundation. |
| MV-D27 Page-only | **Active — load-bearing.** The miners write **Pages only**; no Agent-instruction write, ever. The contradiction gate is **read-only** against existing `text_instructions` and never writes back. |
| MV-D35 evidence-first trust | **Active — extended to corroboration.** Every Page carries its detector evidence (measure/column/conflict) **and its corroboration count**; a single-artifact concept ships **low-confidence + `certify=false`**, ≥2 independent artifacts → full/certify-eligible. No rule ships without a backticked identifier or formula behind it. |
| MV-D36 standalone admin-gated estate page | **Active** — unchanged. |
| MV-D37 governed-tag substrate + advisories | **Partial** — Pages are filed under their Sub-Domain (`genie_ont_pages.domain_id`); the **MV/Agent advisory surface is deferred** (§1 Out, §12). Still **no** `SET/UNSET TAG`. |
| MV-D38 external enrichment | **Dormant** — synonyms/overlay are deterministic (identifiers + column comments + existing instructions); the Context Pack vocabulary prior is Phase 4 (17h). |
| MV-D39 Leiden clustering | **Consumed, not extended** — Pages mine the sub-domain communities 17e produced; no clustering code changes. |
| MV-D40 Lakebase Search similarity | **Reused, not extended** — best-effort Page dedupe reuses the 17d in-process similarity path; **Lakebase Search stays NOT enabled** (17f adds no similarity backend code). |
| MV-D41 nightly batch + on-demand | **Active** — Page mining runs **inside** the existing `ontology_materialize` job (reuse `GSO_ONT_JOB_ID`); **no new job**. |
| MV-D42 catalog allowlist | **Active** — the allowlist already scoped the graph/tree the miners consume. |
| MV-D43 degrade-not-hang | **Active** — LLM down → deterministic stub body; `ask_genie` unavailable → skip routing validation; a mining error records `failed` but leaves the 17d/17e snapshots intact. |
| MV-D44 enrichment OFF by default | **Active (trivially)** — no enrichment path in 17f. |
| MV-D45 minimal install footprint | **Active — no new dependency.** Reuses the existing wheel machinery (`mv_fingerprint`, `mv_scoring`, `leakage`, `genie_client`, `llm_utils`); the only new artifact is one module + tests. `uv.lock` **untouched**. |
| MV-D46 / MV-D47 web search / MCP registry | **Dormant.** |

**Load-bearing consequence:** the mirror now carries **Page proposal rows** for the
first time. The 17e Domain/Sub-Domain proposals are unchanged; `genie_ont_pages` is a
**separate** surface that stays unserved until 17g. No route contract changes.

---

## 3. Subsystem layout (extends Phase 3b in the GSO wheel; no backend touch)

Batch-side only, in the **GSO wheel**. **No backend change, no route, no model, no
frontend** — the Pages are written to Delta/mirror and served later (17g).

```
packages/genie-space-optimizer/
  src/genie_space_optimizer/
    ontology/
      pages.py              # NEW — L5: per-archetype detectors → LLM draft → gates → PageCandidate → rows
      materialize.py        # MODIFIED — call pages after cluster; MERGE genie_ont_pages
      ddl.py                # MODIFIED — move genie_ont_pages out of PHASE3_TABLES into a PAGE/PROPOSAL set
      transforms.py         # MODIFIED (optional) — pure page-row expansion helper (offline-testable)
    optimization/
      leakage.py            # MODIFIED — LeakageOracle gains a page-body scan (same oracle, no 2nd scanner)
backend/tests/test_ontology_firewall.py   # MODIFIED — genie_ont_pages now a written table; consents/suppressions still forbidden
```

**Reuse, do not fork:**

- `ontology/materialize.py` (`run_materialize`) — the reader→transforms→MERGE loop
  Page mining slots into, **after** the 17e clustering MERGE (additive-last).
- `ontology/cluster.py` outputs (`genie_ont_domains`/`_members`) + `graph.py` — the
  sub-domain communities + their member assets/measures are the miners' input; do not
  re-cluster.
- `optimization/mv_fingerprint` — the same-concept-different-expression / CONFLICT
  fingerprints feed `[Disambiguation]`; do **not** write a new comparator.
- `optimization/mv_scoring` — measure/format shapes reused by `[Routing]`/`[Guardrail]`.
- `optimization/leakage.LeakageOracle` — **extend** for Page bodies (MV-D8 discipline);
  `er.pii_reject` for name/synonym PII.
- `common/genie_client.run_genie_query` — the `ask_genie` routing-validation path
  (lazy + degrades on the job cluster).
- `backend/services/llm_utils.call_serving_endpoint` (+ `model_catalog.validate_chat_model`)
  — the **only** LLM path, for body drafting only (lazy-imported + degrades, the 17e
  `default_namer` precedent).
- `ontology/similarity` + `er` — best-effort Page dedupe (name/synonym heuristics).

---

## 4. Backend contracts (`backend/ontology/models.py`)

**No new API response model, no route.** Every Phase-1/2/3a/3b model is FROZEN. The
Pages live in Delta + the mirror and are **not** exposed through any route until 17g.
Internal wheel-side types only (dataclasses, not Pydantic API models):

```python
# genie_space_optimizer/ontology/pages.py
Archetype = Literal[
    "Routing", "Disambiguation", "Guardrail", "Taxonomy",
    "Method", "Cross-domain", "Defaults", "Rule",
]

@dataclass(frozen=True)
class PageCandidate:
    page_id: str              # derived: pg_<fingerprint of (canonical_id, archetype, sorted canonical key ids)>
    canonical_id: str         # 17d identity — the CONCEPT this Page elevates (the anchor, MV-D49)
    domain_id: str            # the HOME Sub-Domain (strongest membership) — provenance, NOT in page_id
    archetype: Archetype
    title: str                # "[Archetype] <concept>" — prefix from the eight
    body: str                 # LLM-drafted (or deterministic evidence stub on degrade)
    synonyms: tuple[str, ...] # >=3 across the four classes (retrieval gate)
    related_fqns: tuple[str, ...]   # Discover Related (the serving Agent(s), across the estate)
    source_fqns: tuple[str, ...]    # Discover Sources (ALL corroborating MVs + measures; EXISTING, identifier-gated)
    corroboration: int        # # of independent artifacts backing the concept (>=2 → certify-eligible)
    certify: bool             # formulas + corroboration>=2 → yes; single-artifact/informational → no
    evidence: dict            # {measure|column|conflict, detector, corroboration, contributing_artifacts, gate_results}
    confidence: float         # detector confidence (deterministic; L6 ranking is 17g)
```

These map 1:1 onto the **existing** `genie_ont_pages` columns (§7 — `canonical_id` and
`corroboration` ride in the `evidence` JSON if not first-class columns; no new DDL).
`score` is written as `NULL`/`0.0` — **L6 ranking is 17g**; 17f emits unranked,
gate-validated candidates.

---

## 5. TypeScript mirrors (`frontend/src/ontology/types.ts`)

**None.** Phase 3c adds no API model and no route; the frontend is untouched (the
Page draft frame `17.0e` is wired in 17g).

---

## 6. The Page-miner engine (detectors + retrieval gates — the load-bearing rules)

- **Deterministic detectors, LLM prose only.** Each detector is pure and offline; the
  `page_id`, `archetype`, `source_fqns`, and `certify` decision are all deterministic.
  The LLM writes the *body prose* only — it never invents structure or identifiers, and
  its output never feeds the `page_id` (so idempotency holds even as prose drifts).
- **Identifier gate (hard).** Every backticked identifier in a Page body/Sources must
  **EXIST** among the sub-domain's member assets/measures. An LLM-invented column/table
  fails validation and the Page does not ship (it degrades to the deterministic stub or
  is dropped) — the 17e naming discipline, transposed to bodies.
- **The four retrieval gates (per `genie-retrieval-notes.md`, validator-enforced):**
  - **Synonyms ≥3 across the four classes** (industry acronyms / casual language /
    internal jargon / abbreviation variants). A candidate that can't reach 3 from
    deterministic sources is emitted **low-confidence + `certify=false`** (Phase 4
    Context Pack enriches later), not silently shipped as complete.
  - **Chunk-safe** — every rule sentence names its metric/table inline (no bare pronoun
    opening a rule; no reliance on the title or a previous bullet).
  - **Specificity** — ≥1 backticked identifier or literal formula per Definition/Rules
    section ("be careful with rates" is invisible to the extractor; it fails).
  - **Corroboration is structural** — Sources + Related point at the pattern (the MV /
    measures / serving Agent), **not** a second copy of the prose (MV-D27). This is the
    *retrieval* gate (Sources exist); it is distinct from the **estate corroboration
    gate** below (how many independent artifacts back the concept).
- **Estate corroboration gate (MV-D35, MV-D49 — the account-level rule).** A concept's
  `corroboration` = the count of **independent artifacts** (distinct source assets / MVs
  / Agents across the metastore) resolving to its `canonical_id`. **≥2 → full-confidence
  + certify-eligible; exactly 1 → low-confidence + `certify=false`** (a legitimate
  concept, not yet corroborated — surfaced for review, never dropped, never
  auto-certified). This is what makes a Page an **estate-level** artifact rather than a
  restatement of one measure.
- **Contradiction gate (read-only, MV-D35).** Check each draft against the space's
  current `text_instructions` by **reusing the existing conflict-surface machinery**
  (`mv_fingerprint` / the 17d ER collision path) — **not** a new comparator. A hit
  **downgrades the candidate to CONFLICT for human adjudication (17g), never
  auto-resolves and never writes back**. Pages have no read API, so Page-vs-Page
  contradiction is **not** claimed — say so, don't imply coverage.
- **Certify recommendation.** Certify requires **corroboration ≥2** AND an authoritative
  shape: `[Routing]`/`[Disambiguation]`/`[Guardrail]` certify **yes** when corroborated
  (formulas are authoritative); `[Taxonomy]` certifies **yes only** for a corroborated
  governed code list, else no; a single-artifact concept or any Recent-context overlay is
  always `certify=false`.
- **Routing validation (optional, degrades).** For `[Routing]`, optionally call
  `genie_client.run_genie_query` to confirm the NL question resolves to the intended
  measure. If Genie is unreachable on the job, mark the Page `unvalidated` and continue
  — never block the run (MV-D43).
- **Related/Sources emission (MV-D27, copy-ready).** Sources = source-table +
  metric-view FQNs (all EXISTING, identifier-gated); Related = the Agent space id/name
  + sibling-Page titles. No instruction delta — the user @-tags them in Discover.
- **Degrade, never block.** LLM down → deterministic evidence-derived stub body +
  `certify=false`; a detector/validation exception is logged and that candidate is
  skipped; Page mining is the additive-**last** step, so a failure records the run
  `failed` without corrupting the 17d/17e snapshots (already committed).

---

## 7. Persistence / DDL

**No new DDL** — `genie_ont_pages` was created (empty) in Phase 2 (`ontology/ddl.py`)
and re-grained to `metastore_id` by the re-grain phase (MV-D49). Phase 3c **populates**
it via the existing idempotent MERGE.

- **Keys (idempotent MERGE, §7 precedent, metastore grain):**
  - `genie_ont_pages` → `(metastore_id, page_id)`, with `WHEN NOT MATCHED BY SOURCE …
    DELETE` scoped to `metastore_id`, so a Page whose concept no longer has a live
    signal is removed and a re-run yields the same rows, never duplicates. `workspace_id`
    rides as provenance only.
  - `page_id = pg_<sha256(canonical_id + "|" + archetype + "|" + sorted canonical key
    identifiers)>` — **concept-anchored** deterministic signals only (never `domain_id`,
    never the LLM body), so a concept keeps one stable Page even if it spans/moves
    sub-domains — stable across runs for 17g's suppression ledger.
- **Run ledger** — populate a `page_count` (add the column to the run-ledger DDL if not
  present — this is a run-ledger metric column, not a new proposal table; still no new
  proposal DDL).
- **Firewall guard change** — in `ddl.py`, `PHASE3_TABLES` (the "never written this
  phase" set the firewall asserts) shrinks to `("genie_ont_consents",
  "genie_ont_suppressions")`; `genie_ont_pages` becomes a **written** table. Add it to
  a `PAGE_TABLES` (or extend `PROPOSAL_TABLES`) tuple so the firewall test asserts
  exactly what 17f writes.
- **Lakebase mirror** — register `genie_ont_pages` in `scripts/setup_synced_tables.py`
  (PK `["metastore_id", "page_id"]`), mirroring the re-grained 17e domains/members
  registration.
  It reads Delta-via-warehouse through the Phase-2 `mirror.py` interface until the
  synced-table flip. No new read path.

---

## 8. Batch job implementation notes

No new job task — Page mining runs **inside** `run_materialize`, **after** the 17e
clustering MERGE (the additive-final step):

1. Read the just-MERGEd Sub-Domain communities (`genie_ont_domains` where
   `parent_id IS NOT NULL`) + their members (`genie_ont_members`) + the **17d identity
   map** (`genie_ont_identity`) + the metric-view measures / column metadata for those
   members (reuse the Phase-2 reader + the fused graph already in memory). All reads are
   **metastore-scoped** (MV-D49).
1b. **Aggregate by canonical concept** — resolve each candidate signal to its
   `canonical_id`, group artifacts across **all** sub-domains in the metastore, and
   compute each concept's `corroboration` (# independent artifacts). Pick the home
   `domain_id` (strongest membership).
2. `pages.py`: per-archetype detectors over the **aggregated concept** → `PageCandidate`s
   (corroboration-gated) → LLM draft (degrades) → retrieval-gate validation →
   best-effort dedupe → deterministic concept-anchored `page_id`.
3. Expand `PageCandidate`s into `genie_ont_pages` rows and **`MERGE`** them (idempotent
   keys, §7). Set run-ledger `page_count`.
4. Degrade paths (MV-D43): LLM down → deterministic stub bodies; `ask_genie` down →
   skip routing validation; an empty/trivial estate → zero Pages (MERGE clears any
   stale rows), run still `succeeded`.

---

## 9. Frontend wiring

**None.** No route, no model, no TS, no component. The `17.0e` Page draft renders in
17g (which adds the `/drafts` route + serving).

---

## 10. Grants / deploy (DABs)

- **No new system-table grant** — Page mining consumes signals already read (measures,
  column metadata, lineage, instructions).
- **No new job, no new env var** — runs in the existing `ontology_materialize` task
  (`GSO_ONT_JOB_ID` reused).
- **No dependency change** — `uv.lock` untouched (MV-D45); no new pin.
- **Synced tables** — add `genie_ont_pages` to `scripts/setup_synced_tables.py`.

---

## 11. Tests (offline, `backend/tests/` + GSO `tests/unit/`, run via `./scripts/test.sh`)

All acceptance is **offline** — detectors are deterministic and run in-process; the
drafting LLM and `run_genie_query` are **mocked**; no cluster, no Lakebase Search, no
live Genie.

- **Contract-frozen guard** — Phase-1/2/3a/3b response models byte-identical; **no**
  new API model; taxonomy/tags/refresh routes unchanged.
- **Per-archetype detectors on a fixture** — the architecture worked example
  (`finance.sales.order_revenue` with `total_revenue` + `discount_rate`, two Agents
  defining "revenue" differently) yields: a `[Routing]` Page per **canonical measure
  concept** (measures resolving to the same `canonical_id` collapse to ONE Page), a
  `[Guardrail]` Page for `discount_rate` (percentage/AVG-of-rate), and a
  `[Disambiguation]` Page from the CONFLICT fingerprint; a low-cardinality coded column
  yields `[Taxonomy]`.
- **Canonical-concept keying (MV-D49)** — two measures in **different sub-domains** that
  resolve to the same 17d `canonical_id` collapse to **one** Page (not two); `page_id`
  is derived from `canonical_id` (not `domain_id`), so moving a concept's home sub-domain
  leaves `page_id` unchanged. Sources aggregate **all** contributing artifacts.
- **Estate corroboration gate (MV-D35)** — a concept backed by **≥2 independent
  artifacts** is full-confidence + `certify`-eligible; a **single-artifact** concept is
  emitted **low-confidence + `certify=false`** (not dropped, not certified).
- **Metastore grain (MV-D49)** — `genie_ont_pages` keyed `(metastore_id, page_id)`; the
  MERGE delete predicate is metastore-scoped; `workspace_id` present as provenance only,
  never in the key (assert generated SQL / row keys).
- **Identifier gate** — a draft that backticks an **invented** column fails validation
  (degrades to the stub / is dropped); Sources FQNs must all EXIST in the members.
- **Retrieval gates** — synonym-count/class failure flagged low-confidence + `certify
  false`; a chunk-unsafe rule (bare pronoun) is rejected; a vague rule with no
  backticked identifier fails specificity.
- **Contradiction gate** — a draft contradicting existing `text_instructions` is
  downgraded to **CONFLICT** and is **NOT written back** (assert no instruction write
  path is touched); reuse of the conflict machinery (not a new comparator) is asserted.
- **Related/Sources (MV-D27)** — `[Routing]` emits the MV + measure as Sources and the
  Agent in Related; `[Guardrail]` emits numerator/denominator measures as Sources.
- **Certify rule** — formula archetypes → `certify=true`; a Recent-context overlay /
  non-governed `[Taxonomy]` → `certify=false`.
- **Naming/drafting degrade** — with the LLM mocked to fail, Pages get deterministic
  stub bodies + `certify=false` and the run still succeeds; `run_genie_query` mocked
  unreachable → routing Page marked `unvalidated`, run continues.
- **Determinism / idempotency** — running the materializer twice over the same fixture
  yields the **same** `genie_ont_pages` rows (stable concept-anchored `page_id` from
  deterministic signals only — assert the id is invariant when the mocked body text
  changes **and** when the concept's home sub-domain changes; no dups; a Page whose
  concept loses all signal is deleted via `NOT MATCHED BY SOURCE`).
- **Firewall (updated)** — `test_ontology_firewall.py`:
  - the write guard now allows `genie_ont_pages`; `genie_ont_consents`/`_suppressions`
    are still **never** written.
  - still **no** `SET/UNSET TAG` / `CREATE GOVERNED TAG` / `manage_uc_tags` /
    `web_search` anywhere; **no Agent-instruction write path** (MV-D27); `lakebase_*`
    still confined to `similarity.py`.
  - `LeakageOracle` page-body scan is present (the extended oracle, not a second
    scanner).
- **Additive safety** — a Page-mining exception records the run `failed` but leaves the
  tag_graph / taxonomy / identity / domains / members snapshots (written earlier in the
  run) intact.

---

## 12. Definition of done & explicit deferrals

**Offline done (the agent stops here) when:** `pages.py` + the materializer wiring +
the `LeakageOracle` page-body extension land in the GSO wheel; `genie_ont_pages`
populates idempotently at **metastore grain** (`(metastore_id, page_id)`, MV-D49) with
**canonical-concept-anchored**, **corroboration-gated**, gate-validated, evidence-backed
archetype Page proposals (copy-ready Related/Sources aggregating all contributing
artifacts, deterministic concept `page_id`); the taxonomy/tags/refresh routes and all
contracts are provably unchanged; and `./scripts/test.sh` + `cd frontend &&
npm run lint` + `tsc` are all green — including the per-archetype detector,
canonical-concept keying, estate corroboration gate, metastore grain, identifier gate,
the four retrieval gates, contradiction→CONFLICT, certify rule, drafting-degrade,
determinism/idempotency, additive-safety, and updated-firewall tests. `uv lock --check`
passes (lockfile untouched — MV-D45).

**Deploy-gated (human, after the offline run — the agent must NOT do these):** run the
`ontology_materialize` job once against the live workspace (via "Refresh ontology" or a
manual run) and confirm `genie_ont_pages` populates in Delta + the synced mirror with
sensible archetype Pages under their Sub-Domains. **Note:** there is **no UI change** in
17f — Pages are verified by querying the table / job output, not the page (the drafts
render in 17g). The SP reads, the drafting LLM + `run_genie_query`, the Delta write, and
the mirror can only be validated in a deployed app — that boundary is why 17f's offline
slice stops before the live run.

**Explicitly deferred (do NOT pull forward):**

- **MV / Agent advisories as a persisted/served surface** (MV-D37). MV dupe/quality is
  already surfaced by 17d's identity map and Agent overlap by 17e's domain evidence; a
  dedicated advisory table/contract is an **owner decision** (needs a new table or a
  serving shape) — deferred to 17g. **Owner: confirm whether you want a thin advisory
  surface folded into 17f, or kept in 17g.**
- **17g (Phase 3d)** — L6 rank/trust (`score` the Pages), the suppression *filter* at
  serve time, the `/drafts` route, the `17.0d/e` frontend, and reassignment
  adjudication (STOP checkpoint). **No `/drafts`, no ranking, no frontend in 17f.**
- **Phase 4 (17h)** — the external Context Pack vocabulary/overlay prior for Page
  synonyms + Recent-context. **Phase 5 (17i)** — the consented `SET TAG` apply. 17f
  never mutates a governed tag or an Agent instruction.
