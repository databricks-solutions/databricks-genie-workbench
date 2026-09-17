# Ontology + MV-Advisor — single ordered backlog

One drivable list across **both** tracks in `docs/design/`. Reconciled against code on
branch `ontology` (**2026-09-17**, post P1 harness + P6 Signal Authority Stages 1–4 deploy-verify). This is the sequencing
source of truth; the per-phase build specs / drivers remain the *content* source of truth,
and `mv-advisor-playbook.md` remains the MV-D register.

**Doc layout:** landed build/driver specs are archived under `docs/design/implemented/`;
older mv-advisor origin analysis under `docs/design/reference/`; §8 render scorecards under
`docs/design/reviews/`. Only **unbuilt** specs + **living** docs (this backlog,
`mv-advisor-playbook.md`, `ontology-engine-architecture.md`, `ontology-map-DESIGN.md`,
`page-archetypes.md`, findings/notes) remain at the `docs/design/` root.

## Status legend

| Tag | Meaning |
|---|---|
| ✅ BUILT | Code landed on `ontology`; deploy-verified |
| 🟡 BUILT-OFFLINE | Code landed + offline-green; **deploy-verify pending** |
| 📝 DRAFTED | Build spec **and** driver exist, unbuilt (Goal-Mode-ready) |
| ✏️ UNDRAFTED | Needs a build spec + driver before any build |
| 🧊 HISTORICAL | Superseded / reference only |

---

## What's DONE (context, not backlog — specs archived under `implemented/`)

**Batch proposal engine — ✅ BUILT + deploy-verified.** Curation redesign Stages 1 / 2 /
3 / 3.1 / 3.2, Stage 4 Pages, and 4.1a–4.1f + 4.1g/4.1h/**4.1i** (MV-D51–D70). The
batch-timeout saga is **closed** (4.1f/MV-D68, run `31641283089969` in **20.8 min**);
batch page-body drafting yield is **complete** (4.1i/MV-D70, `llm_auto` 26/26). Snapshot on
the airline estate: surfaced Domains + 641 Pages, `certify` working.

**Ontology Map ("Estate Graph", = Phase 3e / 17k) — ✅ BUILT + deploy-verified** on
`fevm-serverless`. Shipped as **d3-hierarchy + thin SVG** (MV-D84 — the Cytoscape/Sigma
bakeoff was superseded), not the old LOD segmented-control model:
- **Data contract** — Lane D (MV-D82): org root + `parent_id`/`attach_level` + per-edge
  `verb`/`rel_class`; Lane D2 (MV-D86): node `description` + compact `meta` + deeper
  containment; Lane E (MV-D88): per-edge `detail` evidence bag (merged; surfaced via the
  hover tooltip).
- **Renderer** — Lane R (MV-D81/D83): React-controlled SVG tidy-tree, expand/collapse,
  typed verb overlay, Ungrouped tray + dashed "Suggested" hulls; Lane P (MV-D85) polish;
  Lane P2 (MV-D87) navigation + relationship legibility (incl. the parent-domain-closure
  dangling-`parent_id` fix).
- **Visual system** — v2 shell (MV-D73/74/75), v3-fable harness + theme tokens
  (MV-D77/78/79/80): dual-theme `graphTokens`, dev Vite harness + headless-browser diff loop.
- **Typed estate assets** — Stages 1/2/3 (MV-D89/90/91/92): typed display-kinds (table /
  metric_view / measure / agent / dashboard), MV double-emit dedupe, Genie Agents +
  Dashboards placed by their APPLIED governed tag via the `entity-tag-assignments` API.
- **Interactions** — buttery-interactions (O(1)/frame imperative drag + zoom, rAF glide) +
  UX papercuts (`useTheme` live-flip, `FreshnessControls` reload-on-refresh-complete).

**Foundations — ✅ BUILT + deploy-verified.** Phase 1 spine · Phase 2 batch + Lakebase
mirror · Phase 3a signal graph + ER · 3b clustering · 3c page miners · 3d rank + serve ·
Re-grain to metastore (MV-D49) · OBO-first foundations (MV-D50).

---

## The ordered backlog (drive top-to-bottom)

### P0 — Map-interaction polish loop · ✅ DONE (deploy-verified 2026-09-09)
1. **Map interaction + visual fixes (#1/#2/#3)** · ✅ BUILT + deploy-verified (2026-09-09)
   - Landed on `ontology` (629 frontend tests green, `tsc`/eslint clean, prod build OK),
     frontend-only, additive:
     - **#3 tier contrast** — `graphTokens.ts` light-theme container ramp widened to three
       clearly-stepped navies (`#0B2026`→`#234A57`→`#35617A`, ≥~1.4:1 each step) + ring tint
       lifted; hue stays reserved for type (§9-B).
     - **#2 relationships-on-focus** — `ontologyTreeLayout.ts` cross-links now draw only on a
       node/verb selection (nothing at rest; `+N links — select a node to trace` affordance)
       + background-click-to-deselect.
     - **#1 deterministic click model** — `EstateGraph.tsx`: click = select + expand/collapse
       (functional toggle, no stale-closure flip-flop); the confusing "double-click dumps
       assets" 3-state cycle is gone; a domain's direct assets now reveal via an explicit
       "Show N direct assets" control in the inspector.
   - **Done:** deploy-verified 2026-09-09 on `fevm-serverless` (app RUNNING); the frontend map
     deltas are committed on `ontology` (`67ad4cff`, alongside the earlier `6eb20d16`).
2. **Map relational-line detail (#4)** · ✅ BUILT + deploy-verified (2026-09-09)
   - Landed on `ontology` (wheel 346 ontology tests + frontend 150 green, `tsc`/eslint clean),
     additive + reveal-don't-invent:
     - **Producer** — `schema_signals.fk_edges`/`shared_join_column_edges`/`join_key_edges`
       thread the join **column name(s)** end-to-end (`(a,b,weight,source,columns)`);
       `graph.add_edge`/`build_signal_graph` set `edge["columns"]` (empty ⇒ omitted);
       `layout._edge_detail` already surfaced `columns` + gained `dashboard_scope → {role:"reads"}`.
     - **Inspector** — `InspectorRelationship.detail` + `GraphInspector` render the evidence
       bag inline per relationship row (an FK now reads `shares key · route_id`), not only in
       the fleeting hover tooltip.
   - **Done:** re-materialized 2026-09-09 (run `1018106709723767`, SUCCESS); FK column names are
     live in the snapshot (`detail.columns`, e.g. `aircraft_id`) and render inline in the
     inspector. Committed on `ontology` (`67ad4cff`).

### P1 — Quality scoreboard (gates all later tuning) · ✅ DONE (deploy-verified)
3. **§10 Evaluation & trust harness (MV-D59)** · ✅ BUILT + deploy-verified (`8c6af04e`)
   - **Scorer — BUILT** (`7120a6df`, 28 tests): `ontology/eval_harness.py`
     (`assemble_eval_report` + `compare_reports`). Driver of record
     `ontology-eval-harness-driver.md`.
   - **Wiring & gate — BUILT + deploy-verified** (`8c6af04e`): read-only reader
     (`genie_ont_domains`/`genie_ont_members` → scorer shape), runnable
     `jobs/run_ontology_eval.py` (report JSON, no new table — MV-D49), baseline-vs-current
     gating, `genie_ont_eval` results, optional industry gold reference. Wheel-only, additive,
     no new dep. The gate is **LIVE** — every P6 Signal Authority stage recorded its
     before/after on it.
   - **Done:** the harness reads materialized runs and gates real changes (it caught the
     Stage-1 junk-surfacing regression, precision 1.00→0.426, forcing the rank-only fix).

### P2 — The one write path — **THE VALUE-UNLOCK**
4. **Phase 5 / 17i — consented `SET TAG` apply (L9)** · 🟡 OFFLINE SLICE BUILT · live apply human-gated
   - Offline slice **LANDED** (`9e1a82c4`): `apply.py` service + `apply/preview` +
     `apply/execute` routes; firewall tests assert no writes by default. **Remaining:**
     `execute` identity wiring + `ApplyPreview.tsx` + the *live* apply (dry-run-first,
     default-OFF, OBO-attributed, request-time only — never in the batch job).
   - Specs: `ontology-phase5-apply-{build,driver}.md` (MV-D37/D26/D50/D23/D27). Consumes the
     17g `genie_ont_consents` ledger (✅ built); prereqs (re-grain / OBO / `certify`) all shipped.
   - **Why now:** the engine lands a trustworthy set — apply is the gap between "discovery
     aid" and "ontology builder."
   - **Next action:** finish the `apply.py` execute path + `ApplyPreview.tsx` → STOP at the
     apply-safety checkpoint → human deploy-verify.

### P3 — Curator enrichment loop (finish the review UX)
5. **Stage 4.1d Steps 2–4 — curator Draft-with-AI** · 🟡 PARTIAL
   - **Step 2** (`body_source` preservation across re-materialize) — **BUILT** (`7bc610c9`).
   - **Steps 3 & 4** — 📝 DRAFTED: on-demand single-Page "Draft with AI" (OBO) and bulk
     "Draft this sub-domain with AI" (OBO). Drivers `…-stage4.1d-step3/step4-driver.md`
     (+ `ontology-stage4.1d-step34-backend-driver.md` backend half); build spec
     `ontology-curation-redesign-stage4.1d-build.md`.
   - **Next action:** build Steps 3 & 4 (backend routes + `PageDraftCard` actions).

### P4 — External enrichment
6. **Phase 4 / 17h — external Context Pack + §9 industry alignment** · 📝 DRAFTED (Stage A build-ready)
   - Build spec `ontology-phase4-external-build.md` (§1→§12), staged **A→B→C** with a human
     STOP between each, all DEFAULT OFF (MV-D44), estate-only byte-identical when off.
   - **Stage A (safe backbone) — 🟡 BUILT-OFFLINE:** `context_sources.py` registry +
     firewall-by-class + capability probe + `external_context` config + real tier-5 banner
     (**NO egress**, DEFAULT OFF, byte-identical when off; 2907 backend tests green). MV-D47
     `web_search` is named as a registry **securable only** (narrow `_WEB_SEARCH_NAMING_ALLOWED`
     exemption in `context_sources.py`); the Stage-B egress path stays token-banned everywhere else.
   - **Stage B (resolver + egress + influence) — ✅ BUILT + deploy-verified (2026-09-15):**
     `ontology-phase4-external-stageB-driver.md` — Context Pack resolver (batch identity) + web
     search (AI-Gateway MCP `system.ai.web_search` + fallback ladder) + `Provenanced<T>`
     self-validation + the two read-only plug-points (naming/gap hypotheses + Page Recent-context)
     + additive DDL (`genie_ont_context_pack`/`_context_sources`), DEFAULT OFF. Single-sources the
     registry + firewall into the **wheel** (the resolver can't import `backend.*`), then builds on them.
     - **Deploy-verify (`fevm-serverless`, Alaska Airlines, tier ON):** run `826181768666957` /
       ontology run `a5225591…` SUCCESS in 30.3 min. Pack written: `industry_code=481111`
       (Scheduled Passenger Air Transportation), industry label T2-sourced; **26 sourced leaves /
       3 URLs** in the egress log; `canonical_domains[0].is_template=true` (firewall held); estate
       byte-stable (2642 tags · 134 domains · 378 pages · 1995 identities). OFF ⇒ empty pack, no egress.
     - **Live fix (`69bf9ec6`):** the managed `system.ai.web_search` MCP returns a synthesized
       **markdown answer + citations**, not a JSON hit array — `web_search._extract_hits` now parses
       inline `[title](url)` citations (uncited ⇒ [] ⇒ degrade), which is what turned the positive
       path on. Committed with 2 unit tests against the captured live payload.
   - **Stage C (user surface) — 📝 DRAFTED (build-ready):** `ontology-phase4-external-stageC-driver.md`
     — render-only over Stage B's persisted pack: per-source Context Sources panel in the banner
     (`PermissionBanner` renders the tier-5 `sources` Stage A already returns) + one opt-in Settings
     toggle + a labeled/dated Sources chip on a suggestion whose name came from the pack
     (`mirror.py` reads `evidence.rank.naming_prior`) + `GRANT EXECUTE`/OAuth wiring. Additive,
     read-only, DEFAULT OFF ⇒ byte-identical. Fits the 4000-char Goal-Mode limit (3931).
   - **§9 industry alignment (MV-D58):** `ontology-industry-alignment-driver.md` (drafted) —
     *consumes* the Stage B pack seam; schedule after Stage C.
   - **Next action:** build **Stage C** (Goal Mode) → STOP → deploy-verify; then wire **§9 industry
     alignment** onto the live pack seam.

### P5 — Track hardening (needs drafting)
7. **17j — Ontology hardening + E2E** · ✏️ UNDRAFTED
   - Register entry only: hardening + E2E, undo/rollback of an applied membership, docs /
     changelog / PR. **Next action:** draft `ontology-17j-hardening-{build,driver}.md`; run
     after 17i lands (rollback needs the `genie_ont_applied` audit rows).

### P6 — Signal authority (OntoRank-style) · ✅ BUILT + deploy-verified (Stages 1–4)
8. **Ontology Signal Authority (MV-D93–D97)** · ✅ BUILT + deploy-verified on tbzqg7
   - `ontology-signal-authority-build.md`: wired `system.query.history`+`table_lineage`
     popularity into the reserved `usage×centrality×governance` blend; fed certification into
     the authority rung (+ `deprecated` firewall); upgraded degree→`igraph` PageRank +
     certified-seeded assignment + usage-weighted clustering; encoded popularity=size /
     certified=ring on the map. Additive/read-only; reused GenieWatch SP plumbing + the
     `igraph` already lazy in `cluster.py` (no new dep). Each stage recorded before/after on
     the P1 harness (MV-D59).
   - **Stage 1** (popularity, MV-D94) + curated-groundtruth harness refinement — ✅ deploy-verified
     (precision 1.00 / F1 0.872, run `543453527733713`). Drivers `…-stage1{,b}-driver.md`,
     `ontology-eval-curated-groundtruth-driver.md`.
   - **Stage 2** (certification authority + `deprecated` firewall, MV-D95) — ✅ deploy-verified
     (`aaba5214`, tbzqg7 run `471095489310894`). Driver `…-stage2-driver.md`.
   - **Stage 3** (MV-D96) — ✅ deploy-verified, three sub-parts: **3a** PageRank centrality
     (`09491fbd`, run `938908355620342`), **3b** certified-seeded PPR assignment (`6c5ce11e`,
     run `898722223308820`), **3c** usage-weighted clustering + PageRank sub-domain hubs
     (`612d1047`, run `441962027551109`). Drivers `…-stage3{,b,c}-driver.md`.
   - **Stage 4** (visual encoding: popularity=size, certification=ring, MV-D97) — ✅ deploy-verified
     (`49afccf4`, run `742471949652671`; 17 certified rings live, harness flat). Driver `…-stage4-driver.md`.
   - **Stage 4b** (thread PageRank centrality → `node_scores` → size) — ✅ deploy-verified
     (`b2da7c2b`, tbzqg7 run `1076509192463517`): backend-only follow-on — Stage 4 confirmed asset
     `size` rendered uniform because `materialize` passed `node_scores=None`; 4b computes
     `pagerank_centrality` once and re-keys it into `node_scores` (`asset:<fqn>`) so hubs read bigger.
     Live: asset `size` distinct **1→5** (FK-spine hubs `format` 1.5 / `location` 1.38 at the top;
     127 leaves stay 1.0 — the `max(1.0, 0.5+score)` curve only lifts centrality > 0.5), harness
     **flat** (P1.00/R0.923/F0.96). Driver `…-stage4b-driver.md`. Signal Authority arc COMPLETE.

---

## Track A — MV-Advisor / Semantic Graph (parallel, independent of ontology)

9. **Semantic Blueprint v4 — Join-Advisor candidate source** · 📝 DRAFTED (feature BUILT behind flag)
   - Built: `SemanticBlueprint.tsx` + `blueprint/` modules + Phase-2 backend, behind the
     `blueprint` canvas toggle. **Deploy-gated remainder:** server-side FK / name-type
     discovery + containment probe + wiring `onSeed` to a real Auto-Optimize run. Specs
     `semantic-graph-v4-build-prompt.md` / `…-blueprint-note.md`.
   - **Next action:** build the candidate-source backend + `onSeed` → deploy-verify.
10. **MV-Advisor main track — HEAD deployment review** · ✅ BUILT (verification gap)
    - `reference/mv-advisor-gap-report.md`: code-complete through create-and-attach / Genie-v2
      round-trip + Blueprint, but **no deployment review of current HEAD**.
    - **Next action:** a deployed human-review round on current HEAD (not a build).

🧊 `semantic-graph-v2-note.md`, `semantic-graph-v3-note.md` — superseded by v4.
🧊 `ontology-frontend-batch-driver.md`, `ontology-wave2-launcher.md`,
`ontology-wave3-launcher.md` — historical Wave-2/3 orchestration; the work re-sliced into the
per-lane drivers now in `implemented/`. `ontology-phase3e-{build,driver}.md` — the estate
graph shipped via the map lanes (d3/SVG, MV-D84); kept as the original spec of record.

---

## Scale hardening (enterprise levers — context, not yet scheduled)

The architecture bounds the problem by design — reads are **catalog-allowlist scoped**, the
served graph is **capped at `TOP_N_BY_CENTRALITY = 2000`** display nodes, the tree is
**progressively disclosed** (default `Estate→Domain→Sub-domain`, per-parent `+N more`,
relationships-on-focus), and reads are **Lakebase-mirrored + TTL-cached**. Two levers remain
for true-enterprise estates, neither yet scheduled:

1. **Batch runtime + LLM cost → catalog sharding.** The batch materialize is the time/cost
   cliff (~21 min, 641 Pages on the airline estate). For 10×+ estates, materialize
   **per-catalog and union** (and/or raise the job timeout deliberately).
2. **Render → virtualization.** The buttery-interactions work fixed per-frame drag/zoom cost;
   if routine thousand-node expansion near the 2000 cap is needed, add **viewport
   virtualization / LOD** (WebGL renderer only if that isn't enough).

---

## Suggested execution order

Batch engine + Ontology Map are **done**, the **P0 map-interaction/#4 pass is
deploy-verified + committed** (`67ad4cff`), **Phase 4 Stage B is deploy-verified** (`69bf9ec6`),
**P1 (the §10 harness, MV-D59) is LIVE + deploy-verified** (`8c6af04e`), and **P6 (Signal
Authority, MV-D93–D97) is fully deploy-verified** (Stages 1–4; Stage 4b in flight). **Next: (P2)
Phase 5 apply** — the value-unlock that closes the curator loop (its offline slice already
landed). Then **P3** (curator Draft-with-AI Steps 3–4), the rest of **P4** (Stage C + §9
alignment), **P5** (hardening). **Track A** can run in parallel by anyone off the ontology branch.
