# Ontology + MV-Advisor — single ordered backlog

One drivable list across **both** tracks in `docs/design/`. Reconciled against code on
branch `ontology` (**2026-09-21**, post P1 harness + P6 Signal Authority Stages 1–4b + P2 Phase 5 apply Stage 2 + P5/17j apply hardening+undo + MV-D101 reuse no-op Drafts gate + P3 4.1d Draft-with-AI + Stage 4.1j Related-assets/Links (MV-D102/D103) + Stage 4.1k page↔page relevance rank/cap (MV-D104) + MV-D105 signal-graph edge coverage Phases 0–3 (lineage/co_query producers live, semantic_sim default-off) + MV-D106 Ontology Map render virtualization (viewport culling, Phase 1) + P4 external enrichment COMPLETE incl §9 alignment (MV-D58) — all deploy-verified; SHAs re-verified on-branch this date). This is the sequencing
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

### P2 — The one write path — **THE VALUE-UNLOCK** · ✅ DONE (deploy-verified 2026-09-18)
4. **Phase 5 / 17i — consented `SET TAG` apply (L9)** · ✅ BUILT + deploy-verified
   - **Stage 1 — offline slice LANDED** (`9e1a82c4`): `apply.py` service + `apply/preview` +
     `apply/execute` routes; firewall tests assert no writes by default.
   - **Stage 2 — harden + go-live BUILT + deploy-verified** (`cf92ef58`; driver `518f949d`):
     governed-tag statements injection-safe by backtick-escaping (FQN/key/value are identifier
     positions) + `VALUES('…')` literal for CREATE; audit INSERT + consent UPDATE fully bound via
     `StatementParameterListItem`; `grants.membership_write_probe` (read-only `get_effective`,
     fail-soft per MV-D43) wired per-item so the preflight `membership_write` tier is real
     (ok/blocked + copy-ready GRANT lines); `grants.current_tag_value` drives the add-vs-move
     diff; OBO write / SP bookkeeping split reconciled (firewall guard test). `ApplyPreview.tsx`
     + `ApplyDiff.tsx` render the plan. Default-OFF; execute gated on `confirm=true` + `plan_hash`.
   - **Deploy-verify (`fevm-serverless` / 6t92c3, active deployment 2026-09-18T20:18:28Z):** app
     RUNNING, clean startup, all 9 ontology routers mounted; ontology routes respond **200 live
     under OBO**. Governed-tag SQL contract verified on the app's own workspace/metastore —
     `CREATE GOVERNED TAG … VALUES('…')` (NOT `WITH ALLOWED_VALUES`), `GRANT ASSIGN ON GOVERNED
     TAG`, and the full `SET`/`UNSET TAG` + `information_schema.*_tags` read-back round-trip all
     pass; `get_effective` probe readable.
   - Specs: `ontology-phase5-apply-{build,driver}.md` + `ontology-phase5-apply-harden-driver.md`
     (MV-D37/D26/D50/D23/D27). Consumed the 17g `genie_ont_consents` ledger (✅ built).
   - **Scoped remaining → P5 / 17j:** a full UI-driven E2E apply (preview → confirm → execute
     against a real asset) + undo/rollback of an applied membership (needs `genie_ont_applied`
     audit rows). The write path is live and SQL-verified; E2E + undo are the 17j deliverables.

### P3 — Curator enrichment loop (finish the review UX)
5. **Stage 4.1d Steps 2–4 — curator Draft-with-AI** · ✅ BUILT + hardened + deploy-verified (6t92c3, 2026-09-19)
   - **Step 2** (`body_source` preservation across re-materialize) — **BUILT** (`7bc610c9`).
   - **Steps 3 & 4** — **BUILT** (`4b9b21de` backend `draft_body.py` + the 3 OBO routes/models;
     `a2e45cac`/`4e1832b7` frontend "Draft with AI" / "Draft pages with AI" + bulk progress):
     on-demand single-Page draft and bulk "Draft this sub-domain with AI", both OBO, both
     reusing the wheel drafter + gates (MV-D65/D66). (The `…-stage4.1d-step3/step4-driver.md`
     drivers + `ontology-curation-redesign-stage4.1d-build.md` build spec describe this shipped
     code.)
   - **Hardened (this branch, offline-green):** a `/review` (Bugbot) pass on the shipped code
     found 5 issues, all fixed — (1) `draft_one` read the whole `genie_ont_pages` table instead
     of the target row (→ 500); (2) `facts_hash` recomputed from the body instead of preserving
     the batch hash (false staleness next re-materialize); (3) on-demand skipped `_canonical_body`
     (MV-D70), so a paraphrased-away identifier was rejected not salvaged; (4) the routes could
     500 instead of degrading; (5) bulk stamped `llm_ondemand` not `llm_bulk`. A **re-review** then
     caught a **6th** (the load-bearing one): the evidence `UPDATE` emitted Spark's
     `CAST(map/struct AS STRING)` display form — NOT JSON — into the JSON `evidence` column and
     set a non-existent `updated_at` column, so the write never actually persisted / would corrupt
     evidence; now the merge is done in Python and the whole evidence JSON is bound as a parameter
     (no phantom column). A **live deploy-verify** on 6t92c3 then surfaced a **7th**: the on-demand
     identifier gate ran against `frozenset(source_fqns)` only, so a Routing page that legitimately
     cites its metric-view **measure name** (not a Source FQN — the batch admits it via
     `build_universe`'s `m.name`) was falsely rejected as an invented identifier. Fixed by grounding
     the on-demand universe in `source_fqns ∪ related_fqns ∪` the backticks already proven in the
     persisted stub body (emitted by `_stub_body`, gate-passed at materialize) — honest, nothing
     outside the gate-proven set is admitted. +9 behavioral/SQL-shape tests
     (`test_ontology_draft_body.py`) reproduce each. `./scripts/test.sh` **3115**.
   - **Deploy-verified** (6t92c3, 2026-09-19): single-page "Draft with AI" flips
     `genie_ont_pages.body_source` `stub → llm_ondemand` with valid-JSON `evidence`,
     `body_stale=false`, and the batch `facts_hash` preserved (the 7th-finding fix landed live).
     Optional remaining check: the bulk "Draft pages with AI" path (`llm_bulk`).

6. **Stage 4.1j — Related-assets & links (graph traversal)** · ✅ BUILT + deploy-verified (6t92c3, 2026-09-20; MV-D102/D103 registered)
   - **Gap:** page `related_fqns` is hardcoded to the serving Genie Agent (pages.py:748-749 et al.),
     so measure Pages render an empty Related section — even though a full weighted, typed,
     provenanced relatedness heterograph (`graph.build_signal_graph`, graph.py:33) and PageRank
     over it (`graph.pagerank_centrality`, graph.py:261) are already computed EVERY run and the
     miner (`mine_pages`, pages.py:1233) never receives either. Per the
     [Databricks Pages model](https://docs.databricks.com/aws/en/uc-semantics/pages), Related
     assets (parent/child Pages, dependent metrics, associated tables) + Sources (incl. external
     links) are first-class fields we leave empty. "Draft with AI" only paraphrases the same facts,
     so it adds no new information — this does.
   - **Approach (deterministic-first):** traverse the graph from each page's `source_fqns` anchor
     over the relatedness edge kinds (join_key / lineage_adjacency / co_query / mv_membership /
     semantic_sim / dashboard_scope / agent_scope), rank by `weight × kind-prior × centrality`,
     surface top-N each with a per-edge "why" (join_key even names the shared column, MV-D88).
     Precedent: `_domain_adjacency` (materialize.py:531) already traverses the same graph. Related
     assets need NO LLM (higher trust than the prose); external links stay best-effort / labeled /
     leakage-scanned (17.x web-enrichment).
   - **Driver:** `docs/design/ontology-related-assets-links-driver.md` (Builds A–E; proposed
     **MV-D102** graph Related assets, **MV-D103** external Links + Copy-for-Discover surfacing).
     `signal_graph=None` ⇒ byte-identical agents-only (MV-D43); harness flat-or-up (related_fqns
     changes NO domain/page set).
   - **BUILT (offline-green, 2026-09-19):** BUILD A hoists `pagerank_centrality` above `mine_pages`
     (byte-identical; feeds Related scoring + rank + node-sizing); BUILD B adds the pure igraph-free
     `graph.related_assets` traversal (weight × kind-prior × centrality-floor, dedupe keep-max,
     per-kind why incl. the join-key column); BUILD C threads `signal_graph`/`centrality` into
     `mine_pages` + a page↔page post-pass (same-sub-domain siblings + linked-domain pages, by title);
     BUILD D adds best-effort `evidence.links` (bounded, labeled "not certified", leakage-scanned,
     degrade-to-[]), gated on the external-context flag (job wires the searcher); BUILD E surfaces
     both via `PageLink`/`links` (models.py + types.ts + mirror `_page_links`) and a Links section +
     Related/Links blocks in Copy-for-Discover. `signal_graph=None` ⇒ agents-only byte-identical;
     `page_link_searcher=None` ⇒ no links. Suites: 3133 (1094 backend + 2039 GSO) + 660 vitest,
     tsc/eslint clean, uv.lock clean.
   - **Deploy-verified (6t92c3, 2026-09-20):** materialize run `611356940571419` TERMINATED
     SUCCESS re-baked `genie_ont_pages` — `related_fqns` **0/642 → 644/644** (max 12),
     `evidence.asset_why` populated; traversal proven live (**161** "Shares join key" + **159**
     "governed dashboard" + 331 serving-agent + 641 sibling whys); harness precision/recall/F1
     flat (None→None, no aligned reference). `links=0` (external-context flag off — default-safe).
     **MV-D102/D103 registered.** NOTE: the app-facing rows are keyed on `workspace_id` +
     `metastore_id` (MV-D49) — the verify re-triggered with the app's `run_now` recipe after a
     first CLI run with the job's empty default `workspace_id` was a no-op on the app dataset.
   - **Stage 4.1k — page↔page Related relevance rank/cap (MV-D104):** ✅ BUILT + deploy-verified
     (6t92c3, 2026-09-20; commit `92f3b28d`). `_rank_sibling_pages` (pages.py:1242) ranks
     same-sub-domain siblings by shared-Source count → corroboration → title; a relevance gate keeps
     only source-sharing siblings EXCEPT an isolated-Page floor (always keep the top-ranked one); a
     separate `max_sibling_pages=2` (pages.py:1268) caps them; linked-domain pages keep priority and
     are exempt from that cap (pages.py:1320-1345). Membership-neutral (only `related_fqns`/
     `asset_why`). Offline 3138 (1094 backend + 2044 GSO, +5) + 660 vitest; `git status -- uv.lock`
     clean. Driver: `docs/design/ontology-related-siblings-rank-driver.md`.
     **Deploy-verified:** materialize run `876390131182810` TERMINATED SUCCESS re-baked
     `genie_ont_pages` (644 pages / 167 domains) via the app `run_now` recipe (MV-D49). The per-page
     sibling cap holds UNIVERSALLY: `sib_max=2` for every archetype incl. **Routing (436 pages)**,
     histogram `{0:8, 1:22, 2:614}` (no page >2), 0 invented/empty related entries — the Routing
     domination (up to 6 arbitrary siblings) is GONE and the kept siblings are relevance-ranked.
     Harness P/R/F None→None (alignment off — see runtime note), structure flat. CAVEAT: the estate
     drifted since the 4.1j run (join-key whys **161→738**, dashboard **159→237** — signals 4.1k does
     NOT touch), so the global sibling total (641→1250, ≈2/page under the floor) is not a like-for-like
     proxy; the per-archetype **`sib_max=2`** is the authoritative acceptance.
   - **Follow-up (a):** ✅ RESOLVED by Stage 4.1k (MV-D104) — the bulk `Routing` sibling domination is
     now ranked/gated/capped (per-page `sib_max=2`, verified live).
   - **Follow-up (b) — signal-graph edge coverage → driver `docs/design/ontology-signal-edge-coverage-driver.md`
     (proposed MV-D105, cheap-first phased, harness-gated).** CORRECTED triage (2026-09-20, from the
     live graph snapshot of run `876390131182810`) — the four "0 related whys" kinds are NOT one problem:
     - **`mv_membership` — ✅ CLOSED (Phase 0 verified, commit `bf129c17`).** The estate has **55 metric
       views** and the fused graph carries **32 `mv_membership` edges** — the map is WIRED + non-empty,
       NOT a data gap. It still yields 0 *page* whys because of an anchor/namespace mismatch: edges are
       `mv:<mv_fqn>` (hub) → `asset:<source_table>` (graph.py:197-202) while `related_assets` anchors a
       measure Page on `asset:<mv_fqn>` (graph.py:473). → a SMALL `related_assets` anchor fix to surface
       the existing edges, NOT missing data.
     - **`lineage_adjacency` — ✅ BUILT + deploy-verified (Phase 1, commit `f84f3def`).** Replaced the
       hardcoded `return []` stub (run_ontology_materialize.py:616-618) with a real 30-day
       `system.access.table_lineage` producer delegating to pure `schema_signals.lineage_adjacency_edges`.
       **Live: 723 "Connected in table lineage" whys on 285/644 pages** (was 0).
     - **`co_query` — ✅ BUILT + deploy-verified (Phase 2, commit `fda722c6`).** New per-statement
       co-occurrence producer (COUNT-weighted saturating weight, mutual top-K fan-out cap) threaded via
       `_gather_structural_signals` → `build_signal_graph`. **Live: 304 "Frequently queried together"
       whys on 100 pages** (was 0) — which also proves `table_lineage.statement_id` is populated (co_query
       groups by it; empty ⇒ would degrade to `[]`).
     - **`semantic_sim` — ✅ BUILT (Phase 3, optional, commit `9458ec34`) · DEFAULT OFF.** Asset
       name+comment similarity via in-process `keyword_score` (no embedding endpoint), threshold-gated +
       mutual top-K bounded, behind `_SEMANTIC_SIM_ENABLED`. Softest signal (prior 0.4). **Deploy-verify
       decision: leave OFF** — Related is NOT thin (sibling 630 / lineage 285 / join_key 161 / dashboard
       115 / agent 285 pages), so the Phase-3 gate says the flip is unnecessary; 0 semantic whys confirms
       the byte-identical default held.
     - **MV-D105 deploy-verify (`fevm-serverless`/6t92c3, alignment-ON, 2026-09-21):** backend-only
       deploy (wheel carrying `f84f3def`+`fda722c6`+`9458ec34`) → materialize run `56389415521594`
       TERMINATED SUCCESS re-baked `genie_ont_pages` (644 pages / 27 domains, all with Related). Phases
       1–2 whys landed as above (0→723 lineage, 0→304 co_query); Phase 3 stays OFF. Offline floor **3159**
       (parity-mirrored); `git status -- uv.lock` clean; no new dep/router/job parameter. Driver:
       `docs/design/ontology-signal-edge-coverage-driver.md`.
     - **`mv_membership` page-why limb — ✅ CLOSED as "correct-empty on this estate" (Step-0 diagnostic,
       2026-09-21, live snapshot run `56389415521594`).** Root cause is graph-shape vs traversal-depth,
       not a namespace typo: `build_signal_graph` models membership hub-and-spoke (`mv:<fqn>` hub →
       `asset:<source>`, graph.py:193-202), but a measure Page's Sources already include its own
       `mv_fqn` (`mv_sources = {mv_fqn} ∪ source tables`, pages.py:726), so the only node a **1-hop**
       `related_assets` walk reaches over `mv_membership` is the Page's OWN MV hub — correctly dropped as
       self (graph.py:496). The one surfaceable case (a source table feeding a DIFFERENT MV) needs 2 hops.
       Diagnostic verdict: only **3 source tables feed >1 MV** across the whole estate (3 MV pairs,
       ≤6 directed links) — `passenger_accessibility`↔`passenger_profile` (`profile`),
       `fact_booking_daily_metrics`↔`revenue_analytics_metrics` (`fact_booking_daily`),
       `dim_property_metrics`↔`property_analytics_metrics` (`dim_property`) — and the 3 connector tables
       are **not** page anchors (mv nodes match page Sources 32/32; mv source tables 0/29). So a 2-hop
       co-membership expansion would add ~3 relationships for a hot-path traversal change + fan-out
       guards + tests + deploy-verify. **Not worth it here.** DEFERRED option (not a defect): a bounded,
       deterministic 2-hop hub expansion in `related_assets` (or asset↔asset co-member projection at
       build time) — revisit only on an estate with real conformed-dimension sharing across MVs.

### P4 — External enrichment
7. **Phase 4 / 17h — external Context Pack + §9 industry alignment** · ✅ COMPLETE — Stages A/B/C + §9 alignment all BUILT + deploy-verified (B `2026-09-15`, C shipped live in the 4.1j deploy, §9 `de65f480` `2026-09-15`); §10 harness tracked under P1. Off-by-default at runtime (MV-D44).
   - Build spec `ontology-phase4-external-build.md` (§1→§12), staged **A→B→C** with a human
     STOP between each, all DEFAULT OFF (MV-D44), estate-only byte-identical when off.
   - **Stage A (safe backbone) — ✅ BUILT + deploy-verified (via the Stage B live run, which
     single-sources this backbone into the wheel and exercises it):** `context_sources.py` registry +
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
     - **Deploy-verify (`fevm-serverless`, an airline estate, tier ON):** run `826181768666957` /
       ontology run `a5225591…` SUCCESS in 30.3 min. Pack written: `industry_code=481111`
       (Scheduled Passenger Air Transportation), industry label T2-sourced; **26 sourced leaves /
       3 URLs** in the egress log; `canonical_domains[0].is_template=true` (firewall held); estate
       byte-stable (2642 tags · 134 domains · 378 pages · 1995 identities). OFF ⇒ empty pack, no egress.
     - **Live fix (`69bf9ec6`):** the managed `system.ai.web_search` MCP returns a synthesized
       **markdown answer + citations**, not a JSON hit array — `web_search._extract_hits` now parses
       inline `[title](url)` citations (uncited ⇒ [] ⇒ degrade), which is what turned the positive
       path on. Committed with 2 unit tests against the captured live payload.
   - **Stage C (user surface) — ✅ BUILT + shipped live (commit `c718d785`; shipped to 6t92c3 in
     the 4.1j full deploy, frontend build ON):** render-only over Stage B's persisted pack — per-source
     Context Sources panel (`PermissionBanner.SourcePanel` + `permissionTiers.ts`) + one opt-in
     Settings toggle & per-source checkboxes (`SettingsForm` `external_context`) + a labeled/dated
     Sources chip (`mirror._domain_sources`/`_assemble_domain_draft` → `DomainDraftCard.SourcesChips`,
     `models.py`/`types.ts`) + `GRANT EXECUTE`/OAuth wiring (`grant_permissions.context_source_grant_statements`
     + `CONTEXT_MCP_OAUTH_SCOPES`). Additive, read-only, DEFAULT OFF ⇒ byte-identical; backend
     `test_ontology_stage_c.py` 7/7 + frontend Settings/Permission/draftCards tests green.
     **Chip is empty-by-design on this estate (PROVEN live, 2026-09-20):** the panel + toggle render
     live, but the **Sources chip has nothing to render** because the chip requires a surfaced
     pure-engine `create` cluster whose name the pack/alignment actually ADOPTS (`naming_prior.applied=true`),
     and every one of the 22 surfaced domains is a curated `reuse`/`reassign` tag (T0 wins). The
     alignment-ON verify run (`922302079051503`) confirmed this: `name_applied=0`. So this is **not** a
     code gap and it is **not** unblocked by alignment — the chip would only light on an estate with
     adopted engine-`create` names. (`test_unapplied_prior_surfaces_no_source` encodes this contract.)
   - **§9 industry alignment (MV-D58) — ✅ BUILT + deploy-verified (`de65f480`, 2026-09-15):**
     `alignment.py` (663 lines) + `reference_models.py` (bundled T2 models from the
     `lakehouse-industry-data-models` repo) + `similarity.py` run the 4 ordered passes (string →
     embedding seed-anchor → structural propagation → semantic-sanity) and emit typed correspondences
     (`exact`/`narrower`/`broader`/`derived`/`not-equivalent`) + gap hypotheses via `rank.apply_alignment`,
     provenance-gated (T2/T3 never outrank T0/curated). Wired end-to-end: `ont_settings.IndustryAlignment`
     → `refresh.py` `run_now` params → job widgets + `alignment.load_reference_model` (run_ontology_
     materialize.py:1075). Additive, off-by-default (MV-D44) ⇒ byte-identical; `test_ontology_alignment.py`
     28/28 green. **Live verify (`reference_model=airline`):** 16/16 surfaced domains carry a typed
     correspondence (`narrower`×12 / `broader`×4) with `reference_name` + a T2 Provenanced leaf;
     `applied_renames=0` (no curated fact outranked). On this curated-heavy estate alignment lands as
     **corroborating evidence**, not renames — so it raises confidence + supplies the labeled/dated
     source (but does NOT flip `naming_prior.applied`, so it does not by itself light the Stage C chip).
     - **Runtime re-verify (alignment ON) — ✅ DONE (6t92c3, 2026-09-20, run `922302079051503`
       TERMINATED SUCCESS, ~17 min):** flipped `industry_alignment_enabled=true` +
       `reference_model=airline` on the app's own `run_now` recipe. **§10 harness populated** — was
       `None`/`None`/`None`, now **precision 1.0 / recall 0.913 / f1 0.955** (`genie_ont_eval` run
       `27c9383…`; structural: singleton 0.045, orphan 0.182, depth 2). **Typed correspondences live** —
       **21/22** surfaced domains carry `evidence.rank.alignment` (`exact`×6 / `narrower`×10 /
       `broader`×5), **all `applied=false`** (corroboration, curated T0 wins). **`name_applied=0`** —
       Stage C chip empty-by-design confirmed. No structural drift; estate byte-safe.
   - **Next action:** **P4 is done + the alignment loop is closed.** Runtime note: the app default stays
     alignment-OFF (MV-D44), so day-to-day `genie_ont_eval` P/R/F return to `None` unless a run opts in;
     turn `industry_alignment` on in Settings when a scored run is wanted.

### P5 — Track hardening · ✅ BUILT + deploy-verified
8. **17j — Ontology hardening + undo + E2E (MV-D100)** · ✅ BUILT + deploy-verified (6t92c3, 2026-09-19)
   - **Built (additive, `ontology`):** an applied governed-tag membership is now **reversible**
     under OBO from the `genie_ont_applied.prev_value` trail, and undo **never drops the governed
     tag** (MV-D100). BUILD A unset pre-value capture (`_reassign_items` probes
     `grants.current_tag_value`); BUILD B read-only `mirror.read_applied_memberships`
     (metastore-scoped, MV-D49); BUILD C `build_undo_plan`/`execute_undo_plan` in
     `services/apply.py` (single writer — inverse `set`/`unset tag` ONLY, never drop/alter;
     `create_tag` excluded + counted into `ApplyPlan.notes`; OBO write + SP audit/consent re-flip
     applied→approved; no-op guard; per-statement fail-soft); BUILD D `POST /apply/undo-preview`
     (writes nothing) + `POST /apply/undo` (confirm 400 + plan_hash 409) in the existing
     `routers/apply.py` (no new file); BUILD E `ApplyPlan.notes` + `ApplyUndoRequest` + an Undo
     affordance that opens ONLY from a persisted applied result (terminal-state rule) → inverse
     diff via the same `ApplyDiff` → confirm → undo, wrapped in a new `OntologyErrorBoundary`.
   - **Reviewed:** `/review` (Isaac frontend pipeline) run twice → **Approve** (0 P0/P1); a11y
     live-regions + focus-to-heading + undo error retry + error boundary added.
   - **Offline-green:** `./scripts/test.sh` 3103; `frontend` tsc/lint clean + vitest 658;
     lockfiles clean; single-writer carve + POST-allowlist + OBO/SP identity-split firewall tests
     extended to cover `execute_undo_plan` + the consent re-flip; offline E2E
     `test_ontology_apply_e2e` (approve→preview→execute→undo round-trip).
   - **Deploy-verified (`fevm-serverless` / 6t92c3, 2026-09-19):** live UI apply→undo round-trip
     on a `create` sub-domain. Apply wrote `create_tag` + 5× `set_tag` (`state=applied`, OBO
     email); undo wrote 5× `unset_tag` (`prev_value` captured), clearing every member; **the
     governed tag remained** (confirmed via `SHOW GOVERNED TAGS`; no `drop`/`alter` row in the
     audit — the MV-D100 invariant held); consent re-flipped `applied→approved`. 11-row audit
     trail complete, OBO-write / SP-bookkeeping split held. (Re-apply idempotency not exercised —
     optional; the core gate is proven.)
   - **Follow-up (surfacing quality, 17j-adjacent, MV-D101) — ✅ BUILT + shipped live (6t92c3, 4.1j deploy):** an
     already-governed `reuse` proposal no longer surfaces as **actionable** when its effect is
     already fully realized. Observed live (6t92c3, run `a6af31be…`): a governed-tag `reuse` card
     surfaced HIGH though **all of its proposed members already carried** the reused tag —
     applying it was a **pure no-op**. **Built:** the producer stamps
     `evidence["reuse_coverage"] = {already_tagged, total}` in `cluster._make_proposal` (from the
     governed-tag membership it already holds); shared pure predicate
     `transforms.reuse_fully_governed(tag_decision, evidence)` (True iff reuse + total>0 +
     already≥total); `mirror.read_domain_drafts` drops a fully-covered reuse from the **Drafts
     actionable list ONLY** — `evidence["surfaced"]` is untouched, so the domain still renders on
     the estate map and **curated-by-fiat (MV-D53 #1) is preserved**. Partial reuse still surfaces
     (scoped to the untagged remainder); create/reassign byte-identical. Harness **flat** (an
     additive evidence key — score/tier/`surfaced` unchanged). Tests: `test_ontology_cluster`
     (coverage stamp) + `test_ontology_apply` (predicate + Drafts-gate). Offline `./scripts/test.sh`
     **3106** (+3).      **Not a 17j regression** — pre-existing in the surfacing path.
     **Shipped live** on 6t92c3 in the 4.1j full deploy (`243c6d0d`, an ancestor of the deployed
     HEAD). Only optional bit left: an in-UI eyeball confirming a fully-governed reuse no-op card no
     longer appears in the Drafts actionable list.

### P6 — Signal authority (OntoRank-style) · ✅ BUILT + deploy-verified (Stages 1–4)
9. **Ontology Signal Authority (MV-D93–D97)** · ✅ BUILT + deploy-verified on tbzqg7
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

10. **Semantic Blueprint v4 — Join-Advisor candidate source** · ✅ BUILT (code-complete; deployed-review open)
   - The former "deploy-gated remainder" (candidate-source backend + `onSeed` wiring) is **BUILT**
     end-to-end in `2051f539` (2026-08-27, on-branch), NOT flag-gated: `SemanticBlueprint` renders
     directly in the Model tab whenever the graph is non-empty.
     - **Candidate source (server-side FK + name-type discovery + containment probe):**
       `GET /api/auto-optimize/spaces/{id}/join-candidates` (`backend/routers/auto_optimize.py:3480`)
       discovers both FK and name-type candidates and scores each with a warehouse containment probe.
     - **`onSeed` → real run, end-to-end:** `GET/POST /join-advice` persists the seed set to Lakebase
       (`lakebase.save_join_advice`/`get_join_advice`) → `integration/trigger.py` carries
       `proposed_join_seeds` into the run as the `operator_proposed_joins` artifact (`wh_write_join_advice`)
       → `optimization/unified_loop._load_operator_proposed_joins` reads it (`wh_read_join_advice`) and
       the loop re-validates + applies via `add_join_spec`.
     - **Tests:** `backend/tests/test_join_advisor.py` (discovery/persistence/endpoints) +
       `packages/.../tests/unit/test_wh_join_advice.py` (artifact round-trip). **Docs:**
       `docs/docs/features/join-advisor.md` + `docs/docs/reference/api.md`. Specs
       `semantic-graph-v4-build-prompt.md` / `…-blueprint-note.md`.
   - **Next action:** no build remaining — the only open item is the deployed-review round (#11),
     which now also covers the Blueprint + Join Advisor.
11. **MV-Advisor main track — HEAD deployed-review round** · ⏳ VERIFICATION-ONLY (code-complete, deploy-review open)
    - `reference/mv-advisor-gap-report.md`: implementation is code-complete through create-and-attach /
      Genie-v2 round-trip **and** the current Semantic Blueprint + Join Advisor, but the gap report is
      explicit (§ "Current reconciliation", and the Semantic Blueprint note) that the later revisions have
      **code + local-test evidence only — no recorded deployment confirmation of current HEAD**.
    - **This is the single remaining open item for Track A** (and it collapses #10's leftover into it):
      not a build — a deployed human-review / live E2E round on current HEAD, recorded back into the gap
      report. Everything upstream is landed and unit-tested.

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
relationships-on-focus), and reads are **Lakebase-mirrored + TTL-cached**. Of the two enterprise
levers, the render one is now built; the batch one remains unscheduled:

1. **Batch runtime + LLM cost → catalog sharding.** ✏️ UNSCHEDULED. The batch materialize is the
   time/cost cliff (~21 min, 641 Pages on the airline estate). For 10×+ estates, materialize
   **per-catalog and union** (and/or raise the job timeout deliberately).
2. **Render → virtualization (MV-D106).** ✅ **BUILT + deploy-verified (Phase 1, 6t92c3, 2026-09-21).**
   Viewport culling: a pure `cullToViewport` (`ontologyTreeLayout.ts`, `CULL_THRESHOLD=600`/
   `CULL_PAD=1.0`) mounts only nodes/edges intersecting the padded viewport once a laid-out estate
   exceeds the threshold; below it (or before a viewport exists) it is a referential passthrough ⇒
   byte-identical DOM. A `keepSet` (selected ∪ ancestors ∪ searchHits ∪ hovered ∪ dragged) keeps
   interactions correct; the minimap stays full-scene. Frontend-only, `vitest` 672 (+12); live eyeball
   confirmed mounted `[data-node-id]` drops on deep zoom. **Phase 2 (WebGL/LOD) DEFERRED** — only if
   culling stutters near the 2000 cap. Driver `ontology-map-virtualization-driver.md`.

---

## Suggested execution order

Batch engine + Ontology Map are **done**, the **P0 map-interaction/#4 pass is
deploy-verified + committed** (`67ad4cff`), **Phase 4 Stage B is deploy-verified** (`69bf9ec6`),
**P1 (the §10 harness, MV-D59) is LIVE + deploy-verified** (`8c6af04e`), **P6 (Signal
Authority, MV-D93–D97) is fully deploy-verified** (Stages 1–4 + 4b), and **P2 (Phase 5 apply,
17i) is BUILT + deploy-verified** (`cf92ef58`; Stage 2 harden live on 6t92c3) — the value-unlock
that closes the curator loop. **P5 / 17j** (apply hardening + E2E + undo/rollback, MV-D100) is now
**BUILT + deploy-verified** (6t92c3, 2026-09-19) — the live apply→undo round-trip reversed a
membership from the `genie_ont_applied` trail and left the governed tag in place; the reuse-no-op
Drafts gate (MV-D101) is BUILT + shipped live (6t92c3, in the 4.1j deploy). **P3** (curator Draft-with-AI
Steps 3–4) is **BUILT + hardened + deploy-verified** (6t92c3, 2026-09-19 — 7 `/review`+live findings
fixed, incl. the evidence-write JSON/column bug that meant the draft never persisted and the
on-demand identifier-universe fix). **Stage 4.1j** (Related-assets & links via graph traversal,
MV-D102/D103) is now **✅ BUILT + deploy-verified** (6t92c3, 2026-09-20) — deterministic graph-derived
Related assets (the highest-value Page field we left empty) went `related_fqns` **0/642 → 644/644**
on a live re-bake (run `611356940571419`), 161 join-key + 159 dashboard + 331 agent + 641 sibling
whys, harness P/R/F flat; best-effort external Links stay default-off (`links=0`). All
additive/default-safe (`signal_graph=None` ⇒ agents-only byte-identical; suites 3133 + 660 vitest).
**P4** external enrichment is **COMPLETE** and the alignment loop is **CLOSED**: Stages A/B/C + **§9
industry alignment (MV-D58, `de65f480`)** + the §10 harness (P1) are all built and deploy-verified. The
opt-in **alignment-ON re-verify** (6t92c3, 2026-09-20, run `922302079051503`) populated the §10 harness
(**precision 1.0 / recall 0.913 / f1 0.955**, was `None`) and landed **21/22** live typed correspondences
(`exact`×6 / `narrower`×10 / `broader`×5, all corroborating). The Stage C Sources chip is empty
**by design** on this fully-curated estate (`name_applied=0`, proven live) — not a gap. All off-by-default
at runtime (MV-D44), so day-to-day harness P/R/F return to `None` unless a run opts in. **Next:** the
ontology arc (P1–P6 + Phase 4/5) is landed. Track A (MV-Advisor) is also **code-complete** — the
Semantic Blueprint v4 + Join-Advisor candidate source + `onSeed`→run path are all BUILT (`2051f539`,
on-branch, unit-tested, not flag-gated); the ONLY open item across both tracks is a **deployed-review /
live E2E round of the current MV-Advisor HEAD** (verification, not a build), recorded into the gap report.
**Track A** can run in parallel by anyone off the ontology branch.
