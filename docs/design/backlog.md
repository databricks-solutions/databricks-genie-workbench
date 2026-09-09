# Ontology + MV-Advisor — single ordered backlog

One drivable list across **both** tracks in `docs/design/`. Reconciled against code
on branch `ontology` (2026-09-05, post-4.1f). This is the sequencing source of truth; the
per-phase build specs / drivers remain the *content* source of truth, and
`mv-advisor-playbook.md` remains the MV-D register.

## Status legend

| Tag | Meaning |
|---|---|
| ✅ BUILT | Code landed on `ontology`; deploy-verified |
| 🟡 BUILT-OFFLINE | Code landed + offline-green; **deploy-verify pending** |
| 📝 DRAFTED | Build spec **and** driver exist, unbuilt (Goal-Mode-ready) |
| ✏️ UNDRAFTED | Needs a build spec + driver before any build |
| 🧊 HISTORICAL | Superseded / reference only |

---

## The ordered backlog (drive top-to-bottom)

### P0 — Batch engine complete; small UX papercuts to finish the loop
1. **Stage 4 + 4.1a–4.1f — the batch proposal engine** · ✅ BUILT + deploy-verified
   - The full Stage-4 Pages path plus 4.1a (warehouse wiring), 4.1b (coded columns +
     Page-attachment gate, MV-D63/64), 4.1c (single wheel-native LLM client, MV-D65),
     4.1d (bounded page drafting + deterministic certify, MV-D66), 4.1e (gate-bounded
     naming, MV-D67), and **4.1f (bounded ER + naming caps, MV-D68, commit `90fdff6c`)**
     all landed. **The batch-timeout saga is closed**: run `31641283089969` succeeded in
     **20.8 min**; snapshot has 17 surfaced Domains, 641 Pages, `certify` working.
   - **No further action on the engine** — further tuning (super-sure auto-draft yield,
     attachment %) is diminishing-returns backlog, not critical path.
2. **UX papercuts — finish the review loop** · ✏️ UNDRAFTED (tiny)
   - (a) **Drafts/Taxonomy do not auto-reload** after a refresh completes — the user must
     revisit the page to see new drafts (`FreshnessControls` polls status but never
     re-fetches taxonomy/tags/drafts). This is the "couldn't see domains" report.
   - (b) **Page `body` is not rendered** on `PageDraftCard` — it rides the copy payload
     only, so the curator can't read a proposed Page in-app.
   - **Next action:** one tiny `ontology-ux-papercuts-{build,driver}.md` (or fold into the
     Phase-5 build) — reload-on-refresh-complete + render body. Frontend-only, no dep.

### P1 — Quality scoreboard (gates all later tuning)
2. **§10 Evaluation & trust harness (MV-D59)** · ✏️ UNDRAFTED
   - Exists only as a stub: `ontology-curation-redesign-build.md` §10 + MV-D59.
   - Scope: offline harness — gold-standard P/R/F vs aligned reference, structural
     health (singleton/orphan/depth/branching), cheap reference-free LLM sanity
     monitor, human spot-review queue. No new dependency (MV-D45).
   - **Next action:** draft `ontology-eval-harness-{build,driver}.md`, then build.
   - **Why here:** MV-D59 gates every subsequent signal/threshold change, so it
     should precede further Stage/threshold tuning and Phase-4 alignment.

### P2 — Drafted + independent (parallelizable read-only win)
3. **Phase 3e / 17k — Estate Graph ("Ontology Map")** · 📝 DRAFTED
   - Specs: `ontology-phase3e-{build,driver}.md` (MV-D48). Not built (no
     `ontology/layout.py`, no `genie_ont_graph_snapshot`).
   - **Blocker:** frontend-library **bakeoff** STOP gate (Sigma.js v3 vs Reagraph
     vs Cytoscape.js) — a human eyeballs 3 static mockups and records the winner
     **before** any npm dep / component lands.
   - **Next action:** run/record the bakeoff decision → build the offline slice.
   - Independent of 17h/17i; runnable any time after P0.

### P3 — External enrichment (needs drafting)
4. **Phase 4 / 17h — external Context Pack + §9 industry alignment** · ✏️ UNDRAFTED
   - Architecture only: MV-D38 (Context Pack, provenance firewall, zero user burden)
     + MV-D58/§9 (Vibe industry-model alignment, typed correspondences). **No build
     or driver doc exists.**
   - **Next action:** draft `ontology-phase4-external-{build,driver}.md` (fetch the
     matching Databricks Vibe airline/travel reference model; pin the match contract:
     string+embedding seed → structural propagation → semantic sanity; typed
     `exact/narrower/broader/derived/not-equivalent`; T2/T3 provenance-gated, off by
     default per MV-D44). Then build.

### P4 — The one write path (drafted) — **NOW THE #1 BUILD (post-4.1f pivot)**
5. **Phase 5 / 17i — consented `SET TAG` apply (L9)** · 📝 DRAFTED — **QUEUED NEXT**
   - Specs: `ontology-phase5-apply-{build,driver}.md` (MV-D37/D26/D50/D23/D27). Not
     built (no `ontology/apply.py`; only firewall tests asserting *no* writes exist).
   - Consumes the 17g `genie_ont_consents` ledger (✅ built). Dry-run-first,
     default-OFF, OBO-attributed, request-time only (never in the batch job).
   - **Why now:** the engine lands a trustworthy set (4.1f), so the value-unlock is
     *acting on it* — apply is the gap between "discovery aid" and "ontology builder."
   - **Driver is paste-ready** (verified). Prereqs 17g consents / MV-D49 re-grain /
     MV-D50 OBO all shipped; `certify` now works (4.1d), satisfying its trust caveat.
   - **Next action:** run `ontology-phase5-apply-driver.md` in Goal Mode → STOP at the
     apply-safety checkpoint → human deploy-verify.

### P5 — Track hardening (needs drafting)
6. **17j — Ontology hardening + E2E** · ✏️ UNDRAFTED
   - Register entry only (`mv-advisor-playbook.md`): the track's own hardening + E2E,
     undo/rollback of an applied membership, docs / changelog / PR. **No doc.**
   - **Next action:** draft `ontology-17j-hardening-{build,driver}.md`; run after 17i
     lands (rollback needs the `genie_ont_applied` audit rows).

### P6 — Signal authority (OntoRank-style enrichment) — **build LAST, gated on P1**
7. **Ontology Signal Authority — popularity + certification into the ranker (MV-D93–D97)** · 📝 DRAFTED (build spec exists; driver pending)
   - Build spec: `ontology-signal-authority-build.md` (MV-D93 umbrella + MV-D94 popularity /
     MV-D95 certification / MV-D96 PageRank+seeding / MV-D97 visual). Driver `…-driver.md` to write.
   - Scope: the L6 ranker already reserves a `usage(0.40) × centrality(0.35) × governance(0.25)`
     blend but sources neither the usage factor (`usage_signals()` returns `{}`) nor the
     `curated` certified-authority rung (`_governance_map` only emits `governed`). Four stages:
     wire `system.query.history`+`table_lineage` popularity (percentile-normalized, honest-gap);
     feed `system.certification_status` into the authority rung + make `deprecated` a rank
     firewall; upgrade degree→`igraph` PageRank over the full fused graph + certified-seeded
     domain assignment + usage-weighted clustering; encode popularity=size / certified=ring on
     the map. Additive/read-only, reuses GenieWatch's SP system-table plumbing + the `igraph`
     already lazy in `cluster.py` (no new dep — MV-D45/D49/D26).
   - **HARD GATE (MV-D59):** it changes signals + thresholds, so it **must land after P1** (the
     §10 eval harness) to be measured, not eyeballed — Stage 3 (PageRank/seeding) especially.
   - **Why last:** highest ceiling (this is the "make the ontology builder propose *better*
     assets, categorize by earned authority, and visualize trust" work), but it depends on the
     scoreboard and reads best on top of the Typed Estate Assets map (MV-D89–D92).
   - **Next action:** write `ontology-signal-authority-driver.md`, then build Stage 1 first
     (smallest change, unblocks the 0.40 factor) once the harness exists.

---

## Track A — MV-Advisor / Semantic Graph (parallel, independent of ontology)

7. **Semantic Blueprint v4 — Join-Advisor candidate source** · 📝 DRAFTED (feature
   BUILT behind flag)
   - Built: `frontend/src/components/model/SemanticBlueprint.tsx` + `blueprint/`
     modules + Phase-2 backend, behind the `blueprint` canvas toggle. Gates green.
   - **Deploy-gated remainder:** server-side FK / name-type discovery + containment
     probe (the advisor's *candidate source*) and wiring `onSeed` to a real
     Auto-Optimize run. Until then it renders its honest-empty state.
   - Spec: `semantic-graph-v4-build-prompt.md` / `semantic-graph-v4-blueprint-note.md`.
   - **Next action:** build the candidate-source backend + `onSeed` wiring → deploy-
     verify. Closest-to-done, high-visibility, independent of everything ontology.

8. **MV-Advisor main track — HEAD deployment review** · ✅ BUILT (verification gap)
   - `reference/mv-advisor-gap-report.md`: code-complete through create-and-attach / Genie-v2
     round-trip + current Blueprint, but **no deployment review of current HEAD**.
   - **Next action:** a deployed human-review round on current HEAD (not a build).

---

## Reference — built ontology foundation (context, not backlog)

✅ Phase 1 spine · ✅ Phase 2 batch + Lakebase mirror · ✅ Phase 3a signal graph + ER ·
✅ Phase 3b clustering · ✅ Phase 3c page miners · ✅ Phase 3d rank + serve ·
✅ Re-grain to metastore (MV-D49) · ✅ OBO-first foundations (MV-D50) ·
✅ Curation redesign Stages 1 / 2 / 3 / 3.1 / 3.2 (deploy-verified) ·
✅ Stage 4 Pages + 4.1a–4.1f (coded columns, attachment gate, LLM-client consolidation,
bounded drafting/naming/ER; MV-D63–D68; timeout resolved, run in ~21 min, deploy-verified).

🧊 `semantic-graph-v2-note.md`, `semantic-graph-v3-note.md` — superseded by v4.

---

## Suggested execution order

Batch engine is **done** (P0 item 1). **Build P4 (Phase 5 apply) NEXT** — it's the
value-unlock that closes the curator loop and its driver is paste-ready. Fold the P0
UX papercuts in alongside (or just before) it. Then the **4.1d Steps 2→4** curator
enrichment loop, **P2** (Estate Graph, read-only win), **P1** (§10 harness, so quality
stops regressing silently), **P3** (external) and **P5** (hardening) later. **P6**
(Signal Authority / OntoRank enrichment) is the **last ontology build** — it needs the
P1 harness as its scoreboard, so it deliberately follows everything above. **Track A #7**
(Blueprint remainder) can run in parallel by anyone not on the ontology branch work.
