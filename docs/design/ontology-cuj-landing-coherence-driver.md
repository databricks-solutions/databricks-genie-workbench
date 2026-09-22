# Ontology CUJ / landing coherence — Overview-first, one-job-per-screen · Goal-Mode driver

> **Frontend-only, on the `ontology` branch.** PHASED (P1→P4; P1 SHIPPED), each phase its own commit + STOP at
> a deploy-verify gate. Additive over surfaces that already exist and are deploy-verified — this is
> re-composition, not new engine work. NO backend/router/wheel/API change (every datum is already
> fetched by `OntologyPage`), NO new dep. Proposed register line: **MV-D107** (the Ontology page
> tells a coherent story — land on an Overview that names the value + the one next action, promote the
> actionable Review surface, demote the permission plumbing, and rewrite empty states as onboarding).
> Determinism/dual-theme preserved; MV-D80 mockup-fidelity + a11y gates apply.

## Why now
The backend/engine is landed and deploy-verified (P1–P6, Phase 4/5, MV-D105/D106). The remaining
leverage is the **critical user journey**: a user lands on the Ontology tab and does not know what it
is or what to do. Today the page leads with an amber **permission/grant matrix** and defaults to a
passive **Taxonomy** lens showing "0 member(s)"; the one surface of value — the ranked **Drafts**
(suggestions you accept/apply/draft) — is tab #3. Current UX best practice is unambiguous: a first
screen must NAME THE VALUE, SHOW THE SHAPE OF SUCCESS, and OFFER EXACTLY ONE PRIMARY ACTION, with
status→context→detail via progressive disclosure and one job per screen. The page violates all of
these, and the charter's **zero-user-burden** principle (MV-D38: the curator never confronts system
tables) is contradicted by a grant matrix on the front door.

## Grounded facts (code, 2026-09-21)
- **Default tab is a passive lens.** `OntologyPage.tsx:90` — `useState<OntologyTab>("taxonomy")`; the
  tab set is `taxonomy | tags | drafts | graph | settings` (`OntologyPage.tsx:37`, TABS `:143-149`).
- **The landing leads with plumbing, with alarm styling.** `PermissionBanner` renders first
  (`OntologyPage.tsx:186`); its header is an amber `ShieldAlert` block (`PermissionBanner.tsx:127-140`)
  whose own copy says "no service-principal grant is required to view."
- **Real bug — "4 of 3 read tiers ready".** `readyCount` counts ALL `ok` tiers but the denominator
  excludes two (`PermissionBanner.tsx:122-131`), so the numerator can exceed the denominator.
- **The value is buried + its trigger is hidden.** The actionable surface is `DraftsView` (ranked
  Domain/Page suggestions with accept/reject, Draft-with-AI, and "Apply approved changes"
  `DraftsView.tsx:140-148`); its cold empty state TELLS the user to "Run a refresh"
  (`DraftsView.tsx:112-128`) while the refresh button lives in a small right-aligned chip
  (`OntologyPage.tsx:223-231`, `FreshnessControls.tsx`).
- **All Overview data is already fetched.** `loadHead` loads `preflight` + `inventory` + `settings`
  (`OntologyPage.tsx:99-113`); the body loads `taxonomy` + `tags` + `drafts` + `graph`
  (`OntologyPage.tsx:120-141`). Shapes (`types.ts`): `OntologyInventory{metric_view_count,
  genie_agent_count, governed_tag_count, catalogs_scanned, as_of}`; `OntologyPreflight{catalog_allowlist,
  can_render_taxonomy, company_name, tiers}`; `OntologyDrafts{domains, pages, source:"mirror"|"live"|"cold"}`;
  `OntologyTaxonomy{domains}`; `TagLens{tags, collisions, cleanup}`.
- **The scan action to reuse.** `FreshnessControls` refreshes via `triggerRefresh` + `getRefreshStatus`
  + `pollSettleAction` and already exposes `onOpenSettings`/`onRefreshComplete` (`FreshnessControls.tsx`).
- **Estate is unnavigable + provenance-blind.** `TaxonomyView` is a flat `domains.map`
  (`TaxonomyView.tsx:117-119`) with no search / filter / sort / collapse-all and no "hide empty"; it
  renders governed tags "as they exist" (DISCOVERED, MV-D37) but never labels that, and never reconciles
  with proposals — yet `DomainDraft` already carries `tag_decision:"create"|"reuse"|"reassign"` +
  `conflict_tag` (`types.ts:206-213`), the exact join to badge Confirmed vs Better-proposal per row.
- **Access panel is relocated, not reorganized (P1 residue).** P1's `AccessSharingPanel` renders the
  UNCHANGED `PermissionBanner` — an always-amber `ShieldAlert` header (`PermissionBanner.tsx:130-143`,
  amber even when every tier is `ok`), a scorecard header (`:134`), and a flat `tiers.map` (`:151-191`)
  that mixes required-read / optional-SP-upgrade / locked-write / external-source tiers in one
  undifferentiated list. It moved off the front door but was never made legible.

## Testability seam
All new logic is PURE + component-testable (vitest): the adaptive-CTA state machine and the KPI
derivation are pure functions of `{preflight, inventory, drafts}`; the Overview is a prop-driven
component; the tab reorg is a constant array. No backend, no API, no wheel, no new dep. Keep the CTA
resolver and KPI builder in a side-effect-free module (react-refresh/only-export-components rule).

---

## GOAL PROMPT (paste verbatim into Goal Mode — run ONE phase, then STOP)

Improve the **Ontology page CUJ**, `ontology` branch, frontend-only, ONE PHASE per run. Additive,
dual-theme, determinism/a11y-preserving. NO backend/router/wheel/API/dep change (`frontend/package-lock.json`
untouched). Proposed **MV-D107**. Recompose existing deploy-verified surfaces into: land → orient → act.
Each phase: `tsc -b` clean, `npm run lint` clean, `npm run test` green (report count), then STOP for
deploy-verify.

P1 — ✅ SHIPPED (deployed, awaiting live eyeball): Overview-first landing (`OverviewPanel` + pure
`overviewModel.nextAction`/`buildKpis`/checklist), `overview` as DEFAULT tab, `PermissionBanner` demoted
into a collapsed Settings `AccessSharingPanel`, and the "4 of 3" counter fix. Full spec: Grounded facts +
Deploy-verify P1.

P2 — Tab reorg + navigable/provenant Estate + empty states as onboarding.
1. Reorder/rename `TABS` (`OntologyPage.tsx:143-149`) → `Overview · Review · Map · Estate · Settings`
   (Review=`drafts`, Map=`graph`, Estate=`taxonomy`+`tags` merged). Reuse the components; labels only.
2. Make **Estate** navigable + provenant, client-side over loaded `taxonomy`+`drafts` (`TaxonomyView.tsx`):
   search, smart "hide 0-member" toggle (OFF until something is tagged), sort (members desc → sub-domain
   count → name), collapse-all; label it "Discovered in your estate" + a domain-vs-tag count explainer. A pure `reconcile(taxonomy, drafts.domains)` join by tag key badges each row —
   `✓ Confirmed` (`reuse` draft), `△ Better proposal` (`reassign`/`conflict_tag` → deep-link Drafts),
   `○ Declared·unpopulated` (0 members). All-0-member but drafts exist ⇒ lead "nothing tagged yet — N
   proposals ready" → Review.
3. Rewrite every empty/gate state to: outcome HEADING + muted GHOST preview + ONE action (+ escape hatch) —
   `EmptyScopeNotice`/`GrantGateNotice` (`OntologyPage.tsx:48-75`) and cold `DraftsView`
   (`DraftsView.tsx:112-128`), with the "Scan the estate" button IN the cold Review state.

P3 — Polish, a11y, instrument. First-use vs returning variants (extend cold/caught-up to Review/Estate);
a11y (semantic headings, `aria-hidden` on icons, focus order, error boundaries); instrument the funnel
(view/CTA/first-scan/first-review) via the existing telemetry seam or TODO markers — no dep.

P4 — Access & sharing redesign (`AccessSharingPanel`/`PermissionBanner` over `preflight.tiers`, no API).
Neutral by default (drop the always-amber header; only a `blocked`/`degraded` tier warns); purpose-first
header, not "N of M ready". Group tiers by purpose — Reading your estate (OBO, "you're set" when ok);
Optional upgrades (SP grant, benefit-led, `GRANT` SQL behind a Show-SQL disclosure); Not used this release
(locked write tier); External sources (Stage C `SourcePanel`, unchanged). Move the company-name card
(`PermissionBanner.tsx:194-206`) into Ontology settings.

GUARDRAILS (every phase): frontend-only; no backend/router/wheel/API/dep change; pure CTA/KPI/reconcile
logic in a side-effect-free module (react-refresh); dual-theme; no jargon in the primary read (MV-D23);
Apply write path + gates UNCHANGED, only relocated; lockfile untouched.

TESTS (vitest): P1 — `nextAction` (four states); KPI builder from fixture; `OverviewPanel` one primary CTA
per state; counter never exceeds denom ("4 of 3"). P2 — TABS order/labels; `reconcile` badges
(reuse⇒Confirmed, reassign|conflict⇒Better, 0-member⇒unpopulated); hide-empty+search filter the tree;
all-0-member-with-drafts banner; each empty state one action; cold Review scan button. P3 — variant + a11y.
P4 — tiers bucket by purpose (fixture); neutral when all read tiers ok, warning only on blocked|degraded;
`GRANT` SQL hidden until disclosed.

ACCEPTANCE (per phase, offline): `npx tsc -b` · `npm run lint` · `npm run test` all green (report count);
`git status -- frontend/package-lock.json` clean. Then STOP for deploy-verify.

---

## Deploy-verify gate (human, after each phase's offline-green — the STOP checkpoint)
Frontend bundle changes, so **frontend build ON**: `./scripts/deploy.sh --update` (do NOT set
`SKIP_FRONTEND_BUILD=1`). `deploy.sh` reads `GENIE_DEPLOY_PROFILE` from `.env.deploy` (today
`fevm-serverless` → 6t92c3) and IGNORES a `DATABRICKS_CONFIG_PROFILE=` prefix. **No materialize run
needed** — this is UI-only, no data change. On the deployed app, open the Ontology tab and confirm:
1. **P1:** you land on **Overview**, not the grant matrix; the hero + KPIs read in plain language; the
   single primary CTA matches the estate's state (scoped+fresh here ⇒ "Review N suggestions"); the
   permission matrix is gone from the front door (now a collapsed Settings panel) and the counter reads
   coherently (never "X of Y<X").
2. **P2:** tabs read `Overview · Review · Map · Estate · Settings`; the actionable suggestions are one
   click from landing; a cold/empty surface names the value + shows a ghost + offers one action. On
   **Estate**: search/hide-empty/sort/collapse work; the tab is labeled "Discovered in your estate"; rows
   carry provenance badges (`✓ Confirmed` / `△ Better proposal` deep-linking Drafts / `○ Declared ·
   unpopulated`); an all-0-member estate with pending drafts leads with the "nothing tagged yet — N
   proposals ready" banner → Review.
3. **P3:** returning vs first-use states differ; keyboard/screen-reader order is sane; no graph/render
   regressions (MV-D106 culling intact).
4. **P4:** Settings → Access & sharing is NEUTRAL when healthy (no amber) and warns + auto-opens only on a
   blocked tier; tiers are grouped (Reading your estate / Optional upgrades / Not used this release /
   External sources); each group leads with a plain-language purpose and the `GRANT` SQL sits behind a
   Show-SQL disclosure; the company-name card has moved to Ontology settings.
Review with a human (the five-second test: can a new admin state what this page is for and what to do
next?) — then mark the phase BUILT and (after the final phase) register **MV-D107**.

## Non-goals / out of scope
No engine/proposal changes; no new API; no change to the Apply write path or its consent gates (only
its placement in the IA); no new npm dep; no materialize-job change. Role-based routing beyond
admin is a future consideration, not this driver.
