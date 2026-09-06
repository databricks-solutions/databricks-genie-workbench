# Ontology — Frontend batch Goal-Mode driver (Draft-with-AI UI · cytoscape estate graph · UX papercuts)

## ⚙️ Parallel-build lane header — Lane 3 of Wave 2 (READ FIRST)

You run in an **isolated git worktree** off the `ontology` HEAD (`isolation: worktree`,
`worktree.baseRef: "head"`). Two sibling lanes edit the repo concurrently. This lane owns the
whole disjoint subtree `frontend/src/ontology/**` (plus the frontend lockfile for one authorized
dependency), so merges are conflict-free — keep it that way. **This is the frontend half of
Stage-4.1d Steps 3 & 4 (Lane 2 is the backend half) plus the Phase-3e estate-graph tab and the
UX papercuts.** Build the draft UI against the frozen contract below — those endpoints live in
Lane 2's worktree and will NOT exist in yours; define the matching `api.ts` functions + `types.ts`
interfaces to the frozen shape so they integrate at merge.

- **OWNS (create/edit freely):** everything under `frontend/src/ontology/**` —
  `api.ts`, `types.ts`, `OntologyPage.tsx`, `components/PageDraftCard.tsx`,
  `components/DomainDraftCard.tsx`, `components/DraftsView.tsx`, and new files
  `components/EstateGraph.tsx` + a graph/estate test. Existing tests
  (`draftCards.test.tsx`, `PermissionBanner.test.tsx`) — extend, keep green.
- **AUTHORIZED DEPENDENCY (this lane only):** add **`cytoscape`**, **`react-cytoscapejs`**, and
  the layout extension **`cytoscape-fcose`** to `frontend/package.json` + `frontend/package-lock.json`.
  Install **exact-pinned**: `cd frontend && npm install cytoscape@<v> react-cytoscapejs@<v>
  cytoscape-fcose@<v> --save-exact` (pick the current stable exact versions; NO `^`/`~`). `npm ci`
  must still succeed **without** `--legacy-peer-deps` (fix a peer conflict by choosing an in-range
  pin, never the flag — Dependency Security Policy in `AGENTS.md`). This is the ONLY new dependency
  authorized; add nothing else.
- **OFF-LIMITS (do NOT touch):** all of `backend/`, all of `packages/…/genie_space_optimizer/`
  (the wheel), the root `package.json`/`uv.lock`, and `docs/design/mv-advisor-playbook.md`.
- **MERGE-ORDER:** **last** (after Lane 1 and Lane 2), so the endpoints this UI targets are on
  `ontology` at integration time.
- **Launch:** via the `ontology-lane-builder` subagent — see `ontology-wave2-launcher.md`.

### 🔒 Frozen API contract (mirror Lane 2 exactly — do not drift)

- `POST /api/ontology/pages/{page_id}/draft-body`
  → `DraftBodyResponse { ok: boolean; page_id: string; body: string; body_source: string; as_of: string }`
- `POST /api/ontology/subdomains/{domain_id}/draft-bodies`
  → `BulkDraftStart { task_id: string; total: number }`
- `GET  /api/ontology/subdomains/{domain_id}/draft-bodies/status?task_id=…`
  → `BulkDraftStatus { done: number; total: number; running: boolean;
       results: { page_id: string; ok: boolean; reason: string | null }[] }`
- **Already live on `ontology` (wire directly, do NOT re-spec):**
  `GET /api/ontology/graph` → `OntologyGraph` (the `OntologyGraph{,Node,Edge,Level}` types
  ALREADY exist in `frontend/src/ontology/types.ts` — reuse them; add only the `getGraph()` fetch).

---

## Spec & decisions

- **Specs:** `docs/design/ontology-curation-redesign-stage4.1d-build.md` §3.3/§3.4/§5 (draft UI);
  `docs/design/ontology-phase3e-build.md` (estate graph — Step A landed the `/graph` route +
  `OntologyGraph` model; this lane is **Step B**, the render). **Mockup (visual north star):**
  `docs/design/mockups/17.0j-ontology-estate-graph-cytoscape-dark.html` (chosen bakeoff library).
- **Decisions:** `mv-advisor-playbook.md` — **MV-D66** (curator drafting), **MV-D48** (estate graph),
  **MV-D36** (Ontology is a standalone admin-gated page, not a SpaceDetail tab). Honor **MV-D43**
  (degrade-not-hang → honest-empty, never crash), **MV-D45** (only the authorized deps above).
- **Project rules & patterns:** `AGENTS.md`; the existing `frontend/src/ontology/api.ts`
  (`fetchJson` + `postDecision` POST pattern) and `OntologyPage.tsx` tab wiring (`OntologyTab`
  union + `TABS` array + per-tab render block, gated by `canRender` / `!emptyScope`).

---

## Driver prompt (paste verbatim into the subagent)

```text
GOAL: The ontology FRONTEND batch — (1) Stage-4.1d Step-3 single-Page "Draft with AI", (2) Step-4
bulk "Draft this sub-domain with AI", (3) Phase-3e Step-B cytoscape estate-graph tab, (4) UX
papercuts. All under frontend/src/ontology/** only. Additive; typecheck + lint + tests green;
STOP before deploy. Branch: ontology. NO backend, NO wheel, NO playbook edits. The draft endpoints
live in a SIBLING worktree — build to the FROZEN CONTRACT (define matching api.ts fns + types.ts
interfaces); they integrate at merge. The /graph endpoint + OntologyGraph types are ALREADY on
ontology — wire directly.

SPECS: ontology-curation-redesign-stage4.1d-build.md §3.3/§3.4/§5; ontology-phase3e-build.md
(Step B render). MOCKUP: docs/design/mockups/17.0j-ontology-estate-graph-cytoscape-dark.html.
DECISIONS: mv-advisor-playbook.md MV-D66/D48/D36; honor MV-D43/D45. RULES: AGENTS.md. Read first.

DEP (authorized, this lane only): cd frontend && npm install cytoscape@<v> react-cytoscapejs@<v>
cytoscape-fcose@<v> --save-exact  (exact pins; npm ci must pass WITHOUT --legacy-peer-deps). No
other new dependency.

BUILD 1 — Step-3 single draft (api.ts + types.ts + PageDraftCard.tsx): add
draftPageBody(pageId): Promise<DraftBodyResponse> (POST /pages/{id}/draft-body, mirror fetchJson);
mirror DraftBodyResponse + body_source in types.ts; add a "Draft with AI" button to PageDraftCard
that calls it, shows a spinner, and swaps the body in place on success (error ⇒ toast; card
otherwise unchanged). Keep the deterministic stub visible until success.

BUILD 2 — Step-4 bulk draft (api.ts + types.ts + DomainDraftCard.tsx / sub-domain header): add
startBulkDraft(domainId): Promise<BulkDraftStart> and pollBulkDraft(domainId, taskId):
Promise<BulkDraftStatus>; mirror both models; add a "Draft this sub-domain with AI" action on the
sub-domain header with a confirm (it costs LLM calls), a progress indicator (done/total), and
per-Page body refresh as results land (reuse the Step-3 body-swap). Poll on an interval until
running=false; stop the timer on unmount.

BUILD 3 — Phase-3e Step-B estate-graph tab (OntologyPage.tsx + api.ts + new EstateGraph.tsx): add
"graph" to the OntologyTab union + a TABS entry (a graph icon); add getGraph(): Promise<OntologyGraph>
(GET /graph — reuse the EXISTING OntologyGraph types); render a new components/EstateGraph.tsx that
draws the OntologyGraph (domains + assets levels + edges) with react-cytoscapejs using the fcose
layout, matching the 17.0j dark-theme mockup (domain vs asset node styling, edge styling, a small
legend, zoom/pan). Gate the tab behind canRender && !emptyScope like the other tabs; on an empty
graph render an honest-empty state (MV-D43), never a crash. Lazy-mount the canvas so the tab is
cheap until opened.

BUILD 4 — UX papercuts: (a) after a successful refresh via FreshnessControls, auto-re-fetch the
Drafts + Taxonomy bodies (so the user sees fresh results without switching tabs); (b) render the
Page body on PageDraftCard (so drafted/stub bodies are visible on the card). Additive, behavior of
other tabs unchanged.

HARD GUARDRAILS: additive — frontend/src/ontology/** ONLY, plus the ONE authorized frontend
dependency (cytoscape + react-cytoscapejs + cytoscape-fcose, exact-pinned; npm ci clean without
--legacy-peer-deps). NO backend/wheel/root-package/uv.lock/playbook edits. Draft UI targets the
FROZEN CONTRACT (no endpoint invention/rename). Degrade-not-hang: any fetch failure ⇒ honest-empty
or a toast, never a crash or a hang; keep the deterministic stub until a draft succeeds. Do NOT
deploy or run npm run dev.

ACCEPTANCE (offline): cd frontend && npm ci && npm run lint (clean) && npx tsc -b (clean) && the
frontend test runner green. Extend draftCards.test.tsx: "Draft with AI" calls draftPageBody and
swaps the body on ok=true, shows a toast + keeps the stub on ok=false; the sub-domain bulk action
confirms, starts, polls to done, and refreshes per-Page bodies. New EstateGraph/graph-tab test:
renders honest-empty on an empty OntologyGraph and renders nodes/edges on a fixture graph; the
"graph" tab is gated by canRender/!emptyScope. `./scripts/test.sh` still green (backend/wheel
untouched). uv.lock + root package.json untouched; frontend/package(-lock).json carry ONLY the
authorized deps.

WORKFLOW: branch ontology. Do NOT deploy. When lint + tsc + tests are green, STOP and report the
worktree branch, `git diff --stat`, and the gate summary; a human runs the deploy-verify.
```

---

## After the run (human-gated — merged with Lane 2)

Full `./scripts/deploy.sh --update` (frontend changed), open the Ontology page, and verify:
the new **Graph** tab renders the estate (domains + assets) via cytoscape; **Draft with AI** on a
Page and **Draft this sub-domain with AI** on a sub-domain header work end-to-end against the
Lane-2 endpoints; after a Refresh, Drafts/Taxonomy auto-reload; Page bodies show on the cards.
Record the pass in the playbook (human edit — the agent never touches it).
