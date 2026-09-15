# Ontology — UX papercuts ("Run 1") Goal-Mode driver

Copy-paste launcher for two small, high-visibility fixes on the **Ontology** page. Run
on the **`ontology`** branch. Frontend-only, additive, **no new npm dep**; ends
offline-green and **STOPs before deploy**.

## The two fixes

1. **Theme staleness (map palette doesn't flip live).** `hooks/useTheme.ts` keeps
   **per-instance** React state (`useState(getStoredTheme())`) with **no shared store and
   no DOM/storage listener**. When `ThemeToggle` flips, it updates `localStorage` + the
   `.dark` class on `<html>` + *its own* copy of the state; every OTHER `useTheme()`
   consumer keeps its mount-time value. The rest of the app restyles via Tailwind `dark:`
   + CSS vars keyed off `<html>.dark` (no React state needed), so only the components that
   pick tokens from `resolvedTheme` in React go stale — `EstateGraph.tsx`
   (`graphTokens(theme)`) and its children (`GraphTooltip`/`GraphInspector`, tokens passed
   down). Result: the map's node/edge/plate palette is frozen at its mount-time theme.
   (The canvas *background* already flips — it was made transparent over the themed
   `bg-sunken` in commit `5e3ff17b` — this fixes the rest of the palette.)
2. **Reload-on-refresh papercut.** `components/FreshnessControls.tsx` polls
   `getRefreshStatus` every 4s and clears the interval when the run settles, but **never
   tells the parent to re-fetch**, so new domains/drafts don't appear until the user
   re-navigates (the "couldn't see domains after refresh" report).

> **Already done — do NOT rebuild:** backlog papercut (b) "render Page `body`" — the
> current `PageDraftCard.tsx` already renders `draft.body` in its Description block.

- **Project rules:** `AGENTS.md`
- **Targets:** `frontend/src/hooks/useTheme.ts`,
  `frontend/src/ontology/components/{EstateGraph.tsx (consumer, no change needed if the
  hook is fixed),FreshnessControls.tsx}`, `frontend/src/ontology/OntologyPage.tsx`

---

## Driver prompt (paste verbatim)

```text
GOAL: OFFLINE frontend-only polish on branch `ontology`. Two fixes, additive, no new dep, no backend, no deploy, no job run. (1) The Ontology Map ignores the app light/dark toggle — its node/edge/plate palette is frozen in one theme. (2) After a "Refresh ontology" run finishes, the Taxonomy/Tags/Drafts panels stay stale until the user re-navigates.

RULES: AGENTS.md. Frontend ONLY — do NOT touch backend, wheel, ddl, or docs. No new npm dep (package-lock.json byte-identical). TDD: write/adjust vitest first. Gates: `cd frontend && npm run test && npm run lint && npm run build` (tsc) all green. Commit on `ontology`, report diff + test summary, then STOP.

FIX 1 — THEME STALENESS (root cause, verified):
`hooks/useTheme.ts` keeps PER-INSTANCE React state (`useState(getStoredTheme())`) with NO shared store and NO DOM/storage listener. When `ThemeToggle` flips it updates localStorage + the `.dark` class on <html> + ITS OWN state copy; every OTHER `useTheme()` consumer keeps its mount-time value. The app restyles via Tailwind `dark:` + CSS vars keyed off `<html>.dark` (no React state), so ONLY components picking tokens from `resolvedTheme` in React go stale — `EstateGraph.tsx` (`graphTokens(theme)`) + its children (GraphTooltip/GraphInspector get tokens passed down).
FIX: make `resolvedTheme` REACTIVE to the applied class so all instances agree. In `useTheme`, on mount subscribe a MutationObserver to `document.documentElement` `class`-attribute changes AND a `storage` listener for STORAGE_KEY; recompute + re-render on either. `resolvedTheme` must reflect the live `<html>.dark` presence. Keep the return shape + toggles/applyTheme unchanged. Result: `EstateGraph`'s `graphTokens(resolvedTheme)` flips live with the toggle.
TESTS: (a) two `useTheme()` consumers — flip via one, assert BOTH `resolvedTheme` update; (b) render `EstateGraph` and assert it emits the DARK plate/typeFill hexes when `<html>` has `.dark`, LIGHT hexes otherwise (use the distinct values from graphTokens LIGHT vs DARK).

FIX 2 — RELOAD ON REFRESH COMPLETE:
`components/FreshnessControls.tsx` polls `getRefreshStatus` every 4s and clears the interval when `state` leaves running/queued, but never tells the parent to re-fetch — new domains/drafts don't appear.
FIX: add optional `onRefreshComplete?: () => void`; call it EXACTLY ONCE on the running/queued → terminal transition (done/succeeded/failed/skipped), in the poll settle branch (guard double-fire). In `OntologyPage.tsx` pass `onRefreshComplete={reload}` where `reload` re-runs the page's initial taxonomy/tags/drafts/graph fetch (reuse the existing loader; a bumpable `reloadKey` is fine). No other polling change.
TESTS: a simulated status transition fires `onRefreshComplete` once (not per-poll, not while still running/queued).

DO NOT BUILD: backlog papercut (b) ("Page body not rendered on PageDraftCard") is ALREADY DONE — `PageDraftCard.tsx` renders `draft.body` in the Description block. Skip it.

WORKFLOW: useTheme.ts (+test) → FreshnessControls.tsx + OntologyPage.tsx (+test) → gates. Stop if ambiguous.
```

---

## After the run (human-gated)

```bash
git diff --stat            # expect: frontend/src/hooks/useTheme.ts (+test),
                           #   frontend/src/ontology/components/FreshnessControls.tsx,
                           #   frontend/src/ontology/OntologyPage.tsx (+test)
cd frontend && npm run test && npm run lint && npm run build && cd ..
git add frontend/src
git commit -m "fix(ontology): live theme flip for the map palette (useTheme reactive to <html>.dark) + reload panels on refresh-complete"
git push origin ontology
```

Then **you** run the deploy-and-verify eyeball:

```bash
./scripts/deploy.sh --update   # SKIP_FRONTEND_BUILD OFF so the changes ship
# In the live app: toggle light/dark — the map's nodes/edges/plates flip immediately
#   (not just the background). Trigger "Refresh ontology"; when it completes the
#   Taxonomy/Tags/Drafts panels re-populate WITHOUT a manual reload.
```
