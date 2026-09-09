# Ontology Map — "Buttery interactions" Goal-Mode driver

Copy-paste launcher for making the **Ontology Map** (`EstateGraph.tsx`) feel like the
north-star mockup — smooth glide on expand/collapse, drag that moves one node + its
edges per frame, and a transitioned camera — with **no new npm dependency** and, just
as importantly, **no per-frame full React relayout**. Run it on the **`ontology`**
branch. Frontend-only, additive; ends offline-green and **STOPs before deploy**.

## Why this matters (two problems, one refactor)

1. **Feel.** The north-star mockup
   (`docs/design/mockups/genie-ontology-knowledge-graph-v2.html`) gets its "butter" from
   d3 enter/update/exit transitions (`duration(420) easeCubicOut`), imperative drag
   (`redrawEdges()` moves the dragged node + its edges only), and an imperative /
   transitioned camera (`svg.transition().call(zoom.transform, …)`). Today's
   `EstateGraph.tsx` snaps: node positions are static SVG `transform`/`d` attributes with
   no transition.
2. **Scale.** The current drag fires `setDragTick` on **every** pointer-move → a full
   `layoutTree()` relayout **+ full React re-render per frame** (O(N)/frame), and zoom
   fires `setTransform` per tick → React re-render per frame. On a large, fully-expanded
   estate (up to the `TOP_N_BY_CENTRALITY = 2000` display cap) that is the visual's
   performance cliff. Making drag and zoom **O(1) per frame** is the same change that
   makes the map hold up at enterprise scale — so this driver is *also* the render-side
   scale-hardening lever (see `backlog.md` → "Scale hardening").

## Hard constraints

- **No new npm dep.** `d3-transition` / `d3-ease` are **not** installed, so the smooth
  camera + glide must be **hand-rolled rAF tweens** (`easeCubicOut = 1-(1-t)^3`). You may
  NOT reach for `svg.transition().call(zoom.transform, …)`.
- **Reduced-motion aware.** `matchMedia('(prefers-reduced-motion: reduce)')` ⇒ every
  transition is instant, i.e. byte-identical to today's positions (a11y + determinism).
- **Frontend only** (`frontend/src/ontology/*`). No backend, wheel, ddl, or docs.
- **Determinism preserved.** `EstateGraphHandle.positions()` already reads layout coords
  (not the animating DOM), so the harness stays stable — but the screenshot harness must
  render the **final** frame (force reduced-motion / `motion=off`), never mid-animation.

## Documented tradeoff

React unmounts a removed node immediately, so **collapse fades/snaps** rather than
animating the child back into its parent. Enter + glide + drag + camera are the buttery
85%; symmetric exit animation (delayed-unmount) is an explicit follow-up, out of scope
here.

- **Project rules:** `AGENTS.md`
- **Mockup (feel reference):** `docs/design/mockups/genie-ontology-knowledge-graph-v2.html`
- **Target:** `frontend/src/ontology/components/EstateGraph.tsx`,
  `frontend/src/ontology/ontologyTreeLayout.ts`, harness
  (`frontend/src/ontology/harness/main.tsx`, `harness/tools/matrix.mjs`)

---

## Driver prompt (paste verbatim)

```text
GOAL: OFFLINE frontend-only "buttery interactions" for the Ontology Map (frontend/src/ontology/components/EstateGraph.tsx) on branch `ontology`. Make expand/collapse, drag, and pan/zoom feel like the north-star mockup (docs/design/mockups/genie-ontology-knowledge-graph-v2.html): smooth glide on relayout, drag that moves ONE node + its edges per frame, a transitioned camera — with NO new npm dep and NO per-frame full React relayout.

RULES: AGENTS.md. Frontend ONLY (frontend/src/ontology/*). NO new npm dep (package-lock.json byte-identical; d3-transition/d3-ease are NOT installed — hand-roll rAF tweens with easeCubicOut = 1-(1-t)^3). TDD on pure helpers. Gates: `cd frontend && npm run test && npm run lint && npm run build` all green. Commit on `ontology`, report diff + tests, then STOP (no deploy).

DIAGNOSIS (current jank):
- Drag effect (~L431): setDragTick on EVERY drag event → full layoutTree() + full React re-render per pointer-move (O(N)/frame). THE main jank + scaling cliff.
- Zoom effect (~L351): setTransform on EVERY zoom tick → React re-render per frame.
- Nodes/spine/cross-links use a static SVG transform/`d` from layout → expand/collapse SNAPS.

BUILD (ALL reduced-motion aware: matchMedia('(prefers-reduced-motion: reduce)') ⇒ instant = today's behavior):
1) PATH BUILDERS: export pure spinePath(sx,sy,tx,ty)+crossPath(...) from ontologyTreeLayout.ts; USE them in BOTH render AND the imperative redraws below so an edge can be recomputed from moved endpoints. Unit-test them.
2) IMPERATIVE ZOOM (O(1)/frame): in the zoom handler write transform straight to gRef.current (setAttribute) each tick; sync React `transform` state only on 'end' (+ once via rAF for the minimap viewport). No per-frame React render.
3) IMPERATIVE DRAG (O(1)/frame): tag each node <g> data-node-id (exists) and each edge path data-src/data-dst. On 'drag' move ONLY this node's <g> transform + recompute the `d` of its incident spine+cross paths, accumulating offset locally. On 'end' commit to offsetsRef + ONE setDragTick. Remove the per-event setDragTick. layoutTree MUST NOT run mid-drag (assert with a spy).
4) GLIDE ON RELAYOUT: keep prev render positions; on layout change run ONE rAF tween t:0→1 (~380ms, easeCubicOut) interpolating each surviving node prev→next, driving BOTH node transforms AND incident edge `d` (via §1) so edges stay glued (no snap). Entering nodes fade+scale in from their parent's position; exiting nodes snap on unmount (documented tradeoff). Pure tween/interp helper unit-tested.
5) TRANSITIONED CAMERA: Fit/Reset buttons rAF-tween {x,y,k} (~480ms easeCubicOut), then sync d3-zoom internal state ONCE at the end (call zoom.transform) so the next pan doesn't jump. AUTO-fits (initial mount, fullscreen enter/exit, reveal) stay INSTANT.
6) HARNESS DETERMINISM: the screenshot harness (harness/main.tsx, harness/tools/matrix.mjs) must render FINAL state — force reduced-motion/motion=off so shots aren't mid-animation. EstateGraphHandle.positions() reads layout coords (stable) — keep it.

ACCEPTANCE: (i) drag = O(1) DOM writes/frame, layoutTree NOT called mid-drag (spy); (ii) zoom syncs state on 'end', not per tick; (iii) expand/collapse glides nodes AND edges together (no edge snap); (iv) reduced-motion ⇒ instant + byte-identical positions to today; (v) no new npm dep; (vi) tsc+lint+vitest green; harness final-frame shots unchanged.

WORKFLOW: ontologyTreeLayout.ts (export builders +tests) → EstateGraph zoom effect → drag effect → glide tween + render-pos → camera → harness motion-off → gates. Stop if ambiguous.
```

---

## After the run (human-gated)

```bash
git diff --stat            # expect: frontend/src/ontology/ontologyTreeLayout.ts (+test),
                           #   components/EstateGraph.tsx, harness/{main.tsx,tools/matrix.mjs}
cd frontend && npm run test && npm run lint && npm run build && cd ..
git add frontend/src/ontology
git commit -m "feat(ontology): buttery map interactions — O(1)/frame drag+zoom, rAF glide on relayout, transitioned camera (no dep, reduced-motion aware)"
git push origin ontology
```

Then **you** run the deploy-and-verify eyeball:

```bash
./scripts/deploy.sh --update   # SKIP_FRONTEND_BUILD OFF so the interaction changes ship
# In the live app: drag a node (only it + its edges move, buttery), expand/collapse
#   (nodes AND edges glide together), click Fit/Reset (camera glides), toggle OS
#   reduced-motion (everything becomes instant). Confirm no stutter when Expand-all
#   is near the display cap.
```
