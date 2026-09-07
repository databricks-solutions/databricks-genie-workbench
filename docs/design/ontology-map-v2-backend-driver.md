# Ontology Map v2 — **backend** Goal-Mode driver (Lane 2 of Wave 3 — MV-D73/74 routes)

## ⚙️ Parallel-build lane header — Lane 2 of Wave 3 (READ FIRST)

You run in an **isolated git worktree** off the `ontology` HEAD (`isolation: worktree`,
`worktree.baseRef: "head"`). Two sibling lanes edit the repo concurrently. This lane owns a
disjoint subtree (`backend/ontology/**` + a backend test), so merges are conflict-free — keep it
that way. It reads Lane 1's blob shape and exposes it to Lane 3 over the frozen API contract below.

- **OWNS (create/edit freely):**
  - `backend/ontology/routers/graph.py` — add `?origin=` to `GET /graph`; **append** the new
    `GET /graph/expand` handler at the END (do not reflow the existing `_level`/`get_ontology_graph`).
  - `backend/ontology/models.py` — **append** `origin` to `OntologyGraphNode` and add
    `OntologyGraphExpand` at the END.
  - `backend/ontology/services/mirror.py` — **append** a read helper only if needed (reuse
    `read_graph_snapshot`; add a bounded `read_pages_by_domain` only if one does not already exist).
  - `backend/tests/test_ontology_graph_route.py` — **new** unit + route tests (or append to the
    existing graph-route test if present).
- **REUSE by import only (do NOT edit):** the wheel is import-only this wave; the blob shape is
  Lane 1's (MV-D73). If a blob key you need is missing, that is a **finding to report — never edit
  the wheel from this lane**.
- **OFF-LIMITS (do NOT touch):** all of `frontend/`, all of `packages/…/genie_space_optimizer/`,
  `backend/ontology/routers/{apply,preflight,taxonomy,tags,settings,refresh,inventory,drafts}.py`,
  and `docs/design/mv-advisor-playbook.md`.
- **MERGE-ORDER:** after Lane 1; **before Lane 3** (Lane 3's UI targets these endpoints).
- **Launch:** via the `ontology-lane-builder` subagent — see `ontology-wave3-launcher.md`.

### 🔒 Frozen API contract (Lane 2 implements; Lane 3 mirrors — do not drift)

- `GET /api/ontology/graph?origin=applied|proposed` — **default `applied`**. Filters `domains.nodes`
  (and, at the assets level, their member assets) by node `origin`; `ungrouped` is always included
  (neutral). Same `OntologyGraph` response shape as today + `origin` on each node.
- `GET /api/ontology/graph/expand?node=<id>&origin=applied|proposed`
  → `OntologyGraphExpand { nodes: OntologyGraphNode[], edges: OntologyGraphEdge[], parent_id: str,
    as_of: str | null }` — the children of ONE node from the blob's `snippets[node]` (measures →
    `kind="measure"` nodes + `mv_measure` edges; pages → `kind="page"` nodes + `page_source` edges).
    Bounded; any miss/failure ⇒ empty children (never a 500 — MV-D43).
- `OntologyGraphNode.origin: str | None` (default None; mirrors the blob).

---

## Spec & decisions

- **Spec (source of truth):** `docs/design/ontology-map-v2-build.md` §2.3 (expand), §2.4 (contracts),
  §3 (Applied-vs-Proposed). **Decisions:** `mv-advisor-playbook.md` **MV-D73/MV-D74**; honor
  **MV-D43** (degrade), **MV-D49** (no DDL — read the blob), **MV-D45** (no new dep), read-only.
- **Project rules:** `AGENTS.md`. Read `routers/graph.py` + `models.py` first.

---

## Driver prompt (paste verbatim into the subagent)

```text
GOAL: Ontology Map v2 — BACKEND ONLY. Expose the MV-D73 layered blob to the UI: (1) add ?origin=
applied|proposed (default applied) to GET /graph so proposed engine-clusters are distinguishable from
applied governed-tag domains (MV-D74), (2) add a read-only GET /graph/expand that returns ONE node's
children (MV measures + attached Pages) from the blob's snippets index for expand-on-demand (MV-D73
§2.3). Read-only, additive, NO DDL (read the blob — MV-D49); offline code + green tests; STOP before
deploy. Branch: ontology. NO frontend, NO wheel edits, NO new router.

SPEC: docs/design/ontology-map-v2-build.md §2.3/§2.4/§3. DECISIONS: MV-D73/D74; honor MV-D43/D49/D45.
RULES: AGENTS.md. Read routers/graph.py + models.py first.

CONTEXT: routers/graph.py serves genie_ont_graph_snapshot from the mirror via mirror.read_graph_snapshot
+ _level()->OntologyGraphLevel; degrades to state="cold" on any miss (MV-D43). Lane 1 (MV-D73) adds to
the blob: origin on every domains.nodes rollup; subdomains:{edges}; snippets:{ "<node_id>":{measures:
[{ref,name,expression,fmt}], pages:[{page_id,title,archetype,domain_id}]} }. OntologyGraphNode/Edge/
Level/Graph already exist in models.py; the "measure" kind is already reserved in the frontend.

BUILD A — models (append to backend/ontology/models.py):
  OntologyGraphNode gains origin: str | None = None (Pydantic then keeps the blob's origin instead of
  dropping it). Add OntologyGraphExpand(nodes: list[OntologyGraphNode], edges: list[OntologyGraphEdge],
  parent_id: str, as_of: str | None = None).

BUILD B — GET /graph gains ?origin (routers/graph.py):
  origin: str = "applied" query param (validate ∈ {"applied","proposed"}; anything else ⇒ "applied").
  After reading the blob, filter domains.nodes to those whose origin==origin OR kind=="ungrouped";
  at the assets level keep only assets whose domain_id maps to a kept rollup (+ ungrouped); drop edges
  whose endpoints were filtered (emitted-only guard). Empty/cold blob ⇒ state="cold" unchanged.

BUILD C — GET /graph/expand (append handler to routers/graph.py):
  params node: str, origin: str = "applied". Resolve metastore_id (ont_settings._metastore_id), read the
  snapshot blob, slice snippets.get(node) ⇒ build children: each measure ⇒ OntologyGraphNode(id=f"measure:
  {m['ref']}", label=m['name'], kind="measure", domain_id=<parent's domain>) + edge {src:node, dst:that id,
  kind:"mv_measure"}; each page ⇒ node(id=f"page:{p['page_id']}", label=p['title'], kind="page",
  domain_id=p['domain_id']) + edge {src:node, dst:that id, kind:"page_source"}. Bounded (cap children).
  Missing node/blob/snippets ⇒ OntologyGraphExpand(nodes=[],edges=[],parent_id=node) — never 500.

HARD GUARDRAILS: read-only (NO UC/tag write); additive — ?origin on the EXISTING route + ONE appended
handler + append-only models; NO new router, NO new DDL/column (read the blob, MV-D49); NO new
dependency (MV-D45); degrade-not-hang — every miss/failure ⇒ typed empty, never 500 (MV-D43). NEVER
touch frontend/, the wheel, or the playbook.

ACCEPTANCE (offline, backend/tests/test_ontology_graph_route.py):
  - a fixture blob with one applied + one proposed rollup: /graph (default) returns the applied rollup +
    ungrouped, not the proposed; /graph?origin=proposed returns the proposed; assets filtered to kept
    rollups; no dangling edges.
  - /graph/expand?node=mv:<fqn> returns the measures as kind="measure" nodes + mv_measure edges;
    node=<sub domain_id> returns the pages as kind="page" nodes + page_source edges; unknown node ⇒
    empty expand, HTTP 200.
  - cold/empty snapshot ⇒ /graph state="cold", /graph/expand empty — no 500.
  - ./scripts/test.sh green; the wheel suite green (unchanged); uv.lock / package-lock.json untouched.

WORKFLOW: branch ontology. Do NOT deploy or run the job. When offline-green, STOP and report the
worktree branch, `git diff --stat`, and the test summary; a human runs the deploy-verify gate.
```

---

## After the run (human-gated — runs with Lane 3 merged)

Full `./scripts/deploy.sh --update`; hit `/api/ontology/graph?origin=applied` (default) and
`?origin=proposed`, and `/graph/expand?node=mv:<fqn>` / `?node=<sub domain_id>`; confirm typed
children come back and cold estates stay 200. Record the pass in the playbook (human edit).
