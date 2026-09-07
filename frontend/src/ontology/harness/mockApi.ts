/**
 * Ontology Map v3 — dev-only mock API seam (§2.2/§2.3, MV-D77).
 *
 * Fixture-backed implementations of the two runtime calls the map makes
 * (`getGraph`, `expandNode`), injected into `EstateGraph` through the
 * `EstateGraphApi` prop. NO new dependency (MV-D45) and NO live API calls:
 * the payloads are the real responses captured once from the deployed app
 * (see `fixtures/README.md`). Slow / failing variants are synthesized here
 * (delay / reject) so the loading & degraded states (MV-D43) can be tuned.
 *
 * This file lives under the dev-only harness and is never imported by the
 * prod bundle (nothing under `src/ontology/harness/` is reachable from
 * `index.html` / `App.tsx`).
 */
import type { EstateGraphApi } from "@/ontology/components/EstateGraph"
import type {
  GraphOrigin,
  OntologyGraph,
  OntologyGraphExpand,
  OntologyGraphNode,
} from "@/ontology/types"
import appliedRaw from "./fixtures/graph.applied.json?raw"
import proposedRaw from "./fixtures/graph.proposed.json?raw"
import expandMvRaw from "./fixtures/expand.mv.json?raw"
import expandSubdomainRaw from "./fixtures/expand.subdomain.json?raw"

// Parsed once at module load; every accessor below hands out fresh deep copies so a
// scene can mutate its copy (e.g. `stale`, `mv`) without polluting another scene.
const APPLIED = JSON.parse(appliedRaw) as OntologyGraph
const PROPOSED = JSON.parse(proposedRaw) as OntologyGraph
const EXPAND_MV = JSON.parse(expandMvRaw) as OntologyGraphExpand
const EXPAND_SUBDOMAIN = JSON.parse(expandSubdomainRaw) as OntologyGraphExpand

function clone<T>(v: T): T {
  return JSON.parse(JSON.stringify(v)) as T
}

export function appliedGraph(): OntologyGraph {
  return clone(APPLIED)
}

export function proposedGraph(): OntologyGraph {
  return clone(PROPOSED)
}

export function emptyGraph(): OntologyGraph {
  return {
    domains: { nodes: [], edges: [], truncated: false },
    assets: { nodes: [], edges: [], truncated: false },
    layout: "fr",
    node_count: 0,
    edge_count: 0,
    state: "cold",
    as_of: APPLIED.as_of ?? null,
  }
}

export function staleGraph(): OntologyGraph {
  const g = appliedGraph()
  g.state = "stale"
  return g
}

/**
 * The captured estate has no metric view in its asset level, but the MV expand fixture
 * was captured against a real metric view in the same workspace. This augment reveals
 * that real asset (id = the expand fixture's own `parent_id`) inside the first focused
 * sub-domain so the measures-satellite state is tunable offline. Harness-only.
 */
export function mvDemoGraph(): OntologyGraph {
  const g = appliedGraph()
  const firstSub = g.domains.nodes.find((n) => n.parent_id)
  const mv: OntologyGraphNode = {
    id: EXPAND_MV.parent_id,
    label: "cost attribution",
    kind: "metric_view",
    domain_id: firstSub?.id ?? null,
    parent_id: null,
    parent_name: null,
    x: 0,
    y: 0,
    size: 2,
    cost: null,
    member_count: null,
    origin: null,
  }
  g.assets.nodes = [...g.assets.nodes, mv]
  return g
}

/**
 * Prod-density stress graph (~2,890 nodes / ~1,560 edges — the §1C bar): the applied
 * fixture with each asset cloned deterministically into its own domain, plus cloned
 * intra-domain edges. Harness-only synthesis for smoothness/perf tuning — clearly not
 * estate truth, never shown as such (the harness banner names the scene).
 */
export function stressGraph(): OntologyGraph {
  const g = appliedGraph()
  // Snapshots, NOT aliases — we push into g.assets.* while iterating these.
  const baseAssets = [...g.assets.nodes]
  const baseEdges = [...g.assets.edges]
  const copies = Math.ceil((2892 - g.domains.nodes.length - baseAssets.length) / baseAssets.length)
  for (let c = 1; c <= copies; c++) {
    for (const a of baseAssets) {
      g.assets.nodes.push({ ...a, id: `${a.id}__s${c}`, label: `${a.label}_${c}` })
    }
  }
  const targetEdges = 1557
  let c = 1
  while (g.assets.edges.length < targetEdges && c <= copies) {
    for (const e of baseEdges) {
      if (g.assets.edges.length >= targetEdges) break
      g.assets.edges.push({ ...e, src: `${e.src}__s${c}`, dst: `${e.dst}__s${c}` })
    }
    c++
  }
  g.node_count = g.domains.nodes.length + g.assets.nodes.length
  g.edge_count = g.domains.edges.length + g.assets.edges.length
  return g
}

/** First non-Ungrouped top-level domain — the deterministic default drill target. */
export function autoFocusTop(g: OntologyGraph): { topId: string; name: string } | null {
  const top = g.domains.nodes.find((n) => !n.parent_id && n.kind !== "ungrouped" && n.id !== "ungrouped")
  return top ? { topId: top.id, name: top.label } : null
}

export type MockScene =
  | "default" // applied + proposed fixtures as captured
  | "mv" // applied augmented with the real metric view (measures expand)
  | "stale" // applied fixture flagged stale
  | "empty" // empty applied graph (honest-empty state)
  | "loading" // getGraph(proposed) never resolves
  | "error" // getGraph(proposed) rejects
  | "slow-expand" // expandNode resolves after a long delay
  | "fail-expand" // expandNode rejects
  | "stress" // ~2,892 nodes / ~1,557 edges (§1C prod-density smoothness check)

export interface MockOptions {
  scene?: MockScene
  /** Delay for the slow variants (default 4000ms). */
  delayMs?: number
}

function sleep(ms: number): Promise<void> {
  return new Promise((res) => setTimeout(res, ms))
}

/** The graph the harness should hand `EstateGraph` as its `graph` prop for a scene. */
export function graphForScene(scene: MockScene): OntologyGraph {
  if (scene === "empty") return emptyGraph()
  if (scene === "stale") return staleGraph()
  if (scene === "mv") return mvDemoGraph()
  if (scene === "stress") return stressGraph()
  return appliedGraph()
}

/** Fixture-backed `EstateGraphApi` for one scene. */
export function createMockApi(opts: MockOptions = {}): EstateGraphApi {
  const scene = opts.scene ?? "default"
  const delayMs = opts.delayMs ?? 4000

  const getGraph = async (origin: GraphOrigin): Promise<OntologyGraph> => {
    if (scene === "loading") return new Promise<OntologyGraph>(() => {}) // never settles
    if (scene === "error") throw new Error("The estate snapshot could not be read.")
    return origin === "proposed" ? proposedGraph() : graphForScene(scene)
  }

  const expandNode = async (node: string): Promise<OntologyGraphExpand> => {
    if (scene === "fail-expand") {
      await sleep(250)
      throw new Error("Couldn't load the details right now.")
    }
    if (scene === "slow-expand") await sleep(delayMs)
    // Metric views expand to measures; everything else (sub-domain rollups) to Pages.
    const payload = clone(node.startsWith("mv:") ? EXPAND_MV : EXPAND_SUBDOMAIN)
    // Re-anchor the captured payload onto the node that was actually tapped, so any
    // sub-domain shows a realistic expand (the live API returns children of that node).
    const capturedParent = payload.parent_id
    payload.parent_id = node
    for (const e of payload.edges) {
      if (e.src === capturedParent) e.src = node
      if (e.dst === capturedParent) e.dst = node
    }
    return payload
  }

  return { getGraph, expandNode }
}
