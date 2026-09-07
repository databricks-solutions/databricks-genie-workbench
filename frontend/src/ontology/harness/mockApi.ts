/**
 * Ontology Map — dev-only mock API seam (MV-D77). Fixture-backed `getGraph`, injected into
 * `EstateGraph` via the `EstateGraphApi` prop. NO live calls; NO new dependency (MV-D45).
 *
 * The captured `graph.applied.json` / `graph.proposed.json` payloads predate Lane D (no
 * `root` / `attach_level` / edge `verb`+`rel_class`), so they exercise the renderer's
 * **degrade** path (MV-D43). The synthesized `northstar` scene carries the full Lane-D
 * contract so the rich tree (org root, agent⊃mv⊃{measure,table}, typed verb cross-links,
 * tray + proposals) is tunable offline. Harness-only; never imported by the prod bundle.
 */
import type { EstateGraphApi } from "@/ontology/components/EstateGraph"
import type { GraphOrigin, OntologyGraph, OntologyGraphEdge, OntologyGraphNode } from "@/ontology/types"
import appliedRaw from "./fixtures/graph.applied.json?raw"
import proposedRaw from "./fixtures/graph.proposed.json?raw"

const APPLIED = JSON.parse(appliedRaw) as OntologyGraph
const PROPOSED = JSON.parse(proposedRaw) as OntologyGraph

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
    layout: "tree",
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

function node(p: Partial<OntologyGraphNode> & { id: string }): OntologyGraphNode {
  return {
    label: p.id,
    kind: "table",
    domain_id: null,
    parent_id: null,
    parent_name: null,
    x: 0,
    y: 0,
    size: 1,
    cost: null,
    member_count: null,
    origin: null,
    ...p,
  }
}

/**
 * Hand-authored Lane-D-shaped estate: an org root, two domains, a sub-domain, an agent that
 * contains a metric view (which contains a measure + a table), shared reference tables, typed
 * verb cross-links (shared + xdom), an ungrouped tray, and a proposed grouping over the tray.
 */
export function northstarGraph(): OntologyGraph {
  const domains: OntologyGraphNode[] = [
    node({ id: "d_fin", label: "Acme Finance", kind: "domain", member_count: 42, origin: "applied" }),
    node({ id: "d_ops", label: "Acme Operations", kind: "domain", member_count: 31, origin: "applied" }),
    node({ id: "s_rev", label: "Revenue Accounting", kind: "subdomain", parent_id: "d_fin", parent_name: "Acme Finance", member_count: 12, origin: "applied" }),
    node({ id: "sug_loyalty", label: "Loyalty", kind: "domain", member_count: 3, origin: "proposed" }),
    node({ id: "ungrouped", label: "Ungrouped", kind: "ungrouped", member_count: 5, origin: "proposed" }),
  ]
  const assets: OntologyGraphNode[] = [
    node({ id: "agent:finance", label: "Finance Agent", kind: "genie_agent", domain_id: "s_rev", attach_level: "subdomain", origin: "applied", cost: 1800 }),
    node({ id: "mv:net_sales", label: "net sales", kind: "metric_view", domain_id: "s_rev", parent_id: "agent:finance", attach_level: "asset", origin: "applied" }),
    node({ id: "measure:net_sales", label: "net_sales", kind: "measure", domain_id: "s_rev", parent_id: "mv:net_sales", attach_level: "asset", origin: "applied" }),
    node({ id: "table:fact_sales", label: "fact_sales_line", kind: "table", domain_id: "s_rev", parent_id: "mv:net_sales", attach_level: "asset", origin: "applied" }),
    node({ id: "table:ref_calendar", label: "ref_calendar", kind: "table", domain_id: "d_fin", attach_level: "domain", origin: "applied" }),
    node({ id: "dash:ops", label: "Ops Overview", kind: "dashboard", domain_id: "d_ops", attach_level: "domain", origin: "applied" }),
    node({ id: "table:ops_events", label: "ops_events", kind: "table", domain_id: "d_ops", attach_level: "domain", origin: "applied" }),
    // tray (ungrouped)
    node({ id: "table:u_ffp", label: "ffp_member", kind: "table", domain_id: "ungrouped" }),
    node({ id: "table:u_tier", label: "tier_status", kind: "table", domain_id: "ungrouped" }),
    node({ id: "table:u_promo", label: "promo_ledger", kind: "table", domain_id: "ungrouped" }),
    // proposed grouping members
    node({ id: "table:p_loyal1", label: "loyalty_txn", kind: "table", domain_id: "sug_loyalty" }),
    node({ id: "table:p_loyal2", label: "loyalty_bal", kind: "table", domain_id: "sug_loyalty" }),
  ]
  const edges: OntologyGraphEdge[] = [
    { src: "table:fact_sales", dst: "table:ref_calendar", kind: "join_key", weight: 3, verb: "joins calendar", rel_class: "shared" },
    { src: "table:fact_sales", dst: "table:ops_events", kind: "co_query", weight: 1, verb: "also queried with", rel_class: "xdom" },
    { src: "mv:net_sales", dst: "table:ops_events", kind: "lineage_adjacency", weight: 1, verb: "reads", rel_class: "xdom" },
  ]
  return {
    root: node({ id: "org:acme", label: "Acme", kind: "org", member_count: 73 }),
    domains: { nodes: domains, edges: [], truncated: false },
    assets: { nodes: assets, edges, truncated: false },
    layout: "tree",
    node_count: domains.length + assets.length + 1,
    edge_count: edges.length,
    state: "fresh",
    as_of: null,
  }
}

/** Prod-density stress graph (~2,900 nodes / ~1,560 edges) for the §6 perf gate. */
export function stressGraph(): OntologyGraph {
  const g = northstarGraph()
  const baseAssets = [...g.assets.nodes]
  const baseEdges = [...g.assets.edges]
  const copies = Math.ceil(2900 / baseAssets.length)
  for (let c = 1; c <= copies; c++) {
    for (const a of baseAssets) {
      const domain_id = a.domain_id && a.domain_id !== "ungrouped" ? a.domain_id : "d_ops"
      g.assets.nodes.push({
        ...a,
        id: `${a.id}__s${c}`,
        label: `${a.label}_${c}`,
        domain_id,
        parent_id: a.parent_id ? `${a.parent_id}__s${c}` : null,
      })
    }
  }
  const targetEdges = 1560
  let c = 1
  while (g.assets.edges.length < targetEdges && c <= copies) {
    for (const e of baseEdges) {
      if (g.assets.edges.length >= targetEdges) break
      g.assets.edges.push({ ...e, src: `${e.src}__s${c}`, dst: `${e.dst}__s${c}` })
    }
    c++
  }
  g.node_count = g.domains.nodes.length + g.assets.nodes.length + 1
  g.edge_count = g.domains.edges.length + g.assets.edges.length
  return g
}

export type MockScene =
  | "default" // captured applied fixture (degrade path)
  | "northstar" // synthesized full Lane-D contract
  | "stale" // applied fixture flagged stale
  | "empty" // honest-empty
  | "loading" // getGraph never resolves
  | "error" // getGraph rejects
  | "stress" // ~2,900 nodes (perf gate)
  | "proposed" // northstar viewed with proposals

export interface MockOptions {
  scene?: MockScene
}

/** The graph the harness hands `EstateGraph` as its `graph` prop for a scene. */
export function graphForScene(scene: MockScene): OntologyGraph {
  if (scene === "empty") return emptyGraph()
  if (scene === "stale") return staleGraph()
  if (scene === "northstar" || scene === "proposed") return northstarGraph()
  if (scene === "stress") return stressGraph()
  return appliedGraph()
}

/** Fixture-backed `EstateGraphApi` for one scene. */
export function createMockApi(opts: MockOptions = {}): EstateGraphApi {
  const scene = opts.scene ?? "default"
  const getGraph = async (origin: GraphOrigin): Promise<OntologyGraph> => {
    if (scene === "loading") return new Promise<OntologyGraph>(() => {}) // never settles
    if (scene === "error") throw new Error("The estate snapshot could not be read.")
    // The synthesized northstar carries applied tree + tray + proposals in ONE snapshot, so
    // it is returned for both origins; only the captured `default` scene swaps in the
    // pre-Lane-D proposed fixture on the proposed origin.
    if (scene === "northstar" || scene === "proposed" || scene === "stress") return graphForScene(scene)
    if (origin === "proposed") return proposedGraph()
    return graphForScene(scene)
  }
  return { getGraph }
}
