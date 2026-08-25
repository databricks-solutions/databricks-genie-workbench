/**
 * MV-advisor mockups — Prompt 12f fidelity-gate exports (REVIEW SCAFFOLD).
 *
 * These frames render the REAL production SemanticGraph (not a hand-drawn mockup
 * like 9a–9d) with representative fixture data, so the fidelity gate compares the
 * shipped component against the committed v7 contract frame (9e) side by side —
 * the gate the third look established after the v7 mockup was never committed and
 * the deployed canvas drifted from it (finding 1).
 *
 * What each frame proves, against 9e:
 *   - 12f-real-v7  : the 9e scenario (Revenue MV selected) through the real
 *     component — the spacious columns, typed FACT/DIM cards, per-box measure
 *     counts + governance roll-up, the select-time boundary, the visible control
 *     row (search · Lineage/Impact · Fit · − % + · Reset), the zoom indicator,
 *     the legend, and the footer tip. This is the density/typography reconcile.
 *   - 12f-real-3 / -10 / -30 : the §8 scale fixtures re-exported through the real
 *     component — 3 opens expanded, 30 opens collapsed (roll-up survives), so the
 *     density reads at the sizes a real space hits.
 *
 * Emitter renders with renderToStaticMarkup (effects do not run), so the canvas
 * frames via the SVG viewBox + preserveAspectRatio meet rather than the runtime
 * computeFit — the STATIC framing the reviewer compares for density; the runtime
 * viewport-fill is verified on the redeploy (a static export cannot exercise the
 * measured-viewport fit, which is why the drift reached production originally).
 */
import type { SemanticGraphEdge, SemanticGraphNode } from "@/types"
import { SemanticGraph } from "@/components/model/SemanticGraph"

// ── The 9e scenario as real graph data ───────────────────────────────────────
// Mirrors MvSemanticV7ContractFrame's model: two facts, three dims (one
// unmodeled), two governed MVs, declared joins + `uses` proof edges, and enough
// unnamed measures on Revenue to exercise the "+N unnamed" row. Revenue is the
// selected MV, so its member tables (orders, order_items, customer) wrap.
const V7_NODES: SemanticGraphNode[] = [
  { id: "sales.core.orders", kind: "table", label: "orders", col: 0, row: 0, coverage: 2 },
  { id: "sales.core.order_items", kind: "table", label: "order_items", col: 0, row: 1, coverage: 1 },
  { id: "sales.core.customer", kind: "table", label: "customer", col: 1, row: 0, coverage: 1 },
  { id: "sales.core.product", kind: "table", label: "product", col: 1, row: 1, coverage: 1 },
  { id: "sales.core.calendar_date", kind: "table", label: "calendar_date", col: 1, row: 2, coverage: 0 },
  { id: "sales.mv.revenue_mv", kind: "metric_view", label: "Revenue MV", col: 2, row: 0, definition_available: true },
  { id: "sales.mv.margin_mv", kind: "metric_view", label: "Margin MV", col: 2, row: 1, definition_available: true },
  { id: "measure:total_revenue", kind: "measure", label: "total_revenue", col: 3, row: 0, governance: "governed", origin: "Revenue MV (attached)" },
  { id: "measure:order_count", kind: "measure", label: "order_count", col: 3, row: 1, governance: "governed", origin: "Revenue MV (attached)" },
  // three unnamed (internal-token) measures on Revenue → the "+3 unnamed" row.
  { id: "measure:rev_u1", kind: "measure", label: "sum(count(?n))", col: 3, row: 2, governance: "governed" },
  { id: "measure:rev_u2", kind: "measure", label: "sug_ab12cd34", col: 3, row: 3, governance: "governed" },
  { id: "measure:rev_u3", kind: "measure", label: "avg(?s)", col: 3, row: 4, governance: "governed" },
  { id: "measure:gross_margin", kind: "measure", label: "gross_margin", col: 3, row: 5, governance: "governed", origin: "Margin MV (attached)" },
]

const V7_EDGES: SemanticGraphEdge[] = [
  // declared joins (proof arrows) — N:1 glyphs at rest.
  { from: "sales.core.order_items", to: "sales.core.orders", kind: "join", on: "order_items.order_id = orders.order_id", relationship: "many-to-one", scd2: false, weight: 2 },
  { from: "sales.core.orders", to: "sales.core.customer", kind: "join", on: "orders.customer_id = customer.id", relationship: "many-to-one", scd2: false, weight: 1 },
  // `uses` — the MV → member-table proof edges (Revenue's define its boundary).
  { from: "sales.mv.revenue_mv", to: "sales.core.orders", kind: "uses" },
  { from: "sales.mv.revenue_mv", to: "sales.core.order_items", kind: "uses" },
  { from: "sales.mv.revenue_mv", to: "sales.core.customer", kind: "uses" },
  { from: "sales.mv.margin_mv", to: "sales.core.orders", kind: "uses" },
  { from: "sales.mv.margin_mv", to: "sales.core.product", kind: "uses" },
  // membership — measures ride inside their owning MV card.
  { from: "measure:total_revenue", to: "sales.mv.revenue_mv", kind: "membership" },
  { from: "measure:order_count", to: "sales.mv.revenue_mv", kind: "membership" },
  { from: "measure:rev_u1", to: "sales.mv.revenue_mv", kind: "membership" },
  { from: "measure:rev_u2", to: "sales.mv.revenue_mv", kind: "membership" },
  { from: "measure:rev_u3", to: "sales.mv.revenue_mv", kind: "membership" },
  { from: "measure:gross_margin", to: "sales.mv.margin_mv", kind: "membership" },
]

export function RealModelV7Frame() {
  return (
    <div className="space-y-3 rounded-xl border border-default bg-surface p-4">
      <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">Semantic model</h3>
      <SemanticGraph
        nodes={V7_NODES}
        edges={V7_EDGES}
        selectedId="sales.mv.revenue_mv"
        label="Semantic model — real component, Revenue MV selected"
      />
    </div>
  )
}

// ── §8 scale fixtures (a star: 1 fact, N-1 dims, one MV, some standalone) ─────
function starSchema(
  tables: number,
  opts: { mvMeasures?: number; standalone?: number } = {},
): { nodes: SemanticGraphNode[]; edges: SemanticGraphEdge[] } {
  const { mvMeasures = 3, standalone = 2 } = opts
  const nodes: SemanticGraphNode[] = []
  const edges: SemanticGraphEdge[] = []
  for (let i = 0; i < tables; i++) {
    const id = `cat.sch.t${i}`
    nodes.push({ id, kind: "table", label: `t${i}`, col: i === 0 ? 0 : 1, row: i })
    if (i > 0) edges.push({ from: id, to: "cat.sch.t0", kind: "join", on: `t${i}.fk = t0.pk`, relationship: "many-to-one", scd2: false })
  }
  nodes.push({ id: "cat.sch.mv", kind: "metric_view", label: "orders_metrics", col: 2, row: 0, definition_available: true })
  edges.push({ from: "cat.sch.mv", to: "cat.sch.t0", kind: "uses" })
  for (let i = 0; i < mvMeasures; i++) {
    const id = `measure:gov_${i}`
    nodes.push({ id, kind: "measure", label: `gov_measure_${i}`, col: 3, row: i, governance: "governed", origin: "attached MV" })
    edges.push({ from: id, to: "cat.sch.mv", kind: "membership" })
  }
  for (let i = 0; i < standalone; i++) {
    const gov = i % 2 === 0 ? "ungoverned" : "curated"
    nodes.push({ id: `measure:free_${i}`, kind: "measure", label: `free_measure_${i}`, col: 3, row: i, governance: gov, origin: "proposal evidence" })
  }
  return { nodes, edges }
}

function ScaleFrame({ tables, mvMeasures, standalone }: { tables: number; mvMeasures?: number; standalone?: number }) {
  const { nodes, edges } = starSchema(tables, { mvMeasures, standalone })
  return (
    <div className="space-y-3 rounded-xl border border-default bg-surface p-4">
      <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">Semantic model · {tables} tables</h3>
      <SemanticGraph nodes={nodes} edges={edges} label={`Semantic model — ${tables} tables`} />
    </div>
  )
}

export function RealModel3Frame() {
  return <ScaleFrame tables={3} mvMeasures={3} standalone={2} />
}
export function RealModel10Frame() {
  return <ScaleFrame tables={10} mvMeasures={3} standalone={0} />
}
export function RealModel30Frame() {
  return <ScaleFrame tables={30} mvMeasures={4} standalone={3} />
}
