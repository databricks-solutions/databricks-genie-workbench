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
import type { MvProposal, SemanticGraphEdge, SemanticGraphNode, SemanticGraphResponse } from "@/types"
import { SemanticGraph } from "@/components/model/SemanticGraph"
import { SemanticModelView, withOverlay } from "@/components/model/SemanticModelTab"

// ── The 9e scenario as real graph data ───────────────────────────────────────
// Mirrors MvSemanticV7ContractFrame's model: two facts, three dims (one
// unmodeled), two governed MVs, declared joins + `uses` proof edges, and enough
// unnamed measures on Revenue to exercise the "+N unnamed" row. Revenue is the
// selected MV, so its member tables (orders, order_items, customer) wrap.
const V7_NODES: SemanticGraphNode[] = [
  // Round-5: `role` is the PROVEN fact/dim tag — orders is Revenue MV's source
  // (fact), customer is an MV-joined table (dim); the rest stay neutral so the
  // frame exercises the "TABLE" caption too rather than guessing from the column.
  { id: "sales.core.orders", kind: "table", label: "orders", col: 0, row: 0, coverage: 2, role: "fact" },
  { id: "sales.core.order_items", kind: "table", label: "order_items", col: 0, row: 1, coverage: 1 },
  { id: "sales.core.customer", kind: "table", label: "customer", col: 1, row: 0, coverage: 1, role: "dim" },
  { id: "sales.core.product", kind: "table", label: "product", col: 1, row: 1, coverage: 1, role: "dim" },
  { id: "sales.core.calendar_date", kind: "table", label: "calendar_date", col: 1, row: 2, coverage: 0 },
  {
    id: "sales.mv.revenue_mv", kind: "metric_view", label: "Revenue MV", col: 2, row: 0,
    definition_available: true,
    mv_source: "sales.core.orders",
    // 12f: the rest of the parsed YAML the payload now carries, so the exported
    // inset exercises the join tree / dimensions-by-binding / filter+served rows
    // the 9e contract draws rather than only the sections that predate them.
    mv_filter: "order_status != 'CANCELLED'",
    materialization: "2 materializations · EVERY 1 DAY",
    dimensions: [
      { name: "order_day", expr: "date_trunc('DAY', orders.order_ts)", binding: "sales.core.orders" },
      { name: "channel", expr: "orders.channel", binding: "sales.core.orders" },
      { name: "region", expr: "customer.region", binding: "sales.core.customer" },
      { name: "segment", expr: "customer.segment", binding: "sales.core.customer" },
    ],
  },
  { id: "sales.mv.margin_mv", kind: "metric_view", label: "Margin MV", col: 2, row: 1, definition_available: true },
  { id: "measure:total_revenue", kind: "measure", label: "total_revenue", col: 3, row: 0, governance: "governed", origin: "Revenue MV (attached)", expr: "sum(order_items.quantity * order_items.unit_price)", description: "Gross revenue across order lines, pre-refund." },
  { id: "measure:order_count", kind: "measure", label: "order_count", col: 3, row: 1, governance: "governed", origin: "Revenue MV (attached)", expr: "count(distinct orders.order_id)" },
  // three unnamed (internal-token) measures on Revenue → the "+3 unnamed" row.
  { id: "measure:rev_u1", kind: "measure", label: "sum(count(?n))", col: 3, row: 2, governance: "governed" },
  { id: "measure:rev_u2", kind: "measure", label: "sug_ab12cd34", col: 3, row: 3, governance: "governed" },
  { id: "measure:rev_u3", kind: "measure", label: "avg(?s)", col: 3, row: 4, governance: "governed" },
  { id: "measure:gross_margin", kind: "measure", label: "gross_margin", col: 3, row: 5, governance: "governed", origin: "Margin MV (attached)" },
  // Loose space-config measures — round-5: their OWN "Space config · measures"
  // column to the right of the metric views. One reuses a governed name under a
  // different expression, which is the overlap warning on that column.
  { id: "measure:aov", kind: "measure", label: "avg_order_value", col: 3, row: 6, governance: "curated", origin: "curated SQL", expr: "sum(orders.amount) / count(distinct orders.order_id)" },
  { id: "measure:dup_revenue", kind: "measure", label: "total_revenue", col: 3, row: 7, governance: "curated", origin: "curated SQL", overlaps: "sales.mv.revenue_mv", expr: "sum(orders.amount)" },
  { id: "measure:refund_rate", kind: "measure", label: "refund_rate", col: 3, row: 8, governance: "ungoverned", origin: "proposal evidence · 9×" },
  { id: "measure:loose_u1", kind: "measure", label: "sum(?n)", col: 3, row: 9, governance: "ungoverned" },
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

// 12f round 2: this frame renders the REAL FULL SURFACE (SemanticModelView —
// panel header, governance ladder, canvas, curator inset), not the bare canvas.
// The bare-canvas version made the comparison against 9e unfair in reality's
// favour-of-doubt: 9e depicts the whole panel including the inset, so exporting
// only the graph hid the surface where most of the polish gap actually lived.
export function RealModelV7Frame() {
  return (
    <SemanticModelView
      graph={{ space_id: "space-v7", nodes: V7_NODES, edges: V7_EDGES, proposals: [], coverage_status: "ok", coverage_reason: null }}
      isLoading={false}
      error={null}
      onRefresh={() => {}}
      initialSelectedId="sales.mv.revenue_mv"
    />
  )
}

// ── Round-6: the proposal overlay ON, through the REAL component ──────────────
// The reviewer reported the overlay "hid measures" and showed "no proposals":
// the old overlay added a membership edge that MOVED the loose measure into an
// off-screen ghost card. Round-6 KEEPS the measure in Space config and draws a
// dashed "would govern →" link from a visible ghost proposed-MV card instead.
// This frame exports that state (no selection) so the gate has a checked-in
// picture of it: the ghost "refund_rate" MV card, the "would govern" link, and
// the loose measures still chipped in the Space-config column.
//
// withOverlay only reads `proposed_object`; the rest of MvProposal is irrelevant
// to the overlay geometry, so the fixture casts a minimal shape (scaffold only).
const OVERLAY_PROPOSALS: MvProposal[] = [
  { proposed_object: "sales.mv.refund_rate" } as unknown as MvProposal,
]

export function RealModelOverlayFrame() {
  const base: SemanticGraphResponse = {
    space_id: "space-v7-overlay",
    nodes: V7_NODES,
    edges: V7_EDGES,
    proposals: OVERLAY_PROPOSALS,
    coverage_status: "ok",
    coverage_reason: null,
  }
  const { nodes, edges } = withOverlay(base)
  return (
    <div className="space-y-3 rounded-xl border border-default bg-surface p-4">
      <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">Semantic model · proposal overlay ON</h3>
      <SemanticGraph nodes={nodes} edges={edges} label="Semantic model — proposal overlay ON" />
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
