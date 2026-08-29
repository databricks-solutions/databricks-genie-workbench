/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * Frame 9e — the v7 SEMANTIC-CANVAS CONTRACT (Prompt 12f step 0).
 *
 * The v3 note (semantic-graph-v3-note.md §3–§4) named the interactive v7 frame
 * "the visual contract" but it was never committed — it lived only in the
 * brainstorm session that produced it, so Prompt 12e landed the mechanics
 * faithfully and missed the experience (third-look finding 1). This frame
 * COMMITS that contract as a static, `renderToStaticMarkup`-testable emitter
 * mockup (exported both themes by frames.tsx), so the fidelity gate has a
 * concrete reference and Prompt 12f step 1 can reconcile the deployed
 * SemanticGraph to a checked-in picture instead of a memory.
 *
 * ROUND-5 SUPERSEDES parts of this static frame. After the reviewer's round-5
 * feedback the AUTHORITATIVE post-round-5 export is the REAL-component frame
 * `RealModelV7Frame` (Mv12fFidelityFrames.tsx), which renders the production
 * SemanticGraph and therefore carries every round-5 change automatically:
 *   - loose measures live in their OWN "Space config · measures" column to the
 *     RIGHT of the metric views (not stacked below them);
 *   - fact/dim captions are shown ONLY when a metric view definition PROVES the
 *     role (node.role), else a neutral "TABLE";
 *   - the palette is calmer (near-neutral MV boxes, neutral coverage chips) with
 *     the accent reserved for selection/lineage;
 *   - definition (dotted `uses`) edges render only for the selected MV;
 *   - the legend disambiguates the solid join from the dotted MV-definition line.
 * This hand-drawn frame is kept as the step-0 layout reference; its geometry is
 * intentionally not re-hand-drawn for round-5.
 *
 * ROUND-6 ALSO SUPERSEDES parts of this frame; the authoritative export stays the
 * REAL-component frames (Mv12fFidelityFrames.tsx), which carry round-6 for free:
 *   - joins are a faint at-rest SKELETON; selecting a node lights its neighbours
 *     and DIMS every other edge to near-invisible (focus+context, reference
 *     BlueprintEdge) — the fix for "arrows are hodge podge";
 *   - join labels are ON DEMAND only (hover / an endpoint IS the selection) — no
 *     "N:1"/"ON …" glyph floats at rest (this frame's DeclaredArrow still hand
 *     -draws an at-rest "N:1", which the real UI no longer does);
 *   - wider inter-column gutter + row spacing so curves have room to separate;
 *   - the proposal overlay KEEPS loose measures in Space config and links them to
 *     a visible ghost MV with a dashed "would govern →" edge (see 9j).
 *
 * It is rebuilt from the v3 note's spec (§3 the model, §4 interaction) and the
 * reviewer's approved v7 screenshot recorded in the third-look review. What it
 * encodes, point for point:
 *   - Deduplicated relational canvas: every table appears ONCE (constraint 1).
 *   - Typed table cards (fact vs dim) with a coverage dot (governed if used by
 *     ≥1 MV, gap if unmodeled). (Round-5: the real UI proves the role or stays
 *     neutral rather than typing every card.)
 *   - Measures boxed by owner; each box carries an "N measures · <governance>"
 *     chip; unnamed measures collapse to a "+N unnamed" row, never internals
 *     (12d / MV-D29 hygiene, no ?n/?s/sug_ labels).
 *   - A Space-config box for loose measures. (Round-5: its OWN column to the
 *     RIGHT of the MV boxes; earlier rounds put it below the canvas (3) or under
 *     the MV boxes in the same column (4).)
 *   - Arrows only where a join is declared (constraint 2); the unmodeled table
 *     draws ZERO arrows and sits in a neutral "Unmodeled · no MV" region.
 *   - Selection state (Revenue MV selected): a labeled boundary WRAPS exactly
 *     its member tables (Euler-contiguous), its declared arrows highlight, its
 *     measures light, the rest dims.
 *   - A visible control row (search · Lineage/Impact · Fit · − % + · Reset), a
 *     zoom indicator, a legend, a footer tip, and the curator inset (join tree
 *     from YAML, dimensions by binding, governance roll-up, filter/mat, reuse).
 *
 * The render is a pure function of a static model + a fixed selection — no
 * state, no layout library (v3 §7/§8). Disposed with the rest of the scaffold.
 */
import type { ReactNode } from "react"
import {
  AlertTriangle,
  Crosshair,
  Layers,
  Maximize2,
  Minus,
  Plus,
  RotateCcw,
  Search,
  ShieldCheck,
  Wrench,
} from "lucide-react"
import { Badge } from "@/components/ui/badge"

// ── Deterministic layered layout (spacious — v3 §3 "spacious columns") ───────
// Three ranks: source facts · dimensions · metric-view measure boxes. Home
// layout keeps each MV's member tables spatially contiguous (v3 §6) so the
// select-time boundary is a tidy rectangle enclosing ONLY its members — here
// Revenue's members (orders, order_items, customer) occupy col0 rows 0–1 + col1
// row 0, and the one foreign dim (product, Margin's) sits at col1 row 2, clear
// of the wrap rectangle.
const TABLE_W = 188
const TABLE_H = 54

interface TableCard {
  id: string
  label: string
  type: "fact" | "dim"
  x: number
  y: number
  /** used by ≥1 MV → governed coverage; unmodeled → gap. */
  coverage: "governed" | "gap"
  usedByCount: number
}

const TABLES: TableCard[] = [
  { id: "orders", label: "orders", type: "fact", x: 40, y: 92, coverage: "governed", usedByCount: 2 },
  { id: "order_items", label: "order_items", type: "fact", x: 40, y: 176, coverage: "governed", usedByCount: 1 },
  { id: "customer", label: "customer", type: "dim", x: 300, y: 92, coverage: "governed", usedByCount: 1 },
  { id: "product", label: "product", type: "dim", x: 300, y: 260, coverage: "governed", usedByCount: 1 },
  { id: "calendar_date", label: "calendar_date", type: "dim", x: 300, y: 372, coverage: "gap", usedByCount: 0 },
]
const TABLE_BY_ID = new Map(TABLES.map((t) => [t.id, t]))

// Metric-view boxes (col 2). Each owns measures + declares the tables it uses.
interface MvBox {
  id: string
  label: string
  x: number
  y: number
  w: number
  h: number
  governance: "governed"
  measureCount: number
  named: string[]
  unnamed: number
  uses: string[]
}
const REVENUE: MvBox = {
  id: "revenue_mv",
  label: "Revenue MV",
  x: 620,
  y: 80,
  w: 336,
  h: 150,
  governance: "governed",
  measureCount: 5,
  named: ["total_revenue", "order_count"],
  unnamed: 3,
  uses: ["orders", "order_items", "customer"],
}
const MARGIN: MvBox = {
  id: "margin_mv",
  label: "Margin MV",
  x: 620,
  y: 258,
  w: 336,
  h: 96,
  governance: "governed",
  measureCount: 1,
  named: ["gross_margin"],
  unnamed: 0,
  uses: ["orders", "product"],
}

const SELECTED = REVENUE // v7 reference frame is captured with Revenue selected.

function rightEdge(t: TableCard) {
  return { x: t.x + TABLE_W, y: t.y + TABLE_H / 2 }
}
function leftEdge(b: MvBox, y: number) {
  return { x: b.x, y }
}

function CoverageDot({ coverage, cx, cy }: { coverage: "governed" | "gap"; cx: number; cy: number }) {
  const color = coverage === "governed" ? "var(--color-success)" : "var(--color-danger)"
  return <circle cx={cx} cy={cy} r="4.5" fill={color} opacity={coverage === "governed" ? 0.9 : 0.75} />
}

function TableNode({ t, dimmed, inWrap }: { t: TableCard; dimmed: boolean; inWrap: boolean }) {
  return (
    <g opacity={dimmed ? 0.4 : 1}>
      <rect
        x={t.x}
        y={t.y}
        width={TABLE_W}
        height={TABLE_H}
        rx="8"
        fill="var(--bg-surface)"
        stroke={inWrap ? "var(--color-accent)" : "var(--border-color-strong)"}
        strokeWidth={inWrap ? "1.75" : "1"}
      />
      {/* type label (fact vs dim) — the "typed card" */}
      <text x={t.x + 14} y={t.y + 20} className="fill-[var(--text-muted)]" fontSize="8.5" fontWeight="700" letterSpacing="0.06em">
        {t.type === "fact" ? "FACT" : "DIM"}
      </text>
      <text x={t.x + 14} y={t.y + 39} className="fill-[var(--text-primary)]" fontSize="13" fontWeight="600" fontFamily="monospace">
        {t.label}
      </text>
      {/* coverage dot, top-right */}
      <CoverageDot coverage={t.coverage} cx={t.x + TABLE_W - 14} cy={t.y + 15} />
    </g>
  )
}

function MeasureBox({ box, dimmed, active }: { box: MvBox; dimmed: boolean; active: boolean }) {
  const chip = `${box.measureCount} ${box.measureCount === 1 ? "measure" : "measures"} · governed`
  const rows = [...box.named]
  return (
    <g opacity={dimmed ? 0.4 : 1}>
      <rect
        x={box.x}
        y={box.y}
        width={box.w}
        height={box.h}
        rx="10"
        fill="var(--color-accent)"
        opacity={active ? 0.16 : 0.08}
        stroke="var(--color-accent)"
        strokeWidth={active ? "2" : "1.25"}
      />
      <text x={box.x + 16} y={box.y + 24} className="fill-[var(--text-primary)]" fontSize="13" fontWeight="700">
        {box.label}
      </text>
      {/* per-box "N measures · governed" chip */}
      <g>
        <rect x={box.x + box.w - 150} y={box.y + 11} width="134" height="18" rx="9" fill="var(--color-success)" opacity="0.16" />
        <circle cx={box.x + box.w - 138} cy={box.y + 20} r="3.5" fill="var(--color-success)" />
        <text x={box.x + box.w - 128} y={box.y + 24} className="fill-[var(--text-secondary)]" fontSize="9.5" fontWeight="600">
          {chip}
        </text>
      </g>
      {rows.map((m, i) => (
        <text key={m} x={box.x + 24} y={box.y + 50 + i * 22} className="fill-[var(--text-secondary)]" fontSize="11.5" fontFamily="monospace">
          • {m}
        </text>
      ))}
      {box.unnamed > 0 && (
        <text
          x={box.x + 24}
          y={box.y + 50 + rows.length * 22}
          className="fill-[var(--text-muted)]"
          fontSize="11"
          fontStyle="italic"
        >
          +{box.unnamed} unnamed
        </text>
      )}
    </g>
  )
}

// The Space-config (loose measures) box — a PEER of the MV boxes, ON the canvas
// in the metric-view column below them (v3 §3: "measures are grouped into boxes
// by owner on the right … plus a Space-config box for the loose measures"). It
// carries the same content the real box carries: the loose measures, the
// governance pill, an overlap warning on a name a governed MV already exposes,
// and a "+N unnamed" summary row.
function SpaceConfigNode() {
  const x = 620
  const y = 362
  const w = 336
  const h = 100
  return (
    <g>
      <rect
        x={x}
        y={y}
        width={w}
        height={h}
        rx="10"
        fill="var(--color-warning)"
        opacity="0.06"
        stroke="var(--color-warning)"
        strokeWidth="1.25"
        strokeDasharray="5 4"
      />
      <text x={x + 16} y={y + 22} className="fill-[var(--text-primary)]" fontSize="13" fontWeight="700">
        Space config
      </text>
      <text x={x + 16} y={y + 37} className="fill-[var(--text-muted)]" fontSize="9.5">
        loose measures · not in any MV
      </text>
      {/* governance pill */}
      <g>
        <rect x={x + w - 156} y={y + 10} width="140" height="18" rx="9" fill="var(--color-warning)" opacity="0.16" />
        <circle cx={x + w - 144} cy={y + 19} r="3.5" fill="var(--color-warning)" />
        <text x={x + w - 134} y={y + 23} className="fill-[var(--text-secondary)]" fontSize="9.5" fontWeight="600">
          2 measures · ungoverned
        </text>
      </g>
      {/* loose measure rows */}
      <text x={x + 24} y={y + 58} className="fill-[var(--text-secondary)]" fontSize="11.5" fontFamily="monospace">
        • avg_order_value
      </text>
      <text x={x + 24} y={y + 78} className="fill-[var(--text-secondary)]" fontSize="11.5" fontFamily="monospace">
        • avg_daily_rate
      </text>
      {/* overlap warning on avg_daily_rate — the name a governed MV already exposes */}
      <g>
        <title>name also exposed by Revenue MV — two definitions, one name</title>
        <path d={`M ${x + 150} ${y + 68} l 5 9 l -10 0 Z`} fill="var(--color-warning)" />
        <text x={x + 150} y={y + 77} textAnchor="middle" fontSize="7" fontWeight="800" className="fill-[var(--bg-sunken)]">
          !
        </text>
        <text x={x + 162} y={y + 78} className="fill-[var(--color-warning)]" fontSize="9.5" fontWeight="600">
          also in Revenue MV (≈ ADR)
        </text>
      </g>
      <text x={x + 24} y={y + 95} className="fill-[var(--text-muted)]" fontSize="11" fontStyle="italic">
        +8 unnamed
      </text>
    </g>
  )
}

function DeclaredArrow({ box, table, active }: { box: MvBox; table: TableCard; active: boolean }) {
  const to = rightEdge(table)
  const from = leftEdge(box, box.y + box.h / 2)
  const midX = (from.x + to.x) / 2
  const color = active ? "var(--color-accent)" : "var(--border-color-strong)"
  return (
    <g opacity={active ? 1 : 0.5}>
      <path
        d={`M ${from.x} ${from.y} C ${midX} ${from.y}, ${midX} ${to.y}, ${to.x + 2} ${to.y}`}
        fill="none"
        stroke={color}
        strokeWidth={active ? "1.75" : "1.25"}
        markerEnd={active ? "url(#v7-arrow-on)" : "url(#v7-arrow)"}
      />
      {/* N:1 glyph at rest (labels-on-demand: full `on` only on focus/hover) */}
      <text x={midX} y={(from.y + to.y) / 2 - 5} textAnchor="middle" fill={color} fontSize="9" fontWeight="600">
        N:1
      </text>
    </g>
  )
}

function CanvasSvg() {
  const wrapTables = SELECTED.uses.map((id) => TABLE_BY_ID.get(id)!).filter(Boolean)
  const minX = Math.min(...wrapTables.map((t) => t.x)) - 16
  const minY = Math.min(...wrapTables.map((t) => t.y)) - 26
  const maxX = Math.max(...wrapTables.map((t) => t.x + TABLE_W)) + 16
  const maxY = Math.max(...wrapTables.map((t) => t.y + TABLE_H)) + 16
  const wrapIds = new Set(SELECTED.uses)

  return (
    <svg viewBox="0 0 1000 470" className="w-full rounded-lg border border-default bg-sunken" role="img" aria-label="Semantic model — v7 contract, Revenue MV selected">
      <defs>
        <marker id="v7-arrow" markerWidth="9" markerHeight="9" refX="6" refY="3" orient="auto">
          <path d="M0,0 L6,3 L0,6 Z" className="fill-[var(--border-color-strong)]" />
        </marker>
        <marker id="v7-arrow-on" markerWidth="9" markerHeight="9" refX="6" refY="3" orient="auto">
          <path d="M0,0 L6,3 L0,6 Z" className="fill-[var(--color-accent)]" />
        </marker>
      </defs>

      {/* column headers */}
      {[
        { h: "Source tables", cx: 40 + TABLE_W / 2 },
        { h: "Joined tables", cx: 300 + TABLE_W / 2 },
        { h: "Metric views · measures", cx: 620 + 336 / 2 },
      ].map((c) => (
        <text key={c.h} x={c.cx} y={40} textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="10" fontWeight="700" letterSpacing="0.04em">
          {c.h}
        </text>
      ))}

      {/* select-time boundary: wraps EXACTLY Revenue's member tables (Euler-contiguous) */}
      <g>
        <rect
          x={minX}
          y={minY}
          width={maxX - minX}
          height={maxY - minY}
          rx="14"
          fill="var(--color-accent)"
          opacity="0.05"
          stroke="var(--color-accent)"
          strokeWidth="1.5"
          strokeDasharray="6 4"
        />
        <text x={minX + 12} y={minY + 16} className="fill-[var(--color-accent)]" fontSize="10" fontWeight="700">
          Tables used by Revenue MV
        </text>
      </g>

      {/* unmodeled region — neutral, NO arrows (constraint 2) */}
      <g>
        <rect x={284} y={352} width={TABLE_W + 32} height={TABLE_H + 34} rx="12" fill="var(--text-muted)" opacity="0.06" stroke="var(--border-color-strong)" strokeDasharray="3 4" />
        <text x={300} y={368} className="fill-[var(--text-muted)]" fontSize="9.5" fontWeight="700" letterSpacing="0.04em">
          UNMODELED · no MV
        </text>
      </g>

      {/* declared arrows (Revenue = active/highlighted; Margin = dimmed at rest) */}
      {REVENUE.uses.map((id) => (
        <DeclaredArrow key={`rev-${id}`} box={REVENUE} table={TABLE_BY_ID.get(id)!} active />
      ))}
      {MARGIN.uses.map((id) => (
        <DeclaredArrow key={`mar-${id}`} box={MARGIN} table={TABLE_BY_ID.get(id)!} active={false} />
      ))}

      {/* tables (dedup — each once) */}
      {TABLES.map((t) => (
        <TableNode key={t.id} t={t} dimmed={t.coverage === "gap"} inWrap={wrapIds.has(t.id)} />
      ))}

      {/* measure boxes */}
      <MeasureBox box={REVENUE} dimmed={false} active />
      <MeasureBox box={MARGIN} dimmed active={false} />

      {/* Space-config (loose measures) box — a peer of the MV boxes, below them
          in the metric-view column (v3 §3), NOT a panel under the canvas. */}
      <SpaceConfigNode />
    </svg>
  )
}

// ── Control row (visible, per v7): search · Lineage/Impact · Fit · − % + · Reset
function ControlRow() {
  return (
    <div className="flex flex-wrap items-center gap-2">
      <div className="flex items-center gap-1.5 rounded-md border border-default bg-elevated px-2 py-1 text-xs text-muted">
        <Search className="h-3.5 w-3.5" />
        <span>Search tables, measures…</span>
      </div>
      <div className="inline-flex overflow-hidden rounded-md border border-default text-xs">
        <span className="bg-accent px-2.5 py-1 font-medium text-white">Lineage</span>
        <span className="px-2.5 py-1 text-secondary">Impact</span>
      </div>
      <span className="inline-flex items-center gap-1 rounded-md border border-default bg-elevated px-2 py-1 text-xs text-secondary">
        <Maximize2 className="h-3.5 w-3.5" /> Fit
      </span>
      <span className="inline-flex items-center gap-1 rounded-md border border-default bg-elevated px-1 py-1 text-xs text-secondary">
        <Minus className="h-3.5 w-3.5" />
        <span className="px-1 tabular-nums">100%</span>
        <Plus className="h-3.5 w-3.5" />
      </span>
      <span className="inline-flex items-center gap-1 rounded-md border border-default bg-elevated px-2 py-1 text-xs text-secondary">
        <RotateCcw className="h-3.5 w-3.5" /> Reset
      </span>
    </div>
  )
}

function Legend() {
  const items: { swatch: ReactNode; label: string }[] = [
    { swatch: <span className="inline-block h-2.5 w-3.5 rounded-sm border border-[var(--border-color-strong)] bg-[var(--bg-surface)]" />, label: "table (fact / dim)" },
    { swatch: <span className="inline-block h-2.5 w-3.5 rounded-sm border border-[var(--color-accent)] bg-[color-mix(in_srgb,var(--color-accent)_16%,transparent)]" />, label: "metric view (measures)" },
    { swatch: <span className="inline-block h-2.5 w-2.5 rounded-full bg-[var(--color-success)]" />, label: "governed / in an MV" },
    { swatch: <span className="inline-block h-2.5 w-2.5 rounded-full bg-[var(--color-danger)]" />, label: "unmodeled (no MV)" },
    { swatch: <span className="text-[var(--color-accent)]">→</span>, label: "declared join (N:1)" },
  ]
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted">
      {items.map((it, i) => (
        <span key={i} className="inline-flex items-center gap-1.5">
          {it.swatch}
          {it.label}
        </span>
      ))}
    </div>
  )
}

// ── Curator inset — the structured internals the graph can't carry (v3 §4) ───
function CuratorInset() {
  return (
    <div className="grid gap-3 sm:grid-cols-2">
      <div className="space-y-2 rounded-lg border border-default bg-elevated p-3">
        <div className="flex items-center justify-between">
          <span className="font-mono text-sm font-medium text-primary">Revenue MV</span>
          <Badge variant="success">
            <ShieldCheck className="mr-1 h-3 w-3" />
            Governed
          </Badge>
        </div>
        {/* join tree from YAML, indented for snowflake */}
        <div className="space-y-0.5 text-xs text-muted">
          <p className="text-secondary">Join tree (from YAML)</p>
          <pre className="overflow-x-auto whitespace-pre font-mono text-[11px] leading-5 text-secondary">{`orders  (source · fact)
├─ order_items  · N:1
│    on orders.order_id = items.order_id
└─ customer     · N:1
│    on orders.customer_id = customer.id
   └─ nation    · N:1  (snowflake)
        on customer.nation_id = nation.id`}</pre>
        </div>
        <div className="text-xs text-muted">
          <p className="text-secondary">Dimensions by binding</p>
          <p>from source: <span className="font-mono">order_date</span> · from join: <span className="font-mono">customer.name</span> · expression: <span className="font-mono">DATE_TRUNC(month, order_date)</span></p>
        </div>
      </div>

      <div className="space-y-2 rounded-lg border border-default bg-elevated p-3">
        <p className="text-xs text-secondary">Governance roll-up</p>
        <div className="flex flex-wrap gap-1.5">
          <Badge variant="success"><ShieldCheck className="mr-1 h-3 w-3" />Governed · 5</Badge>
          <Badge variant="warning"><Wrench className="mr-1 h-3 w-3" />Curated · 1</Badge>
          <Badge variant="danger"><AlertTriangle className="mr-1 h-3 w-3" />Ungoverned · 8</Badge>
        </div>
        <dl className="space-y-1 text-xs text-muted">
          <div className="flex gap-2"><dt className="text-secondary">filter</dt><dd className="font-mono">orders.status = 'COMPLETE'</dd></div>
          <div className="flex gap-2"><dt className="text-secondary">materialization</dt><dd>none (virtual)</dd></div>
          <div className="flex gap-2"><dt className="text-secondary">reuse</dt><dd><span className="font-mono">orders</span> used by 2 metric views — edits ripple</dd></div>
        </dl>
        <p className="flex items-center gap-1.5 rounded-md border border-[var(--color-warning)] bg-[color-mix(in_srgb,var(--color-warning)_10%,transparent)] px-2 py-1 text-xs text-secondary">
          <AlertTriangle className="h-3.5 w-3.5 text-[var(--color-warning)]" />
          Space-config <span className="font-mono">avg_daily_rate</span> overlaps a governed measure (≈ ADR).
        </p>
      </div>
    </div>
  )
}

export function ModelV7ContractFrame() {
  return (
    <div className="space-y-3 rounded-xl border border-default bg-surface p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="flex items-center gap-2 text-sm font-semibold uppercase tracking-wide text-secondary">
          <Layers className="h-4 w-4" /> Semantic model
        </h3>
        <span className="inline-flex items-center gap-1 text-xs text-muted">
          <Crosshair className="h-3.5 w-3.5" /> Revenue MV selected
        </span>
      </div>
      <ControlRow />
      <CanvasSvg />
      <Legend />
      <CuratorInset />
      <p className="text-xs text-muted">
        Tip: click a metric view to wrap the tables in its definition · drag any box to spread the canvas · Reset restores the layout.
      </p>
    </div>
  )
}
