/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * Frame 6: one representative, STATIC frame of the semantic-model visualization
 * (D3). The live force-directed graph is Prompt 12's job with react-force-graph-2d
 * (needs canvas/DOM and cannot render to static markup); this frame exists only
 * to review the node/edge vocabulary and the node-detail panel. Prompt 13 deletes it.
 */
import { Badge } from "@/components/ui/badge"
import { proposalRevenue } from "./mvMockData"

export function SemanticModelFrame() {
  return (
    <div className="rounded-xl border border-default bg-surface p-4">
      <div className="mb-3 flex items-center justify-between">
        <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">Semantic model</h3>
        <span className="text-xs text-muted">Static preview · live graph ships in Prompt 12</span>
      </div>

      <div className="grid gap-4 lg:grid-cols-[1.6fr_1fr]">
        {/* Static node/edge diagram */}
        <svg viewBox="0 0 520 300" className="w-full rounded-lg border border-default bg-sunken" role="img" aria-label="Semantic model preview">
          <defs>
            <marker id="arrow" markerWidth="8" markerHeight="8" refX="6" refY="3" orient="auto">
              <path d="M0,0 L6,3 L0,6 Z" className="fill-[var(--text-muted)]" />
            </marker>
          </defs>

          {/* edges */}
          <line x1="120" y1="90" x2="250" y2="150" stroke="var(--border-color-strong)" strokeWidth="1.5" markerEnd="url(#arrow)" />
          <line x1="120" y1="210" x2="250" y2="150" stroke="var(--border-color-strong)" strokeWidth="1.5" markerEnd="url(#arrow)" />
          <text x="150" y="205" className="fill-[var(--text-muted)]" fontSize="9">ON orders.order_id = items.order_id</text>
          {/* measure/dimension membership */}
          <line x1="370" y1="90" x2="320" y2="140" stroke="var(--color-accent)" strokeWidth="1.5" markerEnd="url(#arrow)" />
          <line x1="370" y1="210" x2="320" y2="160" stroke="var(--color-accent)" strokeWidth="1.5" markerEnd="url(#arrow)" />
          {/* "replaces" dashed edge (tables_freed) */}
          <line x1="250" y1="150" x2="120" y2="150" stroke="var(--color-danger)" strokeWidth="1.5" strokeDasharray="4 3" />
          <text x="128" y="143" className="fill-[var(--color-danger)]" fontSize="9">replaces</text>

          {/* source table */}
          <g>
            <rect x="40" y="72" width="80" height="34" rx="6" fill="var(--bg-surface)" stroke="var(--border-color-strong)" />
            <text x="80" y="93" textAnchor="middle" className="fill-[var(--text-secondary)]" fontSize="10">orders</text>
          </g>
          {/* join table */}
          <g>
            <rect x="40" y="192" width="80" height="34" rx="6" fill="var(--bg-surface)" stroke="var(--border-color-strong)" />
            <text x="80" y="213" textAnchor="middle" className="fill-[var(--text-secondary)]" fontSize="10">order_items</text>
          </g>
          {/* proposed MV (distinct styling) */}
          <g>
            <rect x="248" y="128" width="96" height="44" rx="8" fill="var(--color-accent)" opacity="0.15" stroke="var(--color-accent)" strokeWidth="1.5" />
            <text x="296" y="146" textAnchor="middle" className="fill-[var(--text-primary)]" fontSize="10" fontWeight="600">order_revenue</text>
            <text x="296" y="160" textAnchor="middle" className="fill-[var(--text-muted)]" fontSize="8">metric view</text>
          </g>
          {/* measure */}
          <g>
            <rect x="368" y="72" width="112" height="34" rx="6" fill="var(--bg-surface)" stroke="var(--color-accent)" />
            <text x="424" y="93" textAnchor="middle" className="fill-[var(--text-secondary)]" fontSize="10">total_revenue (measure)</text>
          </g>
          {/* dimension */}
          <g>
            <rect x="368" y="192" width="112" height="34" rx="6" fill="var(--bg-surface)" stroke="var(--border-color-strong)" />
            <text x="424" y="213" textAnchor="middle" className="fill-[var(--text-secondary)]" fontSize="10">order_date (dim)</text>
          </g>
        </svg>

        {/* Node detail panel (on click, in the live graph) */}
        <div className="space-y-2 rounded-lg border border-default bg-elevated p-3">
          <div className="flex items-center justify-between">
            <span className="text-sm font-medium text-primary">total_revenue</span>
            <Badge variant="secondary">measure</Badge>
          </div>
          <dl className="space-y-1 text-xs text-muted">
            <div className="flex gap-2"><dt className="text-secondary">expr</dt><dd className="font-mono">SUM(items.quantity * items.unit_price)</dd></div>
            <div className="flex gap-2"><dt className="text-secondary">format</dt><dd>number</dd></div>
            <div className="flex gap-2"><dt className="text-secondary">synonyms</dt><dd>revenue, sales</dd></div>
            <div className="flex gap-2"><dt className="text-secondary">recurrence</dt><dd>{proposalRevenue.evidence?.recurrence_count} occurrences</dd></div>
            <div className="flex gap-2"><dt className="text-secondary">questions</dt><dd className="font-mono">{proposalRevenue.evidence?.benchmark_question_ids.join(", ")}</dd></div>
          </dl>
        </div>
      </div>
    </div>
  )
}
