/**
 * MV-advisor mockups — Semantic Blueprint (v4) P1 FIDELITY FRAMES, REVIEW
 * SCAFFOLD (see mvMockData.ts).
 *
 * The visual contract for Phase 1 of the v4 "Semantic Blueprint" rebuild
 * (docs/design/semantic-graph-v4-blueprint-note.md §5, §11.3), rendered against
 * the north-star prototype (docs/design/mockups/10-blueprint-prototype.html)
 * via the pure reference math in blueprintMath.ts. Each frame is a STATIC
 * capture of one prototype state — scenario × zoom band × selection — so the
 * gate (mockups.test.tsx) can assert the P1 vocabulary renders:
 *
 *   11a star · Standard      — fact-center layout, crow's-foot joins, crossing
 *                              hops, unmodeled region, island tag, cold-spot
 *                              callout, health headline, legend, toolbar
 *   11b star · Columns LOD   — single COL band, join-key row highlights,
 *                              column-accurate ports (ON leaf names, §5.5/§6)
 *   11c star · measure trace — dashed lineage to source tables via gutters +
 *                              the lane above the cards (§5.10) + detail inset
 *   11d star · MV selected   — member boundary, dotted uses-lineage, MV inset
 *   11e unknown roles        — neutral TABLE captions + connectivity headers
 *   11f single wide table    — no joins is a valid model; adaptive rank-x
 *   11g 30-table snowflake   — density: bridges + barycenter-stable stacking
 *   11h star · Overview      — far band: headers + cards only, no chips
 *
 * Pure render of fixture + fixed state — no effects, no layout library (§2).
 * Disposed when the production SemanticBlueprint.tsx reaches parity.
 */
import type { ReactNode } from "react"
import {
  blueprintStar,
  blueprintUnknown,
  blueprintWide,
  makeBlueprintScale,
  type BlueprintJoinFixture,
  type BlueprintMvFixture,
  type BlueprintScenarioFixture,
  type BlueprintTableFixture,
} from "./mvMockData"
import {
  COL_H,
  COL_TOP,
  derivePlacement,
  govColor,
  headlineCounts,
  layoutBoxes,
  lineagePaths,
  measureIndex,
  neighbourhood,
  nodeById,
  nodeWidth,
  onStr,
  rankLabel,
  resolveEdges,
  routePath,
  type BlueprintLayoutMode,
  type BlueprintZoom,
} from "./blueprintMath"

interface FrameState {
  scenario: BlueprintScenarioFixture
  zoom: BlueprintZoom
  selected: string | null
  layoutMode: BlueprintLayoutMode
}

// ── Toolbar chrome (static): zoom bands · layout toggle · Reset view ─────────
function Seg({ options, on }: { options: string[]; on: string }) {
  return (
    <span className="inline-flex overflow-hidden rounded-md border border-default text-xs">
      {options.map((o) => (
        <span key={o} className={o === on ? "bg-accent px-2.5 py-1 font-medium text-white" : "px-2.5 py-1 text-secondary"}>
          {o}
        </span>
      ))}
    </span>
  )
}

function Toolbar({ zoom, layoutMode }: { zoom: BlueprintZoom; layoutMode: BlueprintLayoutMode }) {
  const zoomLabel = zoom === "far" ? "Overview" : zoom === "mid" ? "Standard" : "Columns"
  return (
    <div className="flex flex-wrap items-center gap-2">
      <Seg options={["Overview", "Standard", "Columns"]} on={zoomLabel} />
      <Seg options={["Fact-center", "Source-left"]} on={layoutMode === "fact" ? "Fact-center" : "Source-left"} />
      <span className="inline-flex items-center rounded-md border border-default bg-elevated px-2 py-1 text-xs text-secondary">
        Reset view
      </span>
    </div>
  )
}

// ── Health headline (§5.7) ────────────────────────────────────────────────────
function Headline({ scenario }: { scenario: BlueprintScenarioFixture }) {
  const c = headlineCounts(scenario)
  const pill = (color: string, n: number) => (
    <span className="inline-flex items-center gap-1 rounded-full border border-default px-2 text-[11px] font-semibold">
      <span className="inline-block h-2 w-2 rounded-full" style={{ background: color }} />
      {n}
    </span>
  )
  return (
    <p className="text-[13px] text-secondary" data-headline>
      <b className="text-primary">{c.governed}</b> governed · <b className="text-primary">{c.curated}</b> curated ·{" "}
      <b className="text-primary">{c.ungoverned}</b> ungoverned{" "}
      <span className="inline-flex gap-1.5 align-middle">
        {pill("var(--color-success)", c.governed)}
        {pill("var(--color-warning)", c.curated)}
        {pill("var(--color-danger)", c.ungoverned)}
      </span>{" "}
      — <b className="text-primary">{c.unmodeled}</b> tables in no metric view · <b className="text-primary">{c.cold}</b> cold spot
    </p>
  )
}

// ── SVG canvas ────────────────────────────────────────────────────────────────
function BlueprintCanvas({ scenario, zoom, selected, layoutMode }: FrameState) {
  const s = scenario
  const byId = nodeById(s)
  const placement = derivePlacement(s, layoutMode)
  const box = layoutBoxes(s, placement, zoom)
  const keep = neighbourhood(s, selected)
  const resolved = resolveEdges(s, placement, box, zoom)
  const chipPos: Record<string, { x: number; y: number }> = {}
  const parts: ReactNode[] = []

  // rank headers (occupied ranks only)
  const occupied = [...new Set(s.nodes.map((n) => placement.rank[n.id]))].sort((a, b) => a - b)
  occupied.forEach((r) => {
    const anyNode = s.nodes.find((n) => placement.rank[n.id] === r) as (typeof s.nodes)[number]
    parts.push(
      <text
        key={`hdr-${r}`}
        x={box[anyNode.id].x + nodeWidth(anyNode) / 2}
        y={20}
        textAnchor="middle"
        fill="var(--text-muted)"
        fontSize={10}
        fontWeight={700}
        letterSpacing=".05em"
      >
        {rankLabel(s, placement, r).toUpperCase()}
      </text>,
    )
  })

  // unmodeled region (self-annotation, §5.6)
  const un = s.nodes.filter((n) => n.kind === "table" && n.unmodeled).map((n) => box[n.id])
  if (un.length) {
    const x = Math.min(...un.map((b) => b.x)) - 8
    const y = Math.min(...un.map((b) => b.y)) - 8
    const x2 = Math.max(...un.map((b) => b.x + b.w)) + 8
    const y2 = Math.max(...un.map((b) => b.y + b.h)) + 8
    parts.push(
      <g key="unmodeled" data-region="unmodeled">
        <rect x={x} y={y} width={x2 - x} height={y2 - y} rx={12} fill="var(--color-danger)" fillOpacity={0.05}
          stroke="var(--color-danger)" strokeWidth={1.25} strokeDasharray="5 4" />
        <text x={x + 8} y={y - 4} fill="var(--color-danger)" fontSize={9.5} fontWeight={700}>
          UNMODELED · in no metric view
        </text>
      </g>,
    )
  }

  // select-time MV member boundary
  if (selected && byId[selected]?.kind === "mv") {
    const mem = (s.uses[selected] ?? []).map((t) => box[t]).filter(Boolean)
    mem.forEach((b, i) =>
      parts.push(
        <rect key={`bound-${i}`} data-boundary="mv-member" x={b.x - 7} y={b.y - 7} width={b.w + 14} height={b.h + 14}
          rx={12} fill="var(--color-accent)" fillOpacity={0.09} stroke="var(--color-accent)" strokeWidth={2}
          strokeDasharray="7 4" />,
      ),
    )
  }

  // declared joins (§5.2–§5.4) — arrows require proof: only s.joins are drawn.
  resolved.forEach((e, idx) => {
    const d = routePath(e.sx, e.sy, e.dx, e.dy, e.midX, e.hops)
    const active = !!keep && keep.has(e.from) && keep.has(e.to)
    const dim = !!keep && !active
    const stroke = active ? "var(--color-accent)" : "var(--text-muted)"
    const baseOp = active ? 1 : 0.85
    const manyX = e.manyOnLeft ? e.sx : e.dx
    const manyY = e.manyOnLeft ? e.sy : e.dy
    const footApex = e.manyOnLeft ? manyX + 12 : manyX - 12
    const oneX = e.manyOnLeft ? e.dx : e.sx
    const oneY = e.manyOnLeft ? e.dy : e.sy
    const oneTick = e.manyOnLeft ? oneX - 6 : oneX + 6
    const onLabel = `${e.fromCol} = ${e.toCol}`
    const lw = onLabel.length * 5.4 + 12
    parts.push(
      <g key={`edge-${idx}`}>
        {active && (
          <path d={d} fill="none" stroke="var(--color-accent)" strokeWidth={6} strokeLinecap="round" opacity={0.16} />
        )}
        <path d={d} fill="none" stroke={stroke} strokeWidth={active ? 2.1 : 1.5} opacity={dim ? 0.1 : baseOp}
          data-edge="join" data-edge-from={e.from} data-edge-to={e.to} data-hops={e.hops.length} />
        <path
          d={`M ${footApex} ${manyY} L ${manyX} ${manyY - 6} M ${footApex} ${manyY} L ${manyX} ${manyY} M ${footApex} ${manyY} L ${manyX} ${manyY + 6}`}
          fill="none" stroke={stroke} strokeWidth={1.4} opacity={dim ? 0.1 : baseOp} data-glyph="crowfoot"
        />
        <path d={`M ${oneTick} ${oneY - 5} L ${oneTick} ${oneY + 5}`} fill="none" stroke={stroke} strokeWidth={1.4}
          opacity={dim ? 0.1 : baseOp} data-glyph="one-tick" />
        {active && !dim && (
          <g>
            <rect x={e.midX - lw / 2} y={(e.sy + e.dy) / 2 - 9} width={lw} height={15} rx={4} fill="var(--bg-sunken)"
              stroke="var(--border-color)" strokeWidth={0.75} />
            <text x={e.midX} y={(e.sy + e.dy) / 2 + 2} textAnchor="middle" fontSize={8.5} fontFamily="var(--font-mono)"
              fill="var(--text-secondary)">
              {onLabel}
            </text>
          </g>
        )}
      </g>,
    )
  })

  // nodes
  s.nodes.forEach((n) => {
    const b = box[n.id]
    const dimmed = !!keep && !keep.has(n.id)
    if (n.kind === "table") {
      const t = n as BlueprintTableFixture
      const sel = selected === n.id
      const wide = !!t.columnCount && t.columnCount > 30
      const cold = t.coverage === 0
      parts.push(
        <g key={n.id} opacity={dimmed ? 0.4 : 1} data-node="table" data-node-id={n.id}>
          <rect x={b.x} y={b.y} width={b.w} height={b.h} rx={9} fill="var(--bg-surface)"
            stroke={sel ? "var(--color-accent)" : t.island ? "var(--color-warning)" : "var(--border-color-strong)"}
            strokeWidth={sel ? 2 : 1.4} strokeDasharray={t.cold || t.island ? "4 3" : undefined} />
          {wide && zoom !== "far" && (
            <g data-pill="wide">
              <rect x={b.x + b.w - 72} y={b.y + 6} width={46} height={15} rx={7} fill="var(--color-warning)"
                fillOpacity={0.16} stroke="var(--color-warning)" strokeWidth={0.75} />
              <text x={b.x + b.w - 49} y={b.y + 16} textAnchor="middle" fontSize={8} fontWeight={700}
                fill="var(--color-warning)">
                {t.columnCount} cols
              </text>
            </g>
          )}
          {t.island && zoom !== "far" && (
            <text x={b.x + b.w - 12} y={b.y + b.h - 6} textAnchor="end" fontSize={8} fontWeight={700}
              fill="var(--color-warning)" data-tag="island">
              no join
            </text>
          )}
          {zoom !== "far" && (
            <text x={b.x + 12} y={b.y + 17} fill="var(--text-muted)" fontSize={8.5} fontWeight={700}
              letterSpacing=".06em" data-caption="role">
              {t.role ?? "TABLE"}
            </text>
          )}
          <text x={b.x + 12} y={zoom === "far" ? b.y + 22 : zoom === "near" ? b.y + 34 : b.y + 37}
            fill="var(--text-primary)" fontSize={12.5} fontWeight={600} fontFamily="var(--font-mono)">
            {n.id}
          </text>
          <circle cx={b.x + b.w - 14} cy={b.y + 14} r={8} fill={cold ? "var(--bg-surface)" : "var(--text-muted)"}
            opacity={cold ? 1 : 0.5} stroke={cold ? "var(--color-danger)" : "var(--border-color-strong)"}
            strokeWidth={1} strokeDasharray={cold ? "2 2" : undefined} />
          <text x={b.x + b.w - 14} y={b.y + 17} textAnchor="middle" fontSize={8} fontWeight={700}
            fill={cold ? "var(--color-danger)" : "var(--bg-surface)"}>
            {t.coverage}
          </text>
          {zoom === "near" && (
            <g>
              <line x1={b.x + 10} y1={b.y + COL_TOP - 6} x2={b.x + b.w - 10} y2={b.y + COL_TOP - 6}
                stroke="var(--border-color)" strokeWidth={1} opacity={0.6} />
              {t.cols.map((c, i) => {
                const cy = b.y + COL_TOP + i * COL_H
                const isKey = s.joins.some(
                  (j) => (j.from === n.id && j.fromCol === c) || (j.to === n.id && j.toCol === c),
                )
                return (
                  <g key={c}>
                    {isKey && (
                      <rect x={b.x + 8} y={cy + 1} width={b.w - 16} height={COL_H - 2} rx={4}
                        fill="var(--accent-cur, var(--color-accent))" fillOpacity={0.12} data-joinkey={c} />
                    )}
                    <text x={b.x + 14} y={cy + COL_H / 2 + 3.5} fontSize={9.5} fontFamily="var(--font-mono)"
                      fill={isKey ? "var(--accent-cur, var(--color-accent))" : "var(--text-muted)"}
                      fontWeight={isKey ? 700 : 400}>
                      {c}
                    </text>
                  </g>
                )
              })}
            </g>
          )}
        </g>,
      )
    } else {
      const mv = n as BlueprintMvFixture
      const isMv = mv.kind === "mv"
      const sel = selected === n.id
      parts.push(
        <g key={n.id} opacity={dimmed ? 0.4 : 1} data-node={mv.kind} data-node-id={n.id}>
          <rect x={b.x} y={b.y} width={b.w} height={b.h} rx={10}
            fill={isMv ? "var(--color-accent)" : "var(--color-warning)"} fillOpacity={isMv ? 0.07 : 0.08}
            stroke={sel ? "var(--color-accent)" : isMv ? "var(--border-color-strong)" : "var(--color-warning)"}
            strokeWidth={sel ? 2 : 1.5} strokeDasharray={isMv ? undefined : "5 3"} />
          <text x={b.x + 12} y={b.y + 22} fill="var(--text-primary)" fontSize={12.5} fontWeight={700}>
            {isMv ? n.id : "Space config"}
          </text>
          <text x={b.x + 12} y={b.y + 38} fill="var(--text-muted)" fontSize={9}>
            {isMv ? `metric view · ${mv.measures.length} measures` : "not in any metric view"}
          </text>
          {zoom !== "far" &&
            mv.measures.map((m, i) => {
              const mid = `${n.id}::${m.name}`
              const my = b.y + 44 + i * 22
              const selM = selected === mid
              chipPos[mid] = { x: b.x + 8, y: my + 9 }
              return (
                <g key={mid} data-chip="measure" data-chip-id={mid}>
                  <rect x={b.x + 8} y={my} width={b.w - 16} height={18} rx={4} fill={govColor(m.gov)}
                    fillOpacity={selM ? 0.3 : 0.14}
                    stroke={selM ? "var(--color-accent)" : m.overlaps ? "var(--color-warning)" : govColor(m.gov)}
                    strokeWidth={selM ? 2 : 1} />
                  <text x={b.x + 14} y={my + 13} fontSize={9.5} fontWeight={600} fill="var(--text-primary)">
                    {m.name}
                  </text>
                  {m.overlaps && (
                    <g data-marker="overlap">
                      <path d={`M ${b.x + b.w - 20} ${my + 4} l 5 9 l -10 0 Z`} fill="var(--color-warning)" />
                      <text x={b.x + b.w - 20} y={my + 13} textAnchor="middle" fontSize={7} fontWeight={800}
                        fill="var(--bg-surface)">
                        !
                      </text>
                    </g>
                  )}
                </g>
              )
            })}
        </g>,
      )
    }
  })

  // lineage on select (§5.10) — dashed for measures, dotted for MV sources.
  if (selected) {
    for (const lp of lineagePaths(s, placement, box, resolved, selected, chipPos)) {
      const dash = lp.mode === "mv" ? "0.1 6" : "5 4"
      parts.push(
        <g key={`lin-${lp.srcId}`}>
          <path d={lp.d} fill="none" stroke="var(--color-accent)" strokeWidth={6} strokeLinecap="round"
            strokeLinejoin="round" opacity={0.12} />
          <path d={lp.d} fill="none" stroke="var(--color-accent)" strokeWidth={lp.mode === "mv" ? 2.25 : 1.9}
            strokeLinecap="round" strokeLinejoin="round" strokeDasharray={dash} data-lineage={lp.mode}
            data-lineage-src={lp.srcId} />
          <circle cx={lp.sx} cy={lp.sy} r={3.5} fill="var(--color-accent)" />
        </g>,
      )
    }
  }

  // cold-spot callout (§5.6) — anchored to the worst cold spot, when flagged.
  const coldSpot = s.nodes.find((n): n is BlueprintTableFixture => n.kind === "table" && !!n.cold)
  if (coldSpot && zoom !== "far") {
    const hb = box[coldSpot.id]
    const ax = hb.x
    const ay = hb.y + hb.h
    const bx = hb.x - 4
    const by = ay + 24
    parts.push(
      <g key="callout-cold" data-callout="cold-spot">
        <path d={`M ${ax + 20} ${ay} L ${bx + 20} ${by}`} stroke="var(--color-danger)" strokeWidth={1}
          strokeDasharray="3 2" fill="none" />
        <rect x={bx} y={by} width={178} height={32} rx={6} fill="var(--bg-surface)" stroke="var(--color-danger)"
          strokeWidth={1} />
        <text x={bx + 10} y={by + 14} fontSize={10} fontWeight={700} fill="var(--color-danger)">
          Cold spot · {coldSpot.id}
        </text>
        <text x={bx + 10} y={by + 26} fontSize={9} fill="var(--text-muted)">
          no curated SQL touches it
        </text>
      </g>,
    )
  }

  const maxX = Math.max(...s.nodes.map((n) => box[n.id].x + box[n.id].w)) + 40
  const maxY = Math.max(...s.nodes.map((n) => box[n.id].y + box[n.id].h)) + 96
  return (
    <svg viewBox={`0 0 ${Math.max(720, maxX)} ${Math.max(360, maxY)}`} className="w-full rounded-lg border border-default bg-sunken"
      role="img" aria-label="Semantic model blueprint">
      {parts}
    </svg>
  )
}

// ── Legend (prototype parity) ─────────────────────────────────────────────────
function Legend() {
  return (
    <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted">
      <span className="inline-flex items-center gap-1.5">
        <span className="inline-block h-2.5 w-3.5 rounded-sm border border-[var(--border-color-strong)] bg-[var(--bg-surface)]" />
        table
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span className="inline-block h-2.5 w-3.5 rounded-sm border border-[var(--color-accent)] bg-[color-mix(in_srgb,var(--color-accent)_10%,transparent)]" />
        metric view
      </span>
      <span className="inline-flex items-center gap-1.5">
        <svg width={30} height={12}>
          <line x1={2} y1={6} x2={26} y2={6} stroke="var(--border-color-strong)" strokeWidth={1.75} />
          <path d="M4 2 L2 6 L4 10 M8 2 L2 6 L8 10" fill="none" stroke="var(--border-color-strong)" strokeWidth={1.25} />
        </svg>
        declared join · crow&apos;s-foot cardinality
      </span>
      <span className="inline-flex items-center gap-1.5">
        <span className="inline-block h-2.5 w-3.5 rounded-sm border border-dashed border-[var(--color-danger)]" />
        unmodeled / cold spot
      </span>
    </div>
  )
}

// ── Detail inset (mirrors NodeDetail — table / MV / measure / Space config) ──
function InsetSection({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div>
      <p className="mb-1 text-[10px] font-semibold uppercase tracking-wider text-muted">{title}</p>
      {children}
    </div>
  )
}

function Chip({ children }: { children: ReactNode }) {
  return (
    <span className="mb-1 mr-1 inline-block rounded border border-default px-1.5 font-mono text-[11px] text-secondary">
      {children}
    </span>
  )
}

function DetailInset({ scenario, selected }: { scenario: BlueprintScenarioFixture; selected: string | null }) {
  if (!selected) return null
  const s = scenario
  const byId = nodeById(s)
  const measures = measureIndex(s)
  const m = measures[selected]

  let head: ReactNode
  let warn: ReactNode = null
  let body: ReactNode

  if (m) {
    const gl = m.gov === "governed" ? "Governed" : m.gov === "curated" ? "Curated" : "Ungoverned"
    head = (
      <>
        <span className="font-mono text-[13px] font-semibold text-primary">{m.name}</span>
        <span className="rounded-full border border-default px-1.5 text-[10.5px] text-muted">
          measure · {m.parentKind === "mv" ? "metric view" : "space config"}
        </span>
        <span className="inline-flex items-center gap-1 rounded-full border border-default px-2 text-[11px] font-semibold">
          <span className="inline-block h-2 w-2 rounded-full" style={{ background: govColor(m.gov) }} />
          {gl}
        </span>
      </>
    )
    if (m.overlaps) {
      warn = (
        <div className="border-b border-default px-3 py-2 text-[11.5px] text-[var(--color-warning)]">
          <b>Name collision</b> — <span className="font-mono">{m.overlaps}</span> already exposes a measure named{" "}
          <span className="font-mono">{m.name}</span>; two definitions, one name.
        </div>
      )
    }
    body = (
      <>
        <InsetSection title="Definition">
          <code className="block rounded bg-sunken px-2 py-1 font-mono text-[11px] text-secondary">{m.expr}</code>
        </InsetSection>
        <InsetSection title="Lineage → source tables">
          {m.src.map((t) => (
            <div key={t} className="mb-0.5 flex items-center gap-1.5 font-mono text-[11px] text-secondary">
              <span className="text-[var(--color-accent-light,var(--color-accent))]">└</span> {t}
            </div>
          ))}
          <p className="mt-1 text-[11px] text-muted">
            exposed by <b className="text-secondary">{m.parent}</b>
          </p>
        </InsetSection>
      </>
    )
  } else {
    const n = byId[selected]
    if (!n) return null
    if (n.kind === "table") {
      const t = n as BlueprintTableFixture
      const usedBy = Object.keys(s.uses).filter((mv) => s.uses[mv].includes(n.id))
      const joins = s.joins.filter((j) => j.from === n.id || j.to === n.id)
      head = (
        <>
          <span className="font-mono text-[13px] font-semibold text-primary">{n.id}</span>
          <span className="rounded-full border border-default px-1.5 text-[10.5px] text-muted">{t.role ?? "TABLE"} table</span>
        </>
      )
      body = (
        <>
          <InsetSection title="Coverage">
            {t.coverage === 0 ? (
              <p className="text-[11px] text-[var(--color-danger)]">
                <b>cold spot</b> — no curated SQL touches it
              </p>
            ) : (
              <p className="text-[11px] text-muted">
                <b className="text-secondary">{t.coverage}</b> curated statement{t.coverage > 1 ? "s" : ""} touch this table
              </p>
            )}
          </InsetSection>
          <InsetSection title="Columns">
            {t.cols.map((c) => (
              <Chip key={c}>{c}</Chip>
            ))}
          </InsetSection>
          <InsetSection title="Used by metric views">
            {usedBy.length ? usedBy.map((u) => <Chip key={u}>{u}</Chip>) : <p className="text-[11px] text-muted">none — this table is unmodeled</p>}
          </InsetSection>
          {joins.length > 0 && (
            <InsetSection title={`Declared joins (${joins.length})`}>
              {joins.map((j: BlueprintJoinFixture, i) => (
                <div key={i} className="mb-1">
                  <div className="flex items-center gap-1.5 font-mono text-[11px] text-secondary">
                    {j.from} <span className="text-[var(--color-accent-light,var(--color-accent))]">→</span> {j.to}
                    <Chip>{j.rel}</Chip>
                  </div>
                  <div className="font-mono text-[11px] text-muted">ON {onStr(j)}</div>
                </div>
              ))}
            </InsetSection>
          )}
        </>
      )
    } else if (n.kind === "mv") {
      const mv = n as BlueprintMvFixture
      const srcSet = s.uses[n.id] ?? []
      const joins = s.joins.filter((j) => srcSet.includes(j.from) && srcSet.includes(j.to))
      const targets = new Set(joins.map((j) => j.to))
      const root = joins.map((j) => j.from).find((f) => !targets.has(f)) ?? srcSet[0]
      const gN = mv.measures.filter((mm) => mm.gov === "governed").length
      head = (
        <>
          <span className="font-mono text-[13px] font-semibold text-primary">{n.id}</span>
          <span className="rounded-full border border-default px-1.5 text-[10.5px] text-muted">metric view</span>
          <span className="inline-flex items-center gap-1 rounded-full border border-default px-2 text-[11px] font-semibold">
            <span className="inline-block h-2 w-2 rounded-full bg-[var(--color-success)]" />
            {gN} governed
          </span>
        </>
      )
      body = (
        <>
          <InsetSection title="Join tree">
            <div className="font-mono text-[11px] text-secondary">
              {root} <span className="text-muted">source</span>
            </div>
            {joins.map((j, i) => (
              <div key={i} className="pl-3">
                <div className="flex items-center gap-1.5 font-mono text-[11px] text-secondary">
                  <span className="text-[var(--color-accent-light,var(--color-accent))]">└</span> {j.to}
                  <Chip>{j.rel}</Chip>
                </div>
                <div className="pl-4 font-mono text-[11px] text-muted">ON {onStr(j)}</div>
              </div>
            ))}
          </InsetSection>
          <InsetSection title={`Measures (${mv.measures.length})`}>
            {mv.measures.map((mm) => (
              <div key={mm.name} className="flex items-center gap-1.5 font-mono text-[11px] text-secondary">
                <span className="inline-block h-2 w-2 rounded-full" style={{ background: govColor(mm.gov) }} />
                {mm.name}
              </div>
            ))}
          </InsetSection>
          <InsetSection title="Definition">
            <dl className="text-[11px] text-muted">
              {mv.mv_filter && (
                <div>
                  <b className="text-secondary">filter</b> <span className="font-mono">{mv.mv_filter}</span>
                </div>
              )}
              {mv.materialization && (
                <div>
                  <b className="text-secondary">served</b> {mv.materialization}
                </div>
              )}
            </dl>
          </InsetSection>
        </>
      )
    } else {
      const cfg = n as BlueprintMvFixture
      head = (
        <>
          <span className="font-mono text-[13px] font-semibold text-primary">Space config</span>
          <span className="rounded-full border border-default px-1.5 text-[10.5px] text-muted">not in any metric view</span>
        </>
      )
      body = (
        <>
          <InsetSection title={`Measures (${cfg.measures.length})`}>
            {cfg.measures.map((mm) => (
              <div key={mm.name} className="flex items-center gap-1.5 font-mono text-[11px] text-secondary">
                <span className="inline-block h-2 w-2 rounded-full" style={{ background: govColor(mm.gov) }} />
                {mm.name}
              </div>
            ))}
          </InsetSection>
          <InsetSection title="Note">
            <p className="text-[11px] text-muted">
              Curated and ungoverned measures defined directly in the space config, outside any metric view. Click a
              measure to trace its lineage back to source tables.
            </p>
          </InsetSection>
        </>
      )
    }
  }

  return (
    <div className="overflow-hidden rounded-lg border border-default bg-elevated" data-inset>
      <div className="flex flex-wrap items-center gap-2 border-b border-default px-3 py-2">{head}</div>
      {warn}
      <div className="grid gap-3 p-3 sm:grid-cols-2">{body}</div>
    </div>
  )
}

// ── Panel wrapper ─────────────────────────────────────────────────────────────
function BlueprintPanel({ title, note, ...state }: FrameState & { title: string; note?: string }) {
  return (
    <div className="space-y-3 rounded-xl border border-default bg-surface p-4">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-semibold uppercase tracking-wide text-secondary">Semantic model · blueprint</h3>
        <span className="text-xs text-muted">{title}</span>
      </div>
      <Headline scenario={state.scenario} />
      <Toolbar zoom={state.zoom} layoutMode={state.layoutMode} />
      <BlueprintCanvas {...state} />
      <Legend />
      <p className="text-xs text-muted">
        Click any card or measure to trace it and open its detail below · switch Overview / Standard / Columns to
        resolve detail · crossing lines hop so relationships stay separable.
      </p>
      <DetailInset scenario={state.scenario} selected={state.selected} />
      {note && <p className="text-xs text-muted">{note}</p>}
    </div>
  )
}

// ── Frames ────────────────────────────────────────────────────────────────────
export function BlueprintStarStandardFrame() {
  return (
    <BlueprintPanel title="11a · star fixture · Standard · fact-center" scenario={blueprintStar} zoom="mid"
      selected={null} layoutMode="fact" />
  )
}

export function BlueprintStarColumnsFrame() {
  return (
    <BlueprintPanel title="11b · star fixture · Columns LOD (join-key rows, column ports)" scenario={blueprintStar}
      zoom="near" selected={null} layoutMode="fact" />
  )
}

export function BlueprintStarMeasureLineageFrame() {
  return (
    <BlueprintPanel title="11c · star fixture · Space-config measure selected (dashed lineage → sources)"
      scenario={blueprintStar} zoom="mid" selected="Space config::bookings_per_customer" layoutMode="fact" />
  )
}

export function BlueprintStarMvSelectedFrame() {
  return (
    <BlueprintPanel title="11d · star fixture · metric view selected (member boundary, dotted uses-lineage)"
      scenario={blueprintStar} zoom="mid" selected="customer_analytics_metrics" layoutMode="fact" />
  )
}

export function BlueprintUnknownRolesFrame() {
  return (
    <BlueprintPanel title="11e · unknown roles · neutral captions + connectivity headers" scenario={blueprintUnknown}
      zoom="mid" selected={null} layoutMode="fact" />
  )
}

export function BlueprintWideTableFrame() {
  return (
    <BlueprintPanel title="11f · single wide table · no joins is a valid model" scenario={blueprintWide} zoom="mid"
      selected={null} layoutMode="fact" />
  )
}

const blueprintScale30 = makeBlueprintScale()

export function BlueprintScale30Frame() {
  return (
    <BlueprintPanel title="11g · 30-table snowflake · Standard (bridges at density)" scenario={blueprintScale30}
      zoom="mid" selected={null} layoutMode="fact" />
  )
}

export function BlueprintStarOverviewFrame() {
  return (
    <BlueprintPanel title="11h · star fixture · Overview band (headers + cards only, no chips)" scenario={blueprintStar}
      zoom="far" selected={null} layoutMode="fact" />
  )
}
