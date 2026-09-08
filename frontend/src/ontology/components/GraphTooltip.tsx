/* eslint-disable react-refresh/only-export-components */
/**
 * Ontology Map north-star (MV-D85, §4/R6/R21) — the hover-snippet tooltip: the
 * "everything tells you something" layer. On node hover it shows the type label, full
 * name, a plain-language description, up to ~6 metadata key/values, and (for a measure)
 * its Expression in mono. It follows the cursor, clamps to the viewport, is
 * `pointer-events:none` (never eats the hover it describes), and is dual-theme via the
 * resolved `graphTokens`. Distinct from click (which docks the inspector) and from the
 * native `<title>` we keep for a11y (R6). Presentational + a pure content assembler so the
 * assembly is unit-testable without a DOM.
 *
 * MV-D87 (Lane P2, §5/R25) adds the EDGE variant: hovering a relationship arc shows
 * `From → To`, the verb, within/cross-domain class, and — when the pre-seed `detail` bag is
 * present (MV-D88) — its evidence lines; it degrades to verb + endpoints + class when absent.
 */
import { useLayoutEffect, useRef, useState, type ReactNode } from "react"
import type { GraphTokens } from "@/ontology/graphTokens"

export interface TooltipData {
  typeLabel: string
  /** Full name (never ellipsized — the canvas caption is the truncated one). */
  name: string
  description: string
  /** Up to `maxMeta` plain key/values (Expression is pulled out separately). */
  meta: [string, string][]
  /** A measure's defining expression, rendered in mono; null when absent. */
  expression: string | null
}

/**
 * Assemble a tooltip payload from a node's real (MV-D86) description/meta, degrading to
 * generic copy when the snapshot has none (R13 — never fabricates, only names what exists).
 * Prefers `description` over `fallbackDescription`; lifts a case-insensitive `expression`
 * key out of `meta` into its own mono line; caps the remaining meta at `maxMeta` (~6).
 */
export function assembleTooltip(input: {
  typeLabel: string
  name: string
  description: string | null | undefined
  meta: Record<string, string> | null | undefined
  fallbackDescription: string
  maxMeta?: number
}): TooltipData {
  const desc =
    input.description && input.description.trim() ? input.description.trim() : input.fallbackDescription
  const entries = input.meta ? Object.entries(input.meta) : []
  const expr = entries.find(([k]) => k.toLowerCase() === "expression")
  const meta = entries
    .filter(([k, v]) => k.toLowerCase() !== "expression" && v != null && `${v}`.trim() !== "")
    .slice(0, input.maxMeta ?? 6)
    .map(([k, v]) => [k, `${v}`] as [string, string])
  return {
    typeLabel: input.typeLabel,
    name: input.name,
    description: desc,
    meta,
    expression: expr ? expr[1] : null,
  }
}

// ── Edge (relationship) tooltip (MV-D87, §5/R25) ─────────────────────────────

export interface EdgeTooltipData {
  fromName: string
  toName: string
  verb: string
  /** "Within business area" (shared) | "Across business areas" (cross-domain). */
  classLabel: string
  xdom: boolean
  /** Evidence key/values from the pre-seed `detail` bag; empty when the edge has none. */
  detail: [string, string][]
}

/**
 * Assemble the edge-tooltip payload. Always carries the two endpoint names, the plain verb,
 * and the within/cross-domain class; folds in the optional pre-seed evidence bag (MV-D88)
 * when present, and DEGRADES to just verb + endpoints + class when `detail` is null/empty —
 * so Lane P2 never requires Lane E to ship (R13, R25). Pure; caps evidence at `maxDetail`.
 */
export function assembleEdgeTooltip(input: {
  fromName: string
  toName: string
  verb: string
  xdom: boolean
  detail?: Record<string, string> | null
  maxDetail?: number
}): EdgeTooltipData {
  const detail = input.detail
    ? Object.entries(input.detail)
        .filter(([, v]) => v != null && `${v}`.trim() !== "")
        .slice(0, input.maxDetail ?? 6)
        .map(([k, v]) => [k, `${v}`] as [string, string])
    : []
  return {
    fromName: input.fromName,
    toName: input.toName,
    verb: input.verb,
    classLabel: input.xdom ? "Across business areas" : "Within business area",
    xdom: input.xdom,
    detail,
  }
}

const WIDTH = 264
const OFFSET = 14
const MARGIN = 8

/**
 * Shared cursor-following card. `x`/`y` are viewport (client) coordinates; the card is
 * `position:fixed` so it tracks the pointer over the zoomed/panned SVG, measures itself, and
 * clamps so it never spills off the viewport (flips to the other side of the cursor near an
 * edge). `pointer-events:none` so it never eats the hover it describes (R21).
 */
function HoverCard({
  x,
  y,
  tokens,
  children,
}: {
  x: number
  y: number
  tokens: GraphTokens
  children: ReactNode
}) {
  const ref = useRef<HTMLDivElement | null>(null)
  const [size, setSize] = useState({ w: WIDTH, h: 0 })
  useLayoutEffect(() => {
    const el = ref.current
    if (el) setSize({ w: el.offsetWidth, h: el.offsetHeight })
  }, [children])

  const vw = typeof window !== "undefined" ? window.innerWidth : 1280
  const vh = typeof window !== "undefined" ? window.innerHeight : 800
  let left = x + OFFSET
  if (left + size.w + MARGIN > vw) left = Math.max(MARGIN, x - OFFSET - size.w)
  let top = y + OFFSET
  if (top + size.h + MARGIN > vh) top = Math.max(MARGIN, y - OFFSET - size.h)

  return (
    <div
      ref={ref}
      role="tooltip"
      aria-hidden
      style={{
        position: "fixed",
        left,
        top,
        width: WIDTH,
        maxWidth: WIDTH,
        pointerEvents: "none",
        zIndex: 60,
        background: tokens.plateBg,
        color: tokens.plateText,
        border: `1px solid ${tokens.trayNodeStroke}`,
        borderRadius: 8,
        padding: "8px 10px",
        boxShadow: "0 8px 24px rgba(0,0,0,0.28)",
        fontSize: 12,
        lineHeight: 1.35,
      }}
    >
      {children}
    </div>
  )
}

/** Node hover snippet (§4/R6/R21). */
export function GraphTooltip({
  data,
  x,
  y,
  tokens,
}: {
  data: TooltipData | null
  x: number
  y: number
  tokens: GraphTokens
}) {
  if (!data) return null
  return (
    <HoverCard x={x} y={y} tokens={tokens}>
      <div style={{ fontSize: 10, textTransform: "uppercase", letterSpacing: "0.04em", opacity: 0.7 }}>
        {data.typeLabel}
      </div>
      <div style={{ fontWeight: 600, wordBreak: "break-word" }}>{data.name}</div>
      {data.description && <div style={{ marginTop: 4, opacity: 0.9 }}>{data.description}</div>}
      {data.meta.length > 0 && (
        <div style={{ marginTop: 6, display: "grid", gridTemplateColumns: "auto 1fr", columnGap: 8, rowGap: 2 }}>
          {data.meta.map(([k, v]) => (
            <div key={k} style={{ display: "contents" }}>
              <span style={{ opacity: 0.6 }}>{k}</span>
              <span style={{ textAlign: "right", wordBreak: "break-word" }}>{v}</span>
            </div>
          ))}
        </div>
      )}
      {data.expression && (
        <div
          style={{
            marginTop: 6,
            fontFamily:
              "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace",
            fontSize: 11,
            opacity: 0.9,
            wordBreak: "break-word",
          }}
        >
          {data.expression}
        </div>
      )}
    </HoverCard>
  )
}

/** Edge (relationship) hover tooltip (§5/R25). */
export function GraphEdgeTooltip({
  data,
  x,
  y,
  tokens,
}: {
  data: EdgeTooltipData | null
  x: number
  y: number
  tokens: GraphTokens
}) {
  if (!data) return null
  const accent = data.xdom ? tokens.verbXdomText : tokens.verbText
  return (
    <HoverCard x={x} y={y} tokens={tokens}>
      <div style={{ fontSize: 10, textTransform: "uppercase", letterSpacing: "0.04em", opacity: 0.7 }}>
        Relationship
      </div>
      <div style={{ fontWeight: 600, wordBreak: "break-word" }}>
        {data.fromName} <span style={{ opacity: 0.6 }}>→</span> {data.toName}
      </div>
      <div style={{ marginTop: 4, display: "flex", flexWrap: "wrap", alignItems: "center", gap: 6 }}>
        <span style={{ fontWeight: 600, color: accent }}>{data.verb}</span>
        <span
          style={{
            fontSize: 10,
            padding: "1px 5px",
            borderRadius: 4,
            border: `1px solid ${tokens.trayNodeStroke}`,
            color: accent,
          }}
        >
          {data.classLabel}
        </span>
      </div>
      {data.detail.length > 0 && (
        <div style={{ marginTop: 6, display: "grid", gridTemplateColumns: "auto 1fr", columnGap: 8, rowGap: 2 }}>
          {data.detail.map(([k, v]) => (
            <div key={k} style={{ display: "contents" }}>
              <span style={{ opacity: 0.6 }}>{k}</span>
              <span style={{ textAlign: "right", wordBreak: "break-word" }}>{v}</span>
            </div>
          ))}
        </div>
      )}
    </HoverCard>
  )
}
