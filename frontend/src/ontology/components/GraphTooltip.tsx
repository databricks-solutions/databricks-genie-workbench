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
 */
import { useLayoutEffect, useRef, useState } from "react"
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

const WIDTH = 264
const OFFSET = 14
const MARGIN = 8

/**
 * Cursor-following hover snippet. `x`/`y` are viewport (client) coordinates; the card is
 * `position:fixed` so it tracks the pointer over the zoomed/panned SVG, and clamps so it
 * never spills off the viewport (flips to the other side of the cursor near an edge).
 */
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
  const ref = useRef<HTMLDivElement | null>(null)
  const [size, setSize] = useState({ w: WIDTH, h: 0 })
  useLayoutEffect(() => {
    const el = ref.current
    if (el) setSize({ w: el.offsetWidth, h: el.offsetHeight })
  }, [data])

  if (!data) return null

  const vw = typeof window !== "undefined" ? window.innerWidth : 1280
  const vh = typeof window !== "undefined" ? window.innerHeight : 800
  // Prefer down-right of the cursor; flip when it would overflow the viewport edge.
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
        zIndex: 50,
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
      <div style={{ fontSize: 10, textTransform: "uppercase", letterSpacing: "0.04em", opacity: 0.7 }}>
        {data.typeLabel}
      </div>
      <div style={{ fontWeight: 600, wordBreak: "break-word" }}>{data.name}</div>
      {data.description && (
        <div style={{ marginTop: 4, opacity: 0.9 }}>{data.description}</div>
      )}
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
    </div>
  )
}
