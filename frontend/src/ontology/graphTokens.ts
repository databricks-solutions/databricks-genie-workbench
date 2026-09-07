/**
 * Ontology Map — theme-token visual system (MV-D79, build spec §10).
 *
 * Cytoscape renders to <canvas>, which CANNOT read the app's CSS custom properties the
 * way DOM elements can. So the map's colours must be an explicit JS palette selected by
 * the resolved app theme and fed into the stylesheet the renderer builds. This replaces
 * the MV-D78 forced-`.dark` shortcut (which dropped light mode) with a first-class
 * dual-mode system: the map is an Operate surface and obeys `useTheme` like every panel.
 *
 * Contract for the two literal sets:
 *  - **dark** reproduces the exact pre-MV-D79 constants, so the dark render is unchanged
 *    (the visual-regression baseline diffs at 0% on dark cells; only light mode moves).
 *  - **light** is tuned so every node fill clears WCAG AA (>=3:1) on the light ground and
 *    label text clears >=4.5:1 on the (flipped) paper label plate. The domain palette
 *    keeps the SAME hue family per index across themes (so a domain's identity is stable)
 *    but shifts to the darker, more-saturated 600-shade that pops on a light ground.
 *
 * Note on icons: per-type glyphs sit on SATURATED node fills (not the ground) in both
 * themes, so a single light glyph stroke reads in both — only the label PLATE flips. The
 * module-level ICONS in EstateGraph therefore stay theme-independent (keeps dark identical).
 */

// Dark domain palette — index-aligned identity with estateGraphModel.PALETTE so the
// dark recolor map is a no-op. Light is the same 12 hue families at 600-shade.
const DARK_PALETTE = [
  "#818CF8", "#6EE7B7", "#FCD34D", "#F472B6", "#22D3EE", "#A78BFA",
  "#FB923C", "#34D399", "#60A5FA", "#F87171", "#C084FC", "#2DD4BF",
]
const LIGHT_PALETTE = [
  "#4F46E5", "#059669", "#D97706", "#DB2777", "#0891B2", "#7C3AED",
  "#EA580C", "#16A34A", "#2563EB", "#DC2626", "#9333EA", "#0D9488",
]

export type ResolvedTheme = "light" | "dark"

export interface GraphTokens {
  // Ground overlay (the sunken canvas colour itself stays `var(--bg-sunken)`, which
  // already flips per theme — this is only the faint dot-grid drawn over it).
  dotGrid: string
  // Label plate — the single most important flip: paper chip + dark text on light,
  // near-black chip + light text on dark. This is what "washed out" under MV-D78.
  plateBg: string
  plateOpacity: number
  // Text (all read on the plate).
  containerText: string
  subcontainerText: string
  domainText: string
  subdomainText: string
  assetText: string
  snippetText: string
  moreText: string
  ungroupedText: string
  // Compound container fills (translucent hue) — higher opacity on light or they vanish.
  containerFillOpacity: number
  subcontainerFillOpacity: number
  // Filled-disc rims.
  domainRim: string
  domainRimOpacity: number
  subdomainRim: string
  subdomainRimOpacity: number
  // Type accents.
  metricViewRim: string
  agentFill: string
  agentRim: string
  moreFill: string
  // Ungrouped = neutral leftover bucket.
  ungroupedFill: string
  ungroupedFillOpacity: number
  ungroupedBorder: string
  // Edges (hue = relationship type; weight = strength).
  edge: string
  edgeOpacity: number
  edgeCoquery: string
  edgeLineage: string
  edgeSnippet: string
  // Interaction + provenance.
  hoverRim: string
  focusRing: string
  proposedRim: string
  // Domain palette + snippet/neutral hues — index-aligned with estateGraphModel so the
  // component can recolour model-emitted `data.color` per theme (build spec §10.2 step 4).
  palette: string[]
  ungrouped: string
  measure: string
  page: string
  metricView: string
  agent: string
}

const DARK: GraphTokens = {
  dotGrid: "rgba(148, 163, 184, 0.07)",
  plateBg: "#0D1321",
  plateOpacity: 0.82,
  containerText: "#F8FAFC",
  subcontainerText: "#CBD5E1",
  domainText: "#E2E8F0",
  subdomainText: "#CBD5E1",
  assetText: "#CBD5E1",
  snippetText: "#CBD5E1",
  moreText: "#94A3B8",
  ungroupedText: "#94A3B8",
  containerFillOpacity: 0.06,
  subcontainerFillOpacity: 0.05,
  domainRim: "#F8FAFC",
  domainRimOpacity: 0.22,
  subdomainRim: "#F8FAFC",
  subdomainRimOpacity: 0.18,
  metricViewRim: "#22D3EE",
  agentFill: "#0D1321",
  agentRim: "#A78BFA",
  moreFill: "#1E293B",
  ungroupedFill: "#64748B",
  ungroupedFillOpacity: 0.18,
  ungroupedBorder: "#64748B",
  edge: "#475569",
  edgeOpacity: 0.45,
  edgeCoquery: "#818CF8",
  edgeLineage: "#64748B",
  edgeSnippet: "#38BDF8",
  hoverRim: "#E2E8F0",
  focusRing: "#22D3EE",
  proposedRim: "#CBD5E1",
  palette: DARK_PALETTE,
  ungrouped: "#64748B",
  measure: "#38BDF8",
  page: "#FBBF24",
  metricView: "#22D3EE",
  agent: "#A78BFA",
}

const LIGHT: GraphTokens = {
  dotGrid: "rgba(100, 116, 139, 0.10)",
  plateBg: "#FFFFFF",
  plateOpacity: 0.88,
  containerText: "#0F172A",
  subcontainerText: "#334155",
  domainText: "#0F172A",
  subdomainText: "#334155",
  assetText: "#1E293B",
  snippetText: "#334155",
  moreText: "#475569",
  ungroupedText: "#475569",
  containerFillOpacity: 0.12,
  subcontainerFillOpacity: 0.1,
  domainRim: "#0F172A",
  domainRimOpacity: 0.14,
  subdomainRim: "#0F172A",
  subdomainRimOpacity: 0.12,
  metricViewRim: "#0891B2",
  agentFill: "#1E293B", // dark node on the light ground: high contrast, light glyph reads
  agentRim: "#7C3AED",
  moreFill: "#E2E8F0",
  ungroupedFill: "#475569",
  ungroupedFillOpacity: 0.16,
  ungroupedBorder: "#475569",
  edge: "#64748B",
  edgeOpacity: 0.5,
  edgeCoquery: "#4F46E5",
  edgeLineage: "#64748B",
  edgeSnippet: "#0284C7",
  hoverRim: "#0F172A",
  focusRing: "#0891B2",
  proposedRim: "#475569",
  palette: LIGHT_PALETTE,
  ungrouped: "#475569",
  measure: "#0284C7",
  page: "#D97706",
  metricView: "#0891B2",
  agent: "#7C3AED",
}

/** The full canvas palette for the resolved app theme (pure; no side effects). */
export function graphTokens(theme: ResolvedTheme): GraphTokens {
  return theme === "light" ? LIGHT : DARK
}

/**
 * Recolour map from the model's dark-constant palette to the theme palette (build spec
 * §10.2 step 4). estateGraphModel emits `data.color` from its fixed dark PALETTE +
 * ungrouped/measure/page + the metric-view (#22D3EE) / agent (#A78BFA) accents (which are
 * themselves palette entries), so a hex->hex map recolours every node without touching the
 * pure model. On dark this is the identity map (⇒ dark cells diff at 0%).
 */
export function recolorMap(tokens: GraphTokens): Map<string, string> {
  const m = new Map<string, string>()
  DARK_PALETTE.forEach((hex, i) => m.set(hex, tokens.palette[i]))
  m.set("#64748B", tokens.ungrouped) // UNGROUPED_COLOR
  m.set("#38BDF8", tokens.measure) // MEASURE_COLOR
  m.set("#FBBF24", tokens.page) // PAGE_COLOR
  return m
}
