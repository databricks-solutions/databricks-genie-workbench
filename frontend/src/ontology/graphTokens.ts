/**
 * Ontology Map — theme-token visual system (MV-D79, §4.2, §9-B).
 *
 * The map renders to real SVG/DOM, but colour still comes from an explicit JS token set
 * selected by the resolved app theme (not raw CSS custom properties) so the layout module
 * and tests stay pure and the two themes are authored deliberately at parity.
 *
 * §9-B — **colour by node TYPE, not by domain.** Type is the primary hue
 * (agent = Lava, dashboard = blue, metric_view = green, measure = amber, table = slate;
 * org/domain/subdomain = ink shades). Domain identity moves to (a) tree position and
 * (b) a subtle domain-hue **tint** on domain/sub-domain rings + that domain's spine edges
 * — never on asset fills. **Lava is reserved for Genie Agents.**
 *
 * Every type hue clears WCAG AA (≥3:1 fill-vs-ground) and every label clears ≥4.5:1 on the
 * (theme-flipped) label plate, in BOTH themes.
 */
import type { NodeType } from "@/ontology/estateGraphModel"

export type ResolvedTheme = "light" | "dark"

export interface GraphTokens {
  /** Faint dot-grid drawn over the sunken canvas (drafting-table depth). */
  dotGrid: string
  ground: string
  /** Label plate — paper chip + dark text on light; near-black chip + light text on dark. */
  plateBg: string
  plateOpacity: number
  plateText: string
  /** Per-type node fill (the primary encoding). */
  typeFill: Record<NodeType, string>
  /** A light or dark glyph stroke that reads on the saturated fills. */
  glyphStroke: string
  nodeStroke: string
  /** Container rings (org/domain/subdomain) carry a domain tint at low weight. */
  ringOpacity: number
  /** Interaction. */
  selectedRing: string
  searchRing: string
  hoverRing: string
  /** Hierarchy spine edges (low-contrast). */
  spine: string
  spineOpacity: number
  /** Typed cross-link hues (§4.5): within-domain slate, cross-domain maroon. */
  sharedEdge: string
  xdomEdge: string
  verbText: string
  verbXdomText: string
  /** Off-tree Ungrouped tray (§4.7) — demoted, neutral. */
  trayDivider: string
  trayText: string
  trayNodeFill: string
  trayNodeStroke: string
  /** Dashed proposal hull (§4.7). */
  proposalStroke: string
  proposalText: string
  bandHigh: string
  bandMedium: string
  bandLow: string
  /** Collapse `+N` badge. */
  badgeFill: string
  badgeText: string
  /** Domain-tint palette (index-aligned per top domain) for rings + spine tint. */
  domainTint: string[]
}

const DARK_DOMAIN_TINT = [
  "#818CF8", "#6EE7B7", "#FCD34D", "#F472B6", "#22D3EE", "#A78BFA",
  "#FB923C", "#34D399", "#60A5FA", "#F87171", "#C084FC", "#2DD4BF",
]
const LIGHT_DOMAIN_TINT = [
  "#4F46E5", "#059669", "#D97706", "#DB2777", "#0891B2", "#7C3AED",
  "#EA580C", "#16A34A", "#2563EB", "#DC2626", "#9333EA", "#0D9488",
]

const DARK: GraphTokens = {
  dotGrid: "rgba(148, 163, 184, 0.07)",
  ground: "#0B0F1A",
  plateBg: "#0D1321",
  plateOpacity: 0.82,
  plateText: "#E2E8F0",
  typeFill: {
    org: "#CBD5E1",
    domain: "#94A3B8",
    subdomain: "#64748B",
    agent: "#FF5F46", // Lava — agents only
    dashboard: "#60A5FA",
    metric_view: "#34D399",
    measure: "#FBBF24",
    table: "#7C8CA3",
  },
  glyphStroke: "#0B0F1A",
  nodeStroke: "#0B0F1A",
  ringOpacity: 0.55,
  selectedRing: "#F8FAFC",
  searchRing: "#22D3EE",
  hoverRing: "#E2E8F0",
  spine: "#475569",
  spineOpacity: 0.4,
  sharedEdge: "#64748B",
  xdomEdge: "#B45E7A",
  verbText: "#94A3B8",
  verbXdomText: "#E5A3B4",
  trayDivider: "#334155",
  trayText: "#94A3B8",
  trayNodeFill: "#1E293B",
  trayNodeStroke: "#475569",
  proposalStroke: "#94A3B8",
  proposalText: "#CBD5E1",
  bandHigh: "#34D399",
  bandMedium: "#FBBF24",
  bandLow: "#94A3B8",
  badgeFill: "#0D1321",
  badgeText: "#F8FAFC",
  domainTint: DARK_DOMAIN_TINT,
}

const LIGHT: GraphTokens = {
  dotGrid: "rgba(100, 116, 139, 0.10)",
  ground: "#F1F5F9",
  plateBg: "#FFFFFF",
  plateOpacity: 0.9,
  plateText: "#0F172A",
  typeFill: {
    org: "#334155",
    domain: "#475569",
    subdomain: "#64748B",
    agent: "#DC2626", // Lava (deeper on light) — agents only
    dashboard: "#2563EB",
    metric_view: "#059669",
    measure: "#D97706",
    table: "#64748B",
  },
  glyphStroke: "#FFFFFF",
  nodeStroke: "#FFFFFF",
  ringOpacity: 0.6,
  selectedRing: "#0F172A",
  searchRing: "#0891B2",
  hoverRing: "#0F172A",
  spine: "#94A3B8",
  spineOpacity: 0.55,
  sharedEdge: "#64748B",
  xdomEdge: "#9F1239",
  verbText: "#475569",
  verbXdomText: "#9F1239",
  trayDivider: "#CBD5E1",
  trayText: "#475569",
  trayNodeFill: "#E2E8F0",
  trayNodeStroke: "#94A3B8",
  proposalStroke: "#64748B",
  proposalText: "#334155",
  bandHigh: "#059669",
  bandMedium: "#D97706",
  bandLow: "#64748B",
  badgeFill: "#0F172A",
  badgeText: "#F8FAFC",
  domainTint: LIGHT_DOMAIN_TINT,
}

/** The full token set for the resolved app theme (pure; no side effects). */
export function graphTokens(theme: ResolvedTheme): GraphTokens {
  return theme === "light" ? LIGHT : DARK
}

/** Stable domain-tint hue for a top-domain id (index-aligned, deterministic). */
export function domainTintFor(tokens: GraphTokens, topId: string | null): string | null {
  if (!topId) return null
  let h = 0
  for (let i = 0; i < topId.length; i++) h = (h * 31 + topId.charCodeAt(i)) >>> 0
  return tokens.domainTint[h % tokens.domainTint.length]
}

/** Band label → its token colour. */
export function bandColor(tokens: GraphTokens, band: "High" | "Medium" | "Low" | null): string {
  if (band === "High") return tokens.bandHigh
  if (band === "Medium") return tokens.bandMedium
  if (band === "Low") return tokens.bandLow
  return tokens.trayText
}
