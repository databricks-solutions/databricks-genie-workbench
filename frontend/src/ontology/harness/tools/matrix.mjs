/**
 * Ontology Map visual loop — the state matrix (dev-only, MV-D80 §11 / DESIGN §6).
 *
 * The single source of truth for WHAT the loop shoots: {scenes} × {themes}. Each scene
 * maps to harness URL params (see harness/main.tsx). shoot.mjs renders every cell,
 * contact.mjs tiles them (rows = scenes, cols = themes), diff.mjs compares by cell.
 * Keep this list and DESIGN.md §6 in sync — it IS the surface the Reviewer scores.
 */
export const THEMES = ["light", "dark"]

/** scene name → harness query params (theme is appended per cell). */
export const SCENES = [
  { name: "domains", p: { lod: "domains" } },
  { name: "subdomains", p: { lod: "subdomains" } },
  { name: "assets", p: { lod: "assets", focus: "auto" } },
  { name: "assets+select", p: { lod: "assets", focus: "auto", select: "ticket_coupon" } },
  { name: "mv-expand", p: { scene: "mv", lod: "assets", focus: "auto", select: "cost attribution" } },
  { name: "proposed", p: { origin: "proposed" } },
  { name: "stale", p: { scene: "stale" } },
  { name: "empty", p: { scene: "empty" } },
  { name: "error", p: { scene: "error", origin: "proposed" } },
  { name: "stress", p: { scene: "stress", lod: "assets", focus: "auto" } },
]

/** Scenes that render an honest CARD (no cytoscape instance ⇒ no `ready` flag). */
export const CARD_SCENES = new Set(["empty", "error"])

/** Flattened {scene × theme} cells with their query string + output filename. */
export function cells() {
  const out = []
  for (const s of SCENES) {
    for (const t of THEMES) {
      const qs = new URLSearchParams({ ...s.p, theme: t }).toString()
      out.push({ scene: s.name, theme: t, q: qs, file: `${s.name}.${t}.png`, card: CARD_SCENES.has(s.name) })
    }
  }
  return out
}
