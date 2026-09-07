/**
 * Ontology Map visual loop — the state matrix (dev-only, MV-D80 §11 / DESIGN §6).
 *
 * The single source of truth for WHAT the loop shoots: {scenes} × {themes}. Each scene
 * maps to harness URL params (see harness/main.tsx). shoot.mjs renders every cell,
 * contact.mjs tiles them (rows = scenes, cols = themes), diff.mjs compares by cell.
 * Keep this list and DESIGN.md §6 in sync — it IS the surface the Reviewer scores.
 */
export const THEMES = ["light", "dark"]

/** scene name → harness query params (theme is appended per cell). See harness/main.tsx. */
export const SCENES = [
  { name: "northstar", p: { scene: "northstar" } },
  { name: "northstar+select", p: { scene: "northstar", select: "net sales" } },
  { name: "proposed", p: { scene: "proposed", origin: "proposed" } },
  { name: "degrade", p: { scene: "default" } },
  { name: "stale", p: { scene: "stale" } },
  { name: "empty", p: { scene: "empty" } },
  { name: "loading", p: { scene: "loading" } },
  { name: "error", p: { scene: "error" } },
  { name: "stress", p: { scene: "stress" } },
]

/** Scenes that render an honest CARD rather than the graph (loading/error/empty). */
export const CARD_SCENES = new Set(["empty", "error", "loading"])

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
