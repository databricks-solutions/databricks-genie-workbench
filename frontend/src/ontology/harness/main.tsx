/**
 * Ontology Map v3 — dev-only visual harness entry (§2.1/§2.4, MV-D77).
 *
 * Mounts the REAL `EstateGraph` (same Tailwind theme, same fonts, dark shell) against
 * fixture-backed data, with every state URL-addressable so screenshots are scriptable
 * and deterministic. Served by `npm run dev` at `/graph-harness.html`; NOT part of
 * `npm run build` (the prod rollup input is `index.html` only) and never imported by
 * the app, so the prod bundle is byte-unaffected.
 *
 * URL query parameters:
 *   ?scene=default|mv|stale|empty|loading|error|slow-expand|fail-expand
 *   &origin=applied|proposed          initial source toggle (default applied)
 *   &lod=domains|subdomains|assets    initial level of detail (default domains)
 *   &focus=<topDomainId>|auto         focused business area ("auto" = first real top)
 *   &select=<node label or id>        tap this node once layout settles
 *   &delay=<ms>                       slow-variant delay (default 4000)
 *
 * Examples:
 *   /graph-harness.html                                  → Domains overview
 *   /graph-harness.html?lod=subdomains                   → Sub-domains
 *   /graph-harness.html?lod=assets&focus=auto            → drilled Assets view
 *   /graph-harness.html?lod=assets&focus=auto&select=ticket_coupon
 *   /graph-harness.html?origin=proposed                  → honest empty-proposed
 *   /graph-harness.html?scene=loading&origin=proposed    → loading card
 *   /graph-harness.html?scene=error&origin=proposed      → error card
 */
import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import "@/index.css"
import type { GraphOrigin } from "@/ontology/types"
import type { Lod } from "@/ontology/estateGraphModel"
import { EstateGraph } from "@/ontology/components/EstateGraph"
import {
  autoFocusTop,
  createMockApi,
  graphForScene,
  type MockScene,
} from "./mockApi"

const SCENES: MockScene[] = [
  "default",
  "mv",
  "stale",
  "empty",
  "loading",
  "error",
  "slow-expand",
  "fail-expand",
]
const LODS: Lod[] = ["domains", "subdomains", "assets"]
const ORIGINS: GraphOrigin[] = ["applied", "proposed"]

function pick<T extends string>(raw: string | null, allowed: readonly T[], fallback: T): T {
  return allowed.includes(raw as T) ? (raw as T) : fallback
}

// State comes from ?query and/or #hash (hash wins) — hash keeps the harness drivable
// on hosts that don't forward query strings, and lets a driver change state without a
// server round-trip (change hash + reload).
const params = new URLSearchParams(window.location.search)
const hashParams = new URLSearchParams(window.location.hash.replace(/^#/, ""))
hashParams.forEach((v, k) => params.set(k, v))
const scene = pick(params.get("scene"), SCENES, "default")
const origin = pick(params.get("origin"), ORIGINS, "applied")
const lod = pick(params.get("lod"), LODS, "domains")
const focusParam = params.get("focus")
const select = params.get("select")
const delayMs = Number(params.get("delay") ?? "4000") || 4000

const graph = graphForScene(scene)
const api = createMockApi({ scene, delayMs })

const initialFocus =
  focusParam === "auto"
    ? autoFocusTop(graph)
    : focusParam
      ? { topId: focusParam, name: graph.domains.nodes.find((n) => n.id === focusParam)?.label ?? focusParam }
      : null

// ── Dev hooks for the screenshot/vision loop ────────────────────────────────
// The headless/driven browser reads `__ontologyHarness.ready` (set on the first
// `layoutstop` after mount) and can tap nodes deterministically via `tapByLabel`.
interface HarnessCy {
  nodes(selector?: string): {
    length: number
    forEach(fn: (n: { data(): Record<string, unknown>; emit(ev: string): void }) => void): void
  }
  one(ev: string, fn: () => void): void
  emit(ev: string): void
}

declare global {
  interface Window {
    __ontologyHarness?: {
      scene: string
      origin: string
      lod: string
      ready: boolean
      cy: HarnessCy | null
      tapByLabel: (q: string) => boolean
    }
  }
}

const harness = {
  scene,
  origin,
  lod,
  ready: false,
  cy: null as HarnessCy | null,
  tapByLabel(q: string): boolean {
    const cy = harness.cy
    if (!cy) return false
    const needle = q.toLowerCase()
    let hit: { data(): Record<string, unknown>; emit(ev: string): void } | null = null
    cy.nodes().forEach((n) => {
      if (hit) return
      const d = n.data()
      const label = String(d.label ?? "").toLowerCase()
      const id = String(d.id ?? "")
      if (label.includes(needle) || id === q) hit = n
    })
    if (hit) (hit as { emit(ev: string): void }).emit("tap")
    return hit != null
  },
}
window.__ontologyHarness = harness

function onCyReady(cy: unknown) {
  const c = cy as HarnessCy
  harness.cy = c
  harness.ready = false
  c.one("layoutstop", () => {
    harness.ready = true
    if (select) setTimeout(() => harness.tapByLabel(select), 60)
  })
  // Some tiny scenes settle before the listener attaches; belt & braces.
  setTimeout(() => {
    if (!harness.ready) {
      harness.ready = true
      if (select) harness.tapByLabel(select)
    }
  }, 2500)
}

// Same page framing as OntologyPage so what the loop sees == prod.
document.documentElement.classList.add("dark")
createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <div className="bg-pattern min-h-screen">
      <div className="mx-auto max-w-[90rem] p-6">
        <p className="mb-3 text-xs text-muted">
          Ontology Map — dev harness · scene <span className="font-mono text-secondary">{scene}</span>
        </p>
        <EstateGraph
          graph={graph}
          api={api}
          initialOrigin={origin}
          initialLod={lod}
          initialFocus={initialFocus}
          onCyReady={onCyReady}
        />
      </div>
    </div>
  </StrictMode>,
)
