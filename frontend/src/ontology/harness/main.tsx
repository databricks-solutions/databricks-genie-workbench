/**
 * Ontology Map — dev-only visual harness entry (MV-D77/D80). Mounts the REAL
 * `EstateGraph` (same Tailwind theme, same fonts, dark shell) against fixture-backed data,
 * every state URL-addressable so screenshots are scriptable + deterministic. Served by
 * `npm run dev` at `/graph-harness.html`; NOT part of `npm run build` and never imported by
 * the app, so the prod bundle is byte-unaffected.
 *
 * URL query (or #hash, which wins):
 *   ?scene=default|northstar|stale|empty|loading|error|stress|proposed
 *   &origin=applied|proposed          initial provenance (default applied)
 *   &select=<node label or id>        reveal + select this node once laid out
 *   &panel=domains                    open the domain show/hide panel on mount (R27 shot)
 *   &theme=light|dark                 app theme (default dark)
 */
import { StrictMode } from "react"
import { createRoot } from "react-dom/client"
import "@/index.css"
import type { GraphOrigin } from "@/ontology/types"
import { EstateGraph, type EstateGraphHandle } from "@/ontology/components/EstateGraph"
import { createMockApi, graphForScene, type MockScene } from "./mockApi"

const SCENES: MockScene[] = [
  "default",
  "northstar",
  "stale",
  "empty",
  "loading",
  "error",
  "stress",
  "proposed",
]
const ORIGINS: GraphOrigin[] = ["applied", "proposed"]

function pick<T extends string>(raw: string | null, allowed: readonly T[], fallback: T): T {
  return allowed.includes(raw as T) ? (raw as T) : fallback
}

const params = new URLSearchParams(window.location.search)
const hashParams = new URLSearchParams(window.location.hash.replace(/^#/, ""))
hashParams.forEach((v, k) => params.set(k, v))
const scene = pick(params.get("scene"), SCENES, "default")
const origin = pick(params.get("origin"), ORIGINS, "applied")
const select = params.get("select")
const domainPanelOpen = params.get("panel") === "domains"

const graph = graphForScene(scene)
const api = createMockApi({ scene })

// ── Dev hooks for the screenshot/vision loop ────────────────────────────────
interface OntologyHarness {
  scene: string
  origin: string
  ready: boolean
  layoutMs: number | null
  nodeCount: number
  layoutHash: string | null
  tapByLabel: (q: string) => boolean
  positions: () => { id: string; x: number; y: number }[]
}

declare global {
  interface Window {
    __ontologyHarness?: OntologyHarness
  }
}

const mountedAt = performance.now()
const harness: OntologyHarness = {
  scene,
  origin,
  ready: false,
  layoutMs: null as number | null,
  nodeCount: graph.node_count,
  layoutHash: null as string | null,
  tapByLabel: (): boolean => false,
  positions: () => [] as { id: string; x: number; y: number }[],
}
window.__ontologyHarness = harness

function onReady(handle: EstateGraphHandle) {
  harness.ready = handle.ready
  harness.nodeCount = handle.nodeCount
  harness.layoutHash = handle.layoutHash
  harness.tapByLabel = handle.tapByLabel
  harness.positions = handle.positions
  if (harness.layoutMs == null) harness.layoutMs = Math.round(performance.now() - mountedAt)
  if (select) setTimeout(() => harness.tapByLabel(select), 120)
}

const theme = params.get("theme") === "light" ? "light" : "dark"
localStorage.setItem("genierx-theme", theme)
document.documentElement.classList.toggle("dark", theme === "dark")
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
          initialDomainPanelOpen={domainPanelOpen}
          onReady={onReady}
        />
      </div>
    </div>
  </StrictMode>,
)
