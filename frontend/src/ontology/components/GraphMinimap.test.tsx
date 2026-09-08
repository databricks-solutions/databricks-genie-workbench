import { renderToStaticMarkup } from "react-dom/server"
import { describe, expect, it } from "vitest"
import { GraphMinimap, computeMiniGeom, miniToContent, type MiniPoint } from "./GraphMinimap"

const points: MiniPoint[] = [
  { x: 0, y: 0, color: "#f00" },
  { x: 100, y: 200, color: "#0f0" },
]

describe("computeMiniGeom + miniToContent — projection math (MV-D87 / R26)", () => {
  it("covers points, rects and the viewport in one shared projection", () => {
    const geom = computeMiniGeom(points, [], { x1: -50, y1: -50, x2: 150, y2: 250 })
    // Bounds must extend to the viewport corners, not just the nodes.
    expect(geom.minX).toBe(-50)
    expect(geom.minY).toBe(-50)
    expect(geom.scale).toBeGreaterThan(0)
  })

  it("miniToContent inverts the forward projection (round-trip)", () => {
    const geom = computeMiniGeom(points)
    // Forward: content → minimap px (mirrors the component's sx/sy at PAD=6).
    const PAD = 6
    const px = PAD + (100 - geom.minX) * geom.scale
    const py = PAD + (200 - geom.minY) * geom.scale
    const back = miniToContent(px, py, geom)
    expect(back.x).toBeCloseTo(100)
    expect(back.y).toBeCloseTo(200)
  })

  it("degrades safely with no points", () => {
    const geom = computeMiniGeom([])
    expect(geom.scale).toBe(1)
  })
})

describe("GraphMinimap — presentational render", () => {
  it("shows the placeholder when empty", () => {
    const html = renderToStaticMarkup(<GraphMinimap points={[]} />)
    expect(html).toContain("Overview")
  })

  it("draws the you-are-here viewport box when a viewport is supplied", () => {
    const html = renderToStaticMarkup(<GraphMinimap points={points} viewport={{ x1: 0, y1: 0, x2: 50, y2: 50 }} />)
    expect(html).toContain("#22D3EE") // the viewport box stroke
  })

  it("advertises a navigable role only when onNavigate is wired (R26)", () => {
    const plain = renderToStaticMarkup(<GraphMinimap points={points} />)
    expect(plain).toContain('role="img"')
    const nav = renderToStaticMarkup(<GraphMinimap points={points} onNavigate={() => {}} />)
    expect(nav).toContain('role="slider"')
    expect(nav).toContain("click or drag to pan")
  })
})
