import { describe, expect, it } from "vitest"
import { MOTION, easeCubicOut, lerp, lerpPoint, prefersReducedMotion } from "@/ontology/ontologyMotion"

describe("easeCubicOut", () => {
  it("pins the endpoints exactly (0→0, 1→1)", () => {
    expect(easeCubicOut(0)).toBe(0)
    expect(easeCubicOut(1)).toBe(1)
  })

  it("clamps out-of-range input so an overshooting frame lands on the target", () => {
    expect(easeCubicOut(-0.5)).toBe(0)
    expect(easeCubicOut(1.7)).toBe(1)
  })

  it("eases OUT — fast start, gentle settle (ahead of linear in the first half)", () => {
    // 1 - (1-0.5)^3 = 1 - 0.125 = 0.875, well past the linear 0.5.
    expect(easeCubicOut(0.5)).toBeCloseTo(0.875)
    // Monotonic increasing.
    expect(easeCubicOut(0.25)).toBeLessThan(easeCubicOut(0.75))
  })

  it("matches the closed form 1-(1-t)^3", () => {
    for (const t of [0.1, 0.33, 0.66, 0.9]) {
      expect(easeCubicOut(t)).toBeCloseTo(1 - Math.pow(1 - t, 3))
    }
  })
})

describe("lerp / lerpPoint", () => {
  it("lerp interpolates and pins endpoints", () => {
    expect(lerp(10, 20, 0)).toBe(10)
    expect(lerp(10, 20, 1)).toBe(20)
    expect(lerp(10, 20, 0.5)).toBe(15)
    expect(lerp(-40, 40, 0.25)).toBe(-20)
  })

  it("lerpPoint interpolates both axes independently", () => {
    const p = lerpPoint({ x: 0, y: 100 }, { x: 200, y: 0 }, 0.5)
    expect(p.x).toBe(100)
    expect(p.y).toBe(50)
  })

  it("lerpPoint at t=0 / t=1 returns the endpoints", () => {
    const a = { x: 3, y: 7 }
    const b = { x: 9, y: 1 }
    expect(lerpPoint(a, b, 0)).toEqual(a)
    expect(lerpPoint(a, b, 1)).toEqual(b)
  })
})

describe("MOTION durations", () => {
  it("exposes positive glide + camera durations", () => {
    expect(MOTION.glideMs).toBeGreaterThan(0)
    expect(MOTION.cameraMs).toBeGreaterThan(0)
  })
})

describe("prefersReducedMotion", () => {
  it("is false in a node/SSR context (no window, no matchMedia)", () => {
    // The vitest environment is 'node' — neither window nor matchMedia exist, so motion is ON
    // by default and callers animate. The harness forces it OFF via emulateMedia / the flag.
    expect(prefersReducedMotion()).toBe(false)
  })
})
