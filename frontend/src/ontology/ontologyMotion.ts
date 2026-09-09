/**
 * Ontology Map — pure motion primitives for the "buttery interactions" pass (frontend-only,
 * no new dependency). d3-transition / d3-ease are NOT installed, so we hand-roll rAF tweens
 * driven by these helpers. NO DOM, NO React — kept pure so the easing / interpolation math
 * is unit-tested without mounting the camera.
 *
 * All motion is reduced-motion aware: `prefersReducedMotion()` gates the caller so a user
 * (or the deterministic screenshot harness) who wants no animation gets today's instant
 * behaviour with byte-identical resting geometry.
 */
import type { Point } from "@/ontology/ontologyTreeLayout"

/** Tween durations (ms). Tuned to the north-star mockup's glide + camera ease. */
export const MOTION = {
  /** Relayout glide (expand/collapse) — nodes + their incident edges slide together. */
  glideMs: 380,
  /** Fit / Reset camera transition. */
  cameraMs: 480,
} as const

/**
 * Ease-out cubic: `1 - (1 - t)^3`. Fast start, gentle settle — the north-star feel. Input is
 * clamped to [0,1] so a caller that overshoots the frame budget still lands exactly on the
 * target (t≥1 ⇒ 1) rather than past it.
 */
export function easeCubicOut(t: number): number {
  const c = t < 0 ? 0 : t > 1 ? 1 : t
  const inv = 1 - c
  return 1 - inv * inv * inv
}

/** Scalar linear interpolation. */
export function lerp(a: number, b: number, t: number): number {
  return a + (b - a) * t
}

/** Point linear interpolation (used for node prev→next glide). */
export function lerpPoint(a: Point, b: Point, t: number): Point {
  return { x: lerp(a.x, b.x, t), y: lerp(a.y, b.y, t) }
}

/**
 * True when animation should be suppressed (instant = today's behaviour). Honours the OS
 * `prefers-reduced-motion` setting AND an explicit harness override
 * (`window.__ontologyMotionOff`), so the deterministic screenshot loop always captures the
 * FINAL frame. SSR / node-test safe: returns `false` when `window`/`matchMedia` are absent.
 */
export function prefersReducedMotion(): boolean {
  if (typeof window !== "undefined") {
    const w = window as unknown as { __ontologyMotionOff?: boolean }
    if (w.__ontologyMotionOff) return true
  }
  if (typeof matchMedia === "function") {
    try {
      return matchMedia("(prefers-reduced-motion: reduce)").matches
    } catch {
      return false
    }
  }
  return false
}
