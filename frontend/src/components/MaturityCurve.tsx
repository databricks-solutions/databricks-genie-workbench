/**
 * MaturityCurve — SVG S-curve visualization showing 3 maturity tiers
 * with colored area bands under the curve and a pulsing "You Are Here" marker.
 * Theme-aware: uses currentColor + Tailwind classes for light/dark mode.
 */
import { useRef, useEffect, useState } from "react"

interface MaturityCurveProps {
  score: number              // 0-15
  total: number              // 15
  maturity: string           // current tier label
}

/** 3 maturity tiers — pct is center of each zone for label placement. */
const TIERS = [
  { label: "Not Ready",          tagline: "Configuration checks still failing",       color: "#ef4444", pct: 1/6 },
  { label: "Ready to Optimize",  tagline: "All config checks pass — run benchmarks",  color: "#3b82f6", pct: 3/6 },
  { label: "Trusted",            tagline: "Benchmarked and verified",                  color: "#10b981", pct: 5/6 },
]

/** Boundary percentages for the 3 tier zones. */
const ZONE_BOUNDARIES = [0, 1/3, 2/3, 1.0]

// Compact S-curve: flat bottom-left → steep rise → flat top-right
// S-curve with room below for labels
const CURVE_D = "M 60,170 C 130,170 200,165 350,90 C 500,15 570,8 740,8"
const AREA_D  = `${CURVE_D} L 740,185 L 60,185 Z`

export function MaturityCurve({ score, total, maturity }: MaturityCurveProps) {
  const measureRef = useRef<SVGPathElement>(null)
  const [pathLen, setPathLen]   = useState(0)
  const [zoneBounds, setZoneBounds] = useState<number[]>([])
  const [tierPts, setTierPts]   = useState<{ x: number; y: number }[]>([])
  const [mPos, setMPos]         = useState({ x: 60, y: 170 })
  const [drawn, setDrawn]       = useState(false)

  useEffect(() => {
    const path = measureRef.current
    if (!path) return

    const len = path.getTotalLength()
    setPathLen(len)

    // Compute x-coordinates at zone boundaries for clip paths
    setZoneBounds(ZONE_BOUNDARIES.map(pct => path.getPointAtLength(len * pct).x))

    // Compute label positions at zone centers
    setTierPts(TIERS.map(t => {
      const p = path.getPointAtLength(len * t.pct)
      return { x: p.x, y: p.y }
    }))

    // Non-linear score-to-position mapping:
    // 0-12 → first third (Not Ready)
    // 13   → middle third (Ready to Optimize)
    // 14   → middle third (optimized but < 85%)
    // 15   → final third (Trusted)
    let pct: number
    if (score <= 12) {
      pct = (score / 12) * (1/3)
    } else if (score === 13) {
      pct = 1/3 + (1/3) * 0.5
    } else if (score === 14) {
      pct = 1/3 + (1/3) * 0.85
    } else {
      pct = 2/3 + (1/3) * 0.85
    }
    pct = Math.max(0, Math.min(1, pct))
    const m = path.getPointAtLength(len * pct)
    setMPos({ x: m.x, y: m.y })
  }, [score, total])

  useEffect(() => {
    if (pathLen > 0 && !drawn) {
      const id = requestAnimationFrame(() => requestAnimationFrame(() => setDrawn(true)))
      return () => cancelAnimationFrame(id)
    }
  }, [pathLen, drawn])

  // Determine which tier index is "reached" based on maturity
  const reachedIndex = maturity === "Trusted" ? 2 : maturity === "Ready to Optimize" ? 1 : 0

  // Fixed Y positions for bottom-aligned labels
  const labelY1 = 205  // tier name
  const labelY2 = 218  // tagline

  return (
    <div className="w-full" role="img" aria-label={`Maturity curve: score ${score}/${total}, level ${maturity}`}>
      <svg viewBox="0 0 800 230" className="w-full">
        <defs>
          <filter id="mc-glow"><feGaussianBlur stdDeviation="6" /></filter>

          {/* Gradient stroke for the S-curve line */}
          <linearGradient id="mc-stroke" x1="60" y1="0" x2="740" y2="0" gradientUnits="userSpaceOnUse">
            <stop offset="0%"   stopColor="#ef4444" />
            <stop offset="33%"  stopColor="#3b82f6" />
            <stop offset="67%"  stopColor="#10b981" />
            <stop offset="100%" stopColor="#10b981" />
          </linearGradient>

          {/* Clip paths for each tier's horizontal zone */}
          {zoneBounds.length === 4 && TIERS.map((tier, i) => (
            <clipPath key={tier.label} id={`mc-zone-${i}`}>
              <rect x={zoneBounds[i]} y="0" width={zoneBounds[i + 1] - zoneBounds[i]} height="230" />
            </clipPath>
          ))}
        </defs>

        {/* Hidden path for measurement */}
        <path ref={measureRef} d={CURVE_D} fill="none" stroke="none" />

        {/* Faint guide lines */}
        <g className="text-muted">
          <line x1="60" y1="185" x2="740" y2="185" stroke="currentColor" strokeWidth="0.5" opacity="0.25" />
        </g>

        {pathLen > 0 && zoneBounds.length === 4 && (
          <>
            {/* 3 colored area fills under the curve, one per tier */}
            {TIERS.map((tier, i) => (
              <path
                key={tier.label}
                d={AREA_D}
                fill={tier.color}
                opacity={drawn ? 0.12 : 0}
                clipPath={`url(#mc-zone-${i})`}
                style={{ transition: `opacity 0.6s ease ${0.3 + i * 0.1}s` }}
              />
            ))}

            {/* Glow (blurred copy) */}
            <path
              d={CURVE_D} fill="none" stroke="url(#mc-stroke)"
              strokeWidth="5" strokeLinecap="round"
              opacity={drawn ? 0.25 : 0} filter="url(#mc-glow)"
              style={{ transition: "opacity 0.8s ease 0.4s" }}
            />

            {/* Main curve with draw-on animation */}
            <path
              d={CURVE_D} fill="none" stroke="url(#mc-stroke)"
              strokeWidth="2.5" strokeLinecap="round"
              strokeDasharray={pathLen}
              strokeDashoffset={drawn ? 0 : pathLen}
              style={{ transition: "stroke-dashoffset 1.4s cubic-bezier(0.4, 0, 0.2, 1)" }}
            />

            {/* ── Tier labels aligned to bottom of graph ── */}
            {tierPts.map((pt, i) => {
              const tier = TIERS[i]
              const reached = i <= reachedIndex

              return (
                <g
                  key={tier.label}
                  opacity={drawn ? 1 : 0}
                  style={{ transition: `opacity 0.4s ease ${0.7 + i * 0.15}s` }}
                >
                  <text
                    x={pt.x} y={labelY1} textAnchor="middle"
                    fill={reached ? tier.color : "currentColor"}
                    className={reached ? undefined : "text-muted"}
                    fontSize="11" fontWeight="600"
                    fontFamily="system-ui, -apple-system, sans-serif"
                  >
                    {tier.label}
                  </text>
                  <text
                    x={pt.x} y={labelY2} textAnchor="middle"
                    fill="currentColor" className="text-muted"
                    fontSize="9" opacity="0.8"
                    fontFamily="system-ui, -apple-system, sans-serif"
                  >
                    {tier.tagline}
                  </text>
                </g>
              )
            })}

            {/* ── "You Are Here" marker ── */}
            <g opacity={drawn ? 1 : 0} style={{ transition: "opacity 0.5s ease 1.4s" }}>
              <circle cx={mPos.x} cy={mPos.y} r={12} fill="currentColor" className="text-primary" opacity="0.05">
                <animate attributeName="r"       values="10;18;10"       dur="2.5s" repeatCount="indefinite" />
                <animate attributeName="opacity"  values="0.08;0.01;0.08" dur="2.5s" repeatCount="indefinite" />
              </circle>
              <circle cx={mPos.x} cy={mPos.y} r={8} fill="currentColor" className="text-primary" opacity="0.1">
                <animate attributeName="r"       values="7;12;7"         dur="2.5s" repeatCount="indefinite" />
                <animate attributeName="opacity"  values="0.12;0.02;0.12" dur="2.5s" repeatCount="indefinite" />
              </circle>
              <circle cx={mPos.x} cy={mPos.y} r={4.5} fill="currentColor" className="text-primary" />
              <text
                x={mPos.x}
                y={mPos.y - 16}
                textAnchor="middle" fill="currentColor" className="text-primary"
                fontSize="10" fontWeight="700" letterSpacing="0.5"
                fontFamily="system-ui, -apple-system, sans-serif"
              >
                YOU ARE HERE
              </text>
            </g>
          </>
        )}
      </svg>
    </div>
  )
}
