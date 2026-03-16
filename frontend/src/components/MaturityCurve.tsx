/**
 * MaturityCurve — SVG S-curve visualization showing 4 maturity tiers
 * plus a bonus "Optimized" milestone at the end, with a pulsing "You Are Here" marker.
 * Theme-aware: uses currentColor + Tailwind classes for light/dark mode.
 */
import { useRef, useEffect, useState } from "react"

interface MaturityCurveProps {
  score: number              // 0-100
  maturity: string           // current tier label
  optimizationPoints: number // 0-20, from breakdown.optimized
}

/** 4 maturity tiers positioned along the first 80% of the curve. */
const TIERS = [
  { label: "Connected",  tagline: "Can Genie see my data?",            color: "#ef4444", threshold: 0,  pct: 0.20 },
  { label: "Configured", tagline: "Does Genie speak my language?",     color: "#eab308", threshold: 26, pct: 0.40 },
  { label: "Calibrated", tagline: "Are Genie's answers consistent?",   color: "#3b82f6", threshold: 51, pct: 0.60 },
  { label: "Trusted",    tagline: "Is Genie ready for everyone?",      color: "#10b981", threshold: 76, pct: 0.80 },
]

/** Bonus 5th milestone — Optimized — at the curve's end. */
const OPTIMIZED = {
  label: "Optimized",
  tagline: "Is Genie battle-tested?",
  color: "#a855f7", // purple — distinct from tier colors
  pct: 0.98,
}

// Compact S-curve: flat bottom-left → steep rise → flat top-right
const CURVE_D = "M 60,195 C 130,195 200,190 350,110 C 500,30 570,22 740,22"
const AREA_D  = `${CURVE_D} L 740,210 L 60,210 Z`

/** Build a 4-pointed sparkle path centered at (cx, cy) with given radius. */
function sparklePath(cx: number, cy: number, r: number): string {
  const ir = r * 0.35 // inner radius
  const pts: string[] = []
  for (let i = 0; i < 8; i++) {
    const angle = -Math.PI / 2 + (i * Math.PI) / 4
    const rad = i % 2 === 0 ? r : ir
    pts.push(`${cx + rad * Math.cos(angle)},${cy + rad * Math.sin(angle)}`)
  }
  return `M ${pts.join(" L ")} Z`
}

export function MaturityCurve({ score, maturity, optimizationPoints }: MaturityCurveProps) {
  const measureRef = useRef<SVGPathElement>(null)
  const [pathLen, setPathLen]   = useState(0)
  const [pts, setPts]           = useState<{ x: number; y: number }[]>([])
  const [optPt, setOptPt]       = useState<{ x: number; y: number } | null>(null)
  const [mPos, setMPos]         = useState({ x: 60, y: 195 })
  const [drawn, setDrawn]       = useState(false)

  useEffect(() => {
    const path = measureRef.current
    if (!path) return

    const len = path.getTotalLength()
    setPathLen(len)

    setPts(TIERS.map(t => {
      const p = path.getPointAtLength(len * t.pct)
      return { x: p.x, y: p.y }
    }))

    const op = path.getPointAtLength(len * OPTIMIZED.pct)
    setOptPt({ x: op.x, y: op.y })

    const pct = Math.max(0, Math.min(100, score)) / 100
    const m = path.getPointAtLength(len * pct)
    setMPos({ x: m.x, y: m.y })
  }, [score])

  useEffect(() => {
    if (pathLen > 0 && !drawn) {
      const id = requestAnimationFrame(() => requestAnimationFrame(() => setDrawn(true)))
      return () => cancelAnimationFrame(id)
    }
  }, [pathLen, drawn])

  // Hide "YOU ARE HERE" text when marker is too close to any milestone
  const allPts = optPt ? [...pts, optPt] : pts
  const showMarkerLabel = allPts.length > 0 && !allPts.some(
    p => Math.hypot(p.x - mPos.x, p.y - mPos.y) < 70
  )

  const optimizedReached = optimizationPoints >= 20

  return (
    <div className="w-full" role="img" aria-label={`Maturity curve: score ${score}, level ${maturity}`}>
      <svg viewBox="0 0 800 225" className="w-full">
        <defs>
          <filter id="mc-glow"><feGaussianBlur stdDeviation="6" /></filter>

          {/* Gradient now extends through purple for the Optimized zone */}
          <linearGradient id="mc-stroke" x1="60" y1="0" x2="740" y2="0" gradientUnits="userSpaceOnUse">
            <stop offset="0%"   stopColor="#ef4444" />
            <stop offset="25%"  stopColor="#eab308" />
            <stop offset="50%"  stopColor="#3b82f6" />
            <stop offset="75%"  stopColor="#10b981" />
            <stop offset="100%" stopColor="#a855f7" />
          </linearGradient>

          <linearGradient id="mc-area" x1="400" y1="22" x2="400" y2="210" gradientUnits="userSpaceOnUse">
            <stop offset="0%"   stopColor="#3b82f6" stopOpacity="0.08" />
            <stop offset="100%" stopColor="#ef4444" stopOpacity="0.01" />
          </linearGradient>
        </defs>

        {/* Hidden path for measurement */}
        <path ref={measureRef} d={CURVE_D} fill="none" stroke="none" />

        {/* Faint guide lines */}
        <g className="text-muted">
          <line x1="60" y1="195" x2="740" y2="195" stroke="currentColor" strokeWidth="0.5" opacity="0.25" />
          <line x1="60" y1="22"  x2="740" y2="22"  stroke="currentColor" strokeWidth="0.5" opacity="0.25" />
        </g>

        {pathLen > 0 && (
          <>
            {/* Area fill under curve */}
            <path
              d={AREA_D} fill="url(#mc-area)"
              opacity={drawn ? 1 : 0}
              style={{ transition: "opacity 0.8s ease 0.5s" }}
            />

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

            {/* ── 4 maturity-tier milestones ── */}
            {pts.map((pt, i) => {
              const tier = TIERS[i]
              const reached = score >= tier.threshold
              const labelsAbove = pt.y > 140
              const ly1 = labelsAbove ? pt.y - 18 : pt.y + 20
              const ly2 = labelsAbove ? pt.y - 32 : pt.y + 34

              return (
                <g
                  key={tier.label}
                  opacity={drawn ? 1 : 0}
                  style={{ transition: `opacity 0.4s ease ${0.7 + i * 0.15}s` }}
                >
                  {reached && (
                    <circle cx={pt.x} cy={pt.y} r={10}
                      fill="none" stroke={tier.color} strokeWidth="1" opacity="0.25"
                    />
                  )}
                  <circle cx={pt.x} cy={pt.y} r={5}
                    fill={reached ? tier.color : "currentColor"}
                    stroke={reached ? tier.color : "currentColor"}
                    strokeWidth="1.5"
                    className={reached ? undefined : "text-muted"}
                    opacity={reached ? 1 : 0.4}
                  />
                  <text
                    x={pt.x} y={ly1} textAnchor="middle"
                    fill={reached ? tier.color : "currentColor"}
                    className={reached ? undefined : "text-muted"}
                    fontSize="11" fontWeight="600"
                    fontFamily="system-ui, -apple-system, sans-serif"
                  >
                    {tier.label}
                  </text>
                  <text
                    x={pt.x} y={ly2} textAnchor="middle"
                    fill="currentColor" className="text-muted"
                    fontSize="9" opacity="0.8"
                    fontFamily="system-ui, -apple-system, sans-serif"
                  >
                    {tier.tagline}
                  </text>
                </g>
              )
            })}

            {/* ── Optimized bonus milestone (star + dashed ring) ── */}
            {optPt && (
              <g
                opacity={drawn ? 1 : 0}
                style={{ transition: "opacity 0.4s ease 1.3s" }}
              >
                {/* Dashed outer ring — always visible to hint at the bonus */}
                <circle cx={optPt.x} cy={optPt.y} r={12}
                  fill="none"
                  stroke={optimizedReached ? OPTIMIZED.color : "currentColor"}
                  className={optimizedReached ? undefined : "text-muted"}
                  strokeWidth="1"
                  strokeDasharray="3 2.5"
                  opacity={optimizedReached ? 0.5 : 0.3}
                />

                {/* Sparkle shape instead of a plain circle */}
                <path
                  d={sparklePath(optPt.x, optPt.y, 6)}
                  fill={optimizedReached ? OPTIMIZED.color : "currentColor"}
                  className={optimizedReached ? undefined : "text-muted"}
                  opacity={optimizedReached ? 1 : 0.35}
                />

                {/* Glow ring when reached */}
                {optimizedReached && (
                  <circle cx={optPt.x} cy={optPt.y} r={16}
                    fill="none" stroke={OPTIMIZED.color} strokeWidth="1" opacity="0.15"
                  />
                )}

                {/* Label */}
                <text
                  x={optPt.x} y={optPt.y + 20} textAnchor="middle"
                  fill={optimizedReached ? OPTIMIZED.color : "currentColor"}
                  className={optimizedReached ? undefined : "text-muted"}
                  fontSize="11" fontWeight="600"
                  fontFamily="system-ui, -apple-system, sans-serif"
                >
                  Optimized
                </text>
                <text
                  x={optPt.x} y={optPt.y + 34} textAnchor="middle"
                  fill="currentColor" className="text-muted"
                  fontSize="9" opacity="0.8"
                  fontFamily="system-ui, -apple-system, sans-serif"
                >
                  {OPTIMIZED.tagline}
                </text>
              </g>
            )}

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
              {showMarkerLabel && (
                <text
                  x={mPos.x}
                  y={mPos.y > 40 ? mPos.y - 18 : mPos.y + 26}
                  textAnchor="middle" fill="currentColor" className="text-primary"
                  fontSize="10" fontWeight="700" letterSpacing="0.5"
                  fontFamily="system-ui, -apple-system, sans-serif"
                >
                  YOU ARE HERE
                </text>
              )}
            </g>
          </>
        )}
      </svg>
    </div>
  )
}
