import { useMemo } from "react"
import type { GSOAttempt } from "@/types"
import {
  buildLadderModel,
  PATCH_ATTEMPT_COLOR,
  PRE_LOOP_ENRICHMENT_COLOR,
  type LadderRung,
} from "@/components/auto-optimize/cockpit"

interface AttemptLadderProps {
  baselineAccuracy: number | null | undefined
  attempts: GSOAttempt[]
  targetUnit: number | null | undefined
}

// Plot geometry (viewBox units; the SVG scales to its container width).
const W = 660
const H = 300
const PAD = { top: 18, right: 64, bottom: 40, left: 46 }
const PLOT_W = W - PAD.left - PAD.right
const PLOT_H = H - PAD.top - PAD.bottom
const INSET = 14

// The Attempt Ladder (the signature element, arch §7.5 / Phase 12). Re-bases
// the per-iteration score chart onto ATTEMPTS: a best-so-far champion staircase
// climbing toward a gold target summit line, over a faint baseline floor.
// Markers are colored by mode, filled when accepted and hollow when rolled back.
export function AttemptLadder({ baselineAccuracy, attempts, targetUnit }: AttemptLadderProps) {
  const model = useMemo(
    () => buildLadderModel({ baselineAccuracy, attempts, targetUnit }),
    [baselineAccuracy, attempts, targetUnit],
  )
  const { rungs, baselineFloor, summit, yMin, yMax } = model

  const n = rungs.length
  const xOf = (i: number) =>
    n <= 1 ? PAD.left + PLOT_W / 2 : PAD.left + INSET + (i / (n - 1)) * (PLOT_W - 2 * INSET)
  const yOf = (v: number) => PAD.top + (1 - (v - yMin) / (yMax - yMin)) * PLOT_H

  // Champion staircase path (step-through best-so-far, monotone). Cheap to
  // compute inline — it depends on the live x/y scales for this render.
  const staircasePts = rungs
    .map((r, i) => ({ i, y: r.bestSoFar }))
    .filter((p): p is { i: number; y: number } => p.y != null)
  let staircase = ""
  if (staircasePts.length > 0) {
    staircase = `M ${xOf(staircasePts[0].i)} ${yOf(staircasePts[0].y)}`
    for (let k = 1; k < staircasePts.length; k++) {
      staircase += ` L ${xOf(staircasePts[k].i)} ${yOf(staircasePts[k - 1].y)} L ${xOf(staircasePts[k].i)} ${yOf(staircasePts[k].y)}`
    }
  }

  const rolledBackCount = rungs.filter((r) => r.rolledBack).length

  return (
    <div className="rounded-xl border border-default p-4">
      <div className="mb-2 flex items-center justify-between">
        <h3 className="text-sm font-semibold text-primary">Attempt Ladder</h3>
        <Legend />
      </div>

      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="w-full"
        role="img"
        aria-label="Champion accuracy staircase across optimization attempts"
        preserveAspectRatio="xMidYMid meet"
      >
        {/* Y axis bounds */}
        <text x={PAD.left - 6} y={yOf(yMax) + 3} textAnchor="end" className="fill-muted" fontSize={10}>
          {yMax}%
        </text>
        <text x={PAD.left - 6} y={yOf(yMin) + 3} textAnchor="end" className="fill-muted" fontSize={10}>
          {yMin}%
        </text>

        {/* Faint baseline floor */}
        {baselineFloor != null && (
          <>
            <line
              x1={PAD.left}
              x2={W - PAD.right}
              y1={yOf(baselineFloor)}
              y2={yOf(baselineFloor)}
              stroke="#9ca3af"
              strokeWidth={1}
              strokeDasharray="2 3"
              opacity={0.6}
            />
            <text x={PAD.left} y={yOf(baselineFloor) - 4} className="fill-muted" fontSize={9}>
              baseline {baselineFloor.toFixed(1)}%
            </text>
          </>
        )}

        {/* Gold target summit line */}
        {summit != null && (
          <>
            <line
              x1={PAD.left}
              x2={W - PAD.right}
              y1={yOf(summit)}
              y2={yOf(summit)}
              stroke="#d4a017"
              strokeWidth={1.5}
              strokeDasharray="5 4"
            />
            <text
              x={W - PAD.right + 4}
              y={yOf(summit) + 3}
              className="fill-amber-600 dark:fill-amber-400"
              fontSize={10}
              fontWeight={600}
            >
              target {summit.toFixed(0)}%
            </text>
          </>
        )}

        {/* Champion staircase */}
        {staircase && (
          <path d={staircase} fill="none" stroke="#6366f1" strokeWidth={2.5} strokeLinejoin="round" />
        )}

        {/* Per-attempt markers + x labels */}
        {rungs.map((r, i) => (
          <RungMarker key={r.key} rung={r} x={xOf(i)} yOf={yOf} />
        ))}
      </svg>

      {/* Rollback note */}
      <div className="mt-1 space-y-0.5">
        {rolledBackCount > 0 && (
          <p className="text-[11px] text-muted">
            {rolledBackCount} attempt{rolledBackCount === 1 ? "" : "s"} rolled back (hollow markers
            drop below the champion staircase).
          </p>
        )}
      </div>
    </div>
  )
}

function RungMarker({
  rung,
  x,
  yOf,
}: {
  rung: LadderRung
  x: number
  yOf: (v: number) => number
}) {
  const labelY = H - PAD.bottom + 14
  return (
    <g>
      {rung.markerY != null && (
        <>
          {rung.isChampion && (
            <circle cx={x} cy={yOf(rung.markerY)} r={9} fill="none" stroke="#6366f1" strokeWidth={1.5} />
          )}
          <circle
            cx={x}
            cy={yOf(rung.markerY)}
            r={5}
            fill={rung.rolledBack ? "var(--color-surface, #ffffff)" : rung.color}
            stroke={rung.color}
            strokeWidth={2}
          />
          {rung.isChampion && (
            <text x={x} y={yOf(rung.markerY) - 13} textAnchor="middle" fontSize={11} fill="#6366f1">
              ★
            </text>
          )}
        </>
      )}
      <text x={x} y={labelY} textAnchor="middle" className="fill-muted" fontSize={10}>
        {rung.shortLabel}
      </text>
    </g>
  )
}

function Legend() {
  return (
    <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-[10px] text-muted">
      <span className="flex items-center gap-1">
        <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ backgroundColor: PATCH_ATTEMPT_COLOR }} />
        patch attempt
      </span>
      <span className="flex items-center gap-1">
        <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ backgroundColor: PRE_LOOP_ENRICHMENT_COLOR }} />
        pre-loop enrichment
      </span>
      <span className="flex items-center gap-1">
        <span className="inline-block h-2.5 w-2.5 rounded-full border-2 border-muted bg-transparent" />
        rolled back
      </span>
      <span className="flex items-center gap-1">
        <span style={{ color: "#6366f1" }}>★</span>
        champion
      </span>
    </div>
  )
}
