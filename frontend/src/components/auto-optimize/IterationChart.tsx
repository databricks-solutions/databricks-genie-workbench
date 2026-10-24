import { useMemo } from "react"
import {
  CartesianGrid,
  Line,
  LineChart,
  ReferenceLine,
  XAxis,
  YAxis,
  Tooltip,
  ResponsiveContainer,
} from "recharts"
import type { GSOIterationResult } from "@/types"

interface IterationChartProps {
  iterations: GSOIterationResult[]
  metricViewAttachAccuracy?: number | null
}

const ATTACH_KEY = "0-mv"

const LEVER_NAMES: Record<number, string> = {
  1: "Tables & Columns",
  2: "Metric Views",
  3: "Table-Valued Functions",
  4: "Join Specifications",
  5: "Instructions & Examples",
  6: "SQL Expressions",
}

interface ChartPoint {
  // Categorical x key: the attach point shares iteration 0 with the baseline.
  key: string
  iteration: number
  lever: number | null
  leverLabel: string
  accuracy: number
  totalQuestions: number
  // GSO v2 Phase 6 — official counts (num_correct / num_questions drive
  // overall_accuracy; num_needs_review is surfaced distinctly).
  numCorrect: number | null
  numNeedsReview: number | null
}

function toPct(value: number): number {
  return value <= 1 ? value * 100 : value
}

// eslint-disable-next-line react-refresh/only-export-components
export function buildChartData(
  iterations: GSOIterationResult[],
  metricViewAttachAccuracy: number | null = null,
): ChartPoint[] {
  const points: ChartPoint[] = iterations
    .filter((it) => it.eval_scope === "full")
    .filter((it) => it.total_questions > 0 || it.iteration === 0)
    .sort((a, b) => a.iteration - b.iteration)
    .map((it) => ({
      key: String(it.iteration),
      iteration: it.iteration,
      lever: it.lever,
      leverLabel:
        it.iteration === 0
          ? "Baseline"
          : it.lever != null && it.lever > 0
            ? (LEVER_NAMES[it.lever] ?? `Lever ${it.lever}`)
            : `Iter ${it.iteration}`,
      accuracy: toPct(Number(it.overall_accuracy)),
      totalQuestions: it.num_questions ?? it.total_questions,
      numCorrect: it.num_correct ?? it.correct_count ?? null,
      numNeedsReview: it.num_needs_review ?? null,
    }))

  const baselineIdx = points.findIndex((p) => p.iteration === 0)
  if (baselineIdx >= 0 && metricViewAttachAccuracy != null && Number.isFinite(metricViewAttachAccuracy)) {
    points.splice(baselineIdx + 1, 0, {
      key: ATTACH_KEY,
      iteration: 0,
      lever: null,
      leverLabel: "Metric view",
      accuracy: toPct(metricViewAttachAccuracy),
      totalQuestions: points[baselineIdx].totalQuestions,
      numCorrect: null,
      numNeedsReview: null,
    })
  }
  return points
}

function CustomTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: ChartPoint }> }) {
  if (!active || !payload?.[0]) return null
  const pt = payload[0].payload
  return (
    <div className="rounded-lg border border-default bg-surface p-3 shadow-md text-xs space-y-1">
      <div><span className="text-muted">Iteration:</span> {pt.iteration}</div>
      <div><span className="text-muted">Accuracy:</span> {pt.accuracy.toFixed(1)}%</div>
      {pt.numCorrect != null && (
        <div><span className="text-muted">Correct:</span> {pt.numCorrect}/{pt.totalQuestions}</div>
      )}
      {pt.numNeedsReview != null && pt.numNeedsReview > 0 && (
        <div><span className="text-muted">Needs review:</span> {pt.numNeedsReview}</div>
      )}
      <div><span className="text-muted">Lever:</span> {pt.key === ATTACH_KEY ? "Metric view attach" : pt.leverLabel}</div>
    </div>
  )
}

export function IterationChart({ iterations, metricViewAttachAccuracy = null }: IterationChartProps) {
  const chartData = useMemo(
    () => buildChartData(iterations, metricViewAttachAccuracy),
    [iterations, metricViewAttachAccuracy],
  )

  if (chartData.length < 2) {
    return (
      <div className="rounded-xl border border-default p-6">
        <h3 className="text-sm font-semibold text-primary mb-3">Score Progression</h3>
        <div className="flex h-[250px] items-center justify-center text-sm text-muted">
          Not enough data to show progression
        </div>
      </div>
    )
  }

  const accuracies = chartData.map((p) => p.accuracy)
  const dataMin = Math.min(...accuracies)
  const dataMax = Math.max(...accuracies)
  const yMin = Math.max(0, Math.floor((dataMin - 10) / 5) * 5)
  const yMax = Math.min(100, Math.ceil((dataMax + 10) / 5) * 5)

  return (
    <div className="rounded-xl border border-default p-6">
      <h3 className="text-sm font-semibold text-primary mb-3">Score Progression</h3>
      <ResponsiveContainer width="100%" height={280} initialDimension={{ width: 480, height: 280 }}>
        <LineChart data={chartData} margin={{ top: 5, right: 10, left: 10, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border, #e5e7eb)" />
          <XAxis
            dataKey="key"
            tick={{ fontSize: 11 }}
            tickFormatter={(val) => {
              const pt = chartData.find((p) => p.key === val)
              return pt?.leverLabel ?? (val === "0" ? "Baseline" : String(val))
            }}
          />
          <YAxis domain={[yMin, yMax]} tickFormatter={(v) => `${v}%`} tick={{ fontSize: 11 }} />
          <ReferenceLine y={80} stroke="#9ca3af" strokeDasharray="4 4" />
          <Tooltip content={<CustomTooltip />} />
          <Line
            type="monotone"
            dataKey="accuracy"
            stroke="#6366f1"
            strokeWidth={2}
            dot={{ r: 4, fill: "#6366f1" }}
            activeDot={{ r: 6 }}
          />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
