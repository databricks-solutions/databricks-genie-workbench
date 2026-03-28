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
}

const LEVER_NAMES: Record<number, string> = {
  1: "Tables & Columns",
  2: "Metric Views",
  3: "TVFs",
  4: "Joins",
  5: "Instructions",
  6: "SQL Expressions",
}

interface ChartPoint {
  iteration: number
  lever: number | null
  leverLabel: string
  accuracy: number
  totalQuestions: number
}

function buildChartData(iterations: GSOIterationResult[]): ChartPoint[] {
  return iterations
    .filter((it) => it.eval_scope === "full")
    .filter((it) => it.total_questions > 0 || it.iteration === 0)
    .sort((a, b) => a.iteration - b.iteration)
    .map((it) => ({
      iteration: it.iteration,
      lever: it.lever,
      leverLabel:
        it.iteration === 0
          ? "Baseline"
          : it.lever != null && it.lever > 0
            ? (LEVER_NAMES[it.lever] ?? `Lever ${it.lever}`)
            : `Iter ${it.iteration}`,
      accuracy: Number(it.overall_accuracy) <= 1 ? Number(it.overall_accuracy) * 100 : Number(it.overall_accuracy),
      totalQuestions: it.total_questions,
    }))
}

function CustomTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: ChartPoint }> }) {
  if (!active || !payload?.[0]) return null
  const pt = payload[0].payload
  return (
    <div className="rounded-lg border border-default bg-surface p-3 shadow-md text-xs space-y-1">
      <div><span className="text-muted">Iteration:</span> {pt.iteration}</div>
      <div><span className="text-muted">Accuracy:</span> {pt.accuracy.toFixed(1)}%</div>
      <div><span className="text-muted">Lever:</span> {pt.leverLabel}</div>
      <div><span className="text-muted">Questions:</span> {pt.totalQuestions}</div>
    </div>
  )
}

export function IterationChart({ iterations }: IterationChartProps) {
  const chartData = useMemo(() => buildChartData(iterations), [iterations])

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
      <ResponsiveContainer width="100%" height={280}>
        <LineChart data={chartData} margin={{ top: 5, right: 10, left: 10, bottom: 0 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="var(--color-border, #e5e7eb)" />
          <XAxis
            dataKey="iteration"
            tick={{ fontSize: 11 }}
            tickFormatter={(val) => {
              const pt = chartData.find((p) => p.iteration === val)
              return pt?.leverLabel ?? (val === 0 ? "Baseline" : String(val))
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
