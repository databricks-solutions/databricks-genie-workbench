import { useState } from "react"
import { BarChart, Bar, XAxis, YAxis, Tooltip, Cell, ResponsiveContainer } from "recharts"
import { ChevronDown, ChevronUp } from "lucide-react"
import type { GSOStageEvent } from "@/types"

interface StageTimelineProps {
  stages: GSOStageEvent[]
}

function shortenStageName(stage: string): string {
  return stage
    .replace(/_STARTED$|_COMPLETE$|_COMPLETED$|_FAILED$/i, "")
    .replace(/_/g, " ")
    .split(" ")
    .map((w) => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase())
    .join(" ")
}

function formatStatusSuffix(status?: string): string {
  if (!status) return ""
  const s = status.toLowerCase()
  if (s === "complete" || s === "completed") return "Done"
  if (s === "failed" || s === "error") return "Failed"
  if (s === "started" || s === "running") return "Running"
  if (s === "rolled_back") return "Rolled Back"
  return status
}

function getStatusColor(status?: string): string {
  if (!status) return "#94a3b8"
  const s = status.toLowerCase()
  if (s === "complete" || s === "completed") return "#22c55e"
  if (s === "failed" || s === "error") return "#ef4444"
  if (s === "started" || s === "running") return "#3b82f6"
  if (s === "rolled_back") return "#f59e0b"
  return "#94a3b8"
}

function formatDateTime(iso?: string | null): string {
  if (!iso) return "\u2014"
  try {
    return new Date(iso).toLocaleString()
  } catch {
    return iso
  }
}

function CustomTooltip({ active, payload }: { active?: boolean; payload?: Array<{ payload: ChartItem }> }) {
  if (!active || !payload?.[0]) return null
  const p = payload[0].payload
  return (
    <div className="rounded-lg border border-default bg-surface p-3 shadow-md text-xs space-y-1 min-w-[200px]">
      <div><span className="text-muted">Stage:</span> <span className="font-medium">{p.stage ?? "Unknown"}</span></div>
      <div><span className="text-muted">Status:</span> <span className="font-medium capitalize">{p.status ?? "\u2014"}</span></div>
      <div><span className="text-muted">Duration:</span> <span className="font-mono">{p.durationSeconds}s</span></div>
      <div><span className="text-muted">Started:</span> <span className="font-mono">{formatDateTime(p.startedAt)}</span></div>
      <div><span className="text-muted">Completed:</span> <span className="font-mono">{formatDateTime(p.completedAt)}</span></div>
    </div>
  )
}

interface ChartItem {
  shortStage: string
  stage: string
  status: string
  durationSeconds: number
  startedAt: string | null
  completedAt: string | null
}

export function StageTimeline({ stages }: StageTimelineProps) {
  const [open, setOpen] = useState(stages.length > 0)

  const rawData = stages
    .map((e) => {
      let duration = e.durationSeconds
      if ((duration == null || duration <= 0) && e.startedAt && e.completedAt) {
        duration = (new Date(e.completedAt).getTime() - new Date(e.startedAt).getTime()) / 1000
      }
      return {
        stage: e.stage,
        status: e.status,
        shortStage: e.stage ? shortenStageName(e.stage) : "Unknown",
        durationSeconds: duration ?? 0,
        startedAt: e.startedAt ?? null,
        completedAt: e.completedAt ?? null,
      }
    })
    .filter((e) => e.durationSeconds > 0)

  // Deduplicate labels
  const nameCounts = new Map<string, number>()
  for (const d of rawData) nameCounts.set(d.shortStage, (nameCounts.get(d.shortStage) ?? 0) + 1)
  const chartData: ChartItem[] = rawData.map((d) => ({
    ...d,
    shortStage:
      (nameCounts.get(d.shortStage) ?? 1) > 1
        ? `${d.shortStage} (${formatStatusSuffix(d.status)})`
        : d.shortStage,
  }))

  const chartHeight = Math.max(300, chartData.length * 32)

  return (
    <div className="rounded-xl border border-default overflow-hidden">
      <div className="flex items-center justify-between px-6 py-3">
        <h3 className="text-sm font-semibold text-primary">Stage Timeline</h3>
        <button
          onClick={() => setOpen(!open)}
          className="flex items-center gap-1 text-xs text-muted hover:text-primary transition-colors"
        >
          {open ? (
            <>Hide stage timeline <ChevronUp className="h-3.5 w-3.5" /></>
          ) : (
            <>Show stage timeline <ChevronDown className="h-3.5 w-3.5" /></>
          )}
        </button>
      </div>

      {open && (
        <div className="px-6 pb-4">
          {chartData.length === 0 ? (
            <p className="text-sm text-muted py-6 text-center">No timeline data available</p>
          ) : (
            <ResponsiveContainer width="100%" height={chartHeight}>
              <BarChart
                layout="vertical"
                data={chartData}
                margin={{ top: 5, right: 30, left: 20, bottom: 5 }}
              >
                <XAxis type="number" dataKey="durationSeconds" tick={{ fontSize: 11 }} />
                <YAxis
                  type="category"
                  dataKey="shortStage"
                  width={180}
                  tick={{ fontSize: 11 }}
                />
                <Tooltip content={<CustomTooltip />} />
                <Bar dataKey="durationSeconds" radius={[0, 4, 4, 0]}>
                  {chartData.map((entry, index) => (
                    <Cell key={index} fill={getStatusColor(entry.status)} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>
      )}
    </div>
  )
}
