import { useState } from "react"
import { Badge } from "@/components/ui/badge"
import {
  Circle,
  Loader2,
  CheckCircle2,
  XCircle,
  ChevronDown,
  Settings2,
  FlaskConical,
  Sparkles,
  Wrench,
  BarChart3,
  Rocket,
} from "lucide-react"

interface PipelineStepCardProps {
  stepNumber: number
  name: string
  status: string
  durationSeconds: number | null
  description: string
  summary?: string | null
  children?: React.ReactNode
}

const stepIcons: Record<number, React.ReactNode> = {
  1: <Settings2 className="h-4 w-4" />,
  2: <FlaskConical className="h-4 w-4" />,
  3: <Sparkles className="h-4 w-4" />,
  4: <Wrench className="h-4 w-4" />,
  5: <BarChart3 className="h-4 w-4" />,
  6: <Rocket className="h-4 w-4" />,
}

function StatusIndicator({ status }: { status: string }) {
  if (status === "running") {
    return (
      <div className="relative flex h-8 w-8 items-center justify-center">
        <div className="absolute inset-0 animate-ping rounded-full bg-blue-500/20" />
        <div className="relative flex h-8 w-8 items-center justify-center rounded-full border-2 border-blue-500 bg-blue-500/10">
          <Loader2 className="h-4 w-4 animate-spin text-blue-500" />
        </div>
      </div>
    )
  }
  if (status === "completed") {
    return (
      <div className="flex h-8 w-8 items-center justify-center rounded-full border-2 border-emerald-500 bg-emerald-500/10">
        <CheckCircle2 className="h-4 w-4 text-emerald-500" />
      </div>
    )
  }
  if (status === "failed") {
    return (
      <div className="flex h-8 w-8 items-center justify-center rounded-full border-2 border-red-500 bg-red-500/10">
        <XCircle className="h-4 w-4 text-red-500" />
      </div>
    )
  }
  return (
    <div className="flex h-8 w-8 items-center justify-center rounded-full border-2 border-default bg-elevated/50">
      <Circle className="h-4 w-4 text-muted" />
    </div>
  )
}

function formatDuration(seconds: number | null): string {
  if (seconds == null) return "\u2014"
  const m = Math.floor(seconds / 60)
  const s = Math.round(seconds % 60)
  if (m === 0) return `${s}s`
  return s > 0 ? `${m}m ${s}s` : `${m}m`
}

export function PipelineStepCard({
  stepNumber,
  name,
  status,
  durationSeconds,
  description,
  summary,
  children,
}: PipelineStepCardProps) {
  const [open, setOpen] = useState(status === "running" || status === "failed")
  const hasExpandableContent = summary || children
  const isExpandable = hasExpandableContent && (status === "completed" || status === "running" || status === "failed")

  const borderColor =
    status === "running"
      ? "border-blue-500/30 shadow-md shadow-blue-500/5"
      : status === "failed"
        ? "border-red-500/30"
        : status === "completed"
          ? "border-emerald-500/20"
          : "border-default"

  return (
    <div className={`rounded-xl border transition-all duration-300 ${borderColor} bg-surface overflow-hidden`}>
      <button
        type="button"
        onClick={() => isExpandable && setOpen(!open)}
        className={`flex w-full items-start gap-4 p-4 text-left ${isExpandable ? "cursor-pointer" : "cursor-default"}`}
      >
        <StatusIndicator status={status} />

        <div className="flex min-w-0 flex-1 flex-col gap-1">
          <div className="flex items-center gap-2">
            <span className="flex items-center gap-1 text-xs font-medium text-muted">
              {stepIcons[stepNumber]}
              Step {stepNumber}
            </span>
            <span className={`text-sm font-semibold ${status === "pending" ? "text-muted" : "text-primary"}`}>
              {name}
            </span>
          </div>
          <p className="text-xs leading-relaxed text-muted">{description}</p>
          {summary && status !== "pending" && (
            <p className="text-xs font-medium text-muted">{summary}</p>
          )}
        </div>

        <div className="flex shrink-0 items-center gap-2">
          {durationSeconds != null && (
            <span className="text-xs tabular-nums text-muted">{formatDuration(durationSeconds)}</span>
          )}
          <Badge
            variant={
              status === "completed" ? "success"
                : status === "failed" ? "danger"
                : status === "running" ? "info"
                : "secondary"
            }
          >
            {status === "running" && <Loader2 className="mr-1 h-3 w-3 animate-spin" />}
            {status === "completed" ? "Complete"
              : status === "failed" ? "Failed"
              : status === "running" ? "Running"
              : "Pending"}
          </Badge>
          {isExpandable && (
            <ChevronDown
              className={`h-4 w-4 text-muted transition-transform duration-200 ${open ? "rotate-180" : ""}`}
            />
          )}
        </div>
      </button>

      {open && hasExpandableContent && (
        <div className="border-t border-dashed border-default px-4 py-3 space-y-3">
          {children}
        </div>
      )}
    </div>
  )
}
