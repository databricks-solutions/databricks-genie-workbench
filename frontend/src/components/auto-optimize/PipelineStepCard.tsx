import { Badge } from "@/components/ui/badge"

interface PipelineStepCardProps {
  stepNumber: number
  name: string
  status: string
  durationSeconds: number | null
  description: string
}

const STATUS_STYLES: Record<string, { bg: string; text: string; extra?: string }> = {
  pending:   { bg: "bg-gray-500/10", text: "text-gray-400" },
  running:   { bg: "bg-blue-500/10",  text: "text-blue-400", extra: "animate-pulse" },
  completed: { bg: "bg-emerald-500/10", text: "text-emerald-400" },
  failed:    { bg: "bg-red-500/10",  text: "text-red-400" },
}

function formatDuration(seconds: number | null): string {
  if (seconds == null) return "—"
  const m = Math.floor(seconds / 60)
  const s = Math.round(seconds % 60)
  if (m === 0) return `${s}s`
  return `${m}m ${s}s`
}

export function PipelineStepCard({ stepNumber, name, status, durationSeconds, description }: PipelineStepCardProps) {
  const style = STATUS_STYLES[status] ?? STATUS_STYLES.pending

  return (
    <div className={`flex items-start gap-4 rounded-lg border border-default p-4 ${style.extra ?? ""}`}>
      {/* Step number circle */}
      <div className={`flex items-center justify-center w-8 h-8 rounded-full shrink-0 text-sm font-bold ${style.bg} ${style.text}`}>
        {stepNumber}
      </div>

      {/* Content */}
      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-2 mb-1">
          <span className="text-sm font-semibold text-primary">{name}</span>
          <Badge variant={status === "completed" ? "success" : status === "failed" ? "danger" : status === "running" ? "info" : "secondary"}>
            {status}
          </Badge>
          <span className="text-xs text-muted ml-auto">{formatDuration(durationSeconds)}</span>
        </div>
        <p className="text-xs text-muted">{description}</p>
      </div>
    </div>
  )
}
