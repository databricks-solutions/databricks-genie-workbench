import type { GSOStageEvent } from "@/types"

interface ActivityLogProps {
  stages: GSOStageEvent[]
}

function formatDuration(seconds: number | null): string {
  if (seconds == null) return "\u2014"
  const m = Math.floor(seconds / 60)
  const s = Math.round(seconds % 60)
  if (m === 0) return `${s}s`
  return `${m}m ${s}s`
}

function formatDateTime(iso: string | null): string {
  if (!iso) return "\u2014"
  try {
    return new Date(iso).toLocaleString("en-US", {
      month: "short",
      day: "numeric",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
    })
  } catch {
    return iso
  }
}

function getStatusColor(status: string): string {
  const s = status.toLowerCase()
  if (s === "completed" || s === "complete") return "bg-emerald-500"
  if (s === "failed" || s === "error") return "bg-red-500"
  if (s === "running" || s === "started") return "bg-blue-500"
  if (s === "rolled_back") return "bg-amber-500"
  return "bg-gray-400"
}

export function ActivityLog({ stages }: ActivityLogProps) {
  const sorted = [...stages].sort((a, b) => {
    const aTime = a.startedAt ? new Date(a.startedAt).getTime() : 0
    const bTime = b.startedAt ? new Date(b.startedAt).getTime() : 0
    return bTime - aTime // most recent first
  })

  return (
    <div className="rounded-xl border border-default p-6">
      <h3 className="text-sm font-semibold text-primary mb-4">Stage Events</h3>

      {sorted.length === 0 ? (
        <p className="text-sm text-muted text-center py-6">No stage events available</p>
      ) : (
        <div className="space-y-3">
          {sorted.map((event, i) => (
            <div key={i} className="flex items-start gap-3 py-2 border-b border-default last:border-0">
              <div className="mt-1.5 shrink-0">
                <div className={`h-2.5 w-2.5 rounded-full ${getStatusColor(event.status)}`} />
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-0.5">
                  <span className="text-sm font-medium text-primary">{event.stage}</span>
                  <span className={`text-xs font-medium capitalize ${
                    event.status.toLowerCase() === "completed" || event.status.toLowerCase() === "complete"
                      ? "text-emerald-600 dark:text-emerald-400"
                      : event.status.toLowerCase() === "failed"
                        ? "text-red-600 dark:text-red-400"
                        : event.status.toLowerCase() === "running"
                          ? "text-blue-600 dark:text-blue-400"
                          : "text-muted"
                  }`}>
                    {event.status}
                  </span>
                </div>
                <div className="flex items-center gap-3 text-xs text-muted">
                  <span>{formatDateTime(event.startedAt)}</span>
                  {event.durationSeconds != null && (
                    <span className="font-mono">{formatDuration(event.durationSeconds)}</span>
                  )}
                </div>
                {event.summary && (
                  <p className="text-xs text-muted mt-1">{event.summary}</p>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
