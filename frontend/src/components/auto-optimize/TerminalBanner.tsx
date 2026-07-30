import { CheckCircle2, XCircle, AlertTriangle, Flag } from "lucide-react"
import type { GSOTerminalReason } from "@/types"
import { classifyTerminal, toAccuracyPct, type TerminalTone } from "@/components/auto-optimize/cockpit"

interface TerminalBannerProps {
  status?: string | null
  terminalReason?: GSOTerminalReason | null
  /** Authoritative published flag from the publish record; null if absent. */
  published?: boolean | null
  publishOutcome?: string | null
  benchmarkUnrepairable?: boolean
  /** Champion accuracy (0–100) to surface on a published banner. */
  championAccuracy?: number | null
  concerns?: string[]
}

const TONE_STYLES: Record<TerminalTone, { wrap: string; icon: string }> = {
  success: {
    wrap: "border-emerald-300 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950/20",
    icon: "text-emerald-600 dark:text-emerald-400",
  },
  danger: {
    wrap: "border-red-300 bg-red-50 dark:border-red-800 dark:bg-red-950/20",
    icon: "text-red-600 dark:text-red-400",
  },
  warning: {
    wrap: "border-amber-300 bg-amber-50 dark:border-amber-800 dark:bg-amber-950/20",
    icon: "text-amber-600 dark:text-amber-400",
  },
  info: {
    wrap: "border-blue-300 bg-blue-50 dark:border-blue-800 dark:bg-blue-950/20",
    icon: "text-blue-600 dark:text-blue-400",
  },
  secondary: {
    wrap: "border-default bg-surface",
    icon: "text-muted",
  },
}

// Terminal banner (Phase 12) — keyed on the typed terminal reason, clearly
// distinguishing "stopped — nothing published" (evaluation/config/loop
// failures, benchmark-unrepairable, or budget) from "champion published"
// (TARGET_REACHED / MAX_ATTEMPTS).
export function TerminalBanner({
  status,
  terminalReason,
  published,
  publishOutcome,
  benchmarkUnrepairable,
  championAccuracy,
  concerns,
}: TerminalBannerProps) {
  const cls = classifyTerminal({
    status,
    terminalReason,
    published,
    publishOutcome,
    benchmarkUnrepairable,
  })
  if (!cls) return null

  const tone = TONE_STYLES[cls.tone]
  const Icon = cls.published
    ? CheckCircle2
    : cls.tone === "danger"
      ? XCircle
      : cls.tone === "warning"
        ? AlertTriangle
        : Flag
  const champ = toAccuracyPct(championAccuracy)

  return (
    <div className={`rounded-xl border px-4 py-3 ${tone.wrap}`}>
      <div className="flex items-start gap-3">
        <Icon className={`mt-0.5 h-5 w-5 shrink-0 ${tone.icon}`} />
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="text-sm font-semibold text-primary">{cls.title}</h3>
            <span
              className={`rounded-full px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide ${
                cls.published
                  ? "bg-emerald-100 text-emerald-700 dark:bg-emerald-900/40 dark:text-emerald-300"
                  : "bg-elevated text-muted"
              }`}
            >
              {cls.published ? "Published" : "Not published"}
            </span>
            {cls.published && champ != null && (
              <span className="text-xs text-muted">champion {champ.toFixed(1)}%</span>
            )}
          </div>
          <p className="mt-0.5 text-xs text-muted">{cls.detail}</p>
          {concerns && concerns.length > 0 && (
            <ul className="mt-1.5 space-y-0.5">
              {concerns.map((c, i) => (
                <li key={i} className="flex items-start gap-1.5 text-xs text-amber-600 dark:text-amber-400">
                  <AlertTriangle className="mt-0.5 h-3 w-3 shrink-0" />
                  <span>{c}</span>
                </li>
              ))}
            </ul>
          )}
        </div>
      </div>
    </div>
  )
}
