import { Fragment, useMemo } from "react"
import { Info } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import {
  Table,
  TableHeader,
  TableBody,
  TableRow,
  TableHead,
  TableCell,
} from "@/components/ui/table"
import type { GSOAttempt } from "@/types"
import {
  attemptModeColor,
  attemptModeLabel,
  buildLedgerModel,
  type LedgerRow,
} from "@/components/auto-optimize/cockpit"

interface AttemptLedgerProps {
  baselineAccuracy: number | null | undefined
  attempts: GSOAttempt[]
  /** Baseline is champion only when explicitly nothing beat it — never idxmax. */
  baselineIsChampion?: boolean
}

function fmtPct(v: number | null): string {
  return v == null ? "—" : `${v.toFixed(1)}%`
}

function fmtDelta(v: number | null): string {
  if (v == null) return "—"
  const sign = v > 0 ? "+" : ""
  return `${sign}${v.toFixed(1)}%`
}

// The Attempt Ledger (Phase 12) — the tabular companion to the ladder. One row
// per baseline / patch attempt. The champion ★ is read from the
// explicit isChampion flag (never re-derived). The highest-accuracy row is
// highlighted separately; when it diverges from the champion, the rollback/
// rejection reason is shown inline so a higher-but-rolled-back attempt is
// explained, not hidden (§5).
export function AttemptLedger({ baselineAccuracy, attempts, baselineIsChampion }: AttemptLedgerProps) {
  const rows = useMemo(
    () => buildLedgerModel({ baselineAccuracy, attempts, baselineIsChampion }),
    [baselineAccuracy, attempts, baselineIsChampion],
  )

  return (
    <div className="rounded-xl border border-default p-4">
      <h3 className="mb-2 text-sm font-semibold text-primary">Attempt Ledger</h3>
      {/* Four columns fit the half-width card without horizontal scroll: the
          champion ★ and HIGHEST marker live inline in the Attempt cell (no
          separate Champion column), and Δ-vs-baseline sits under the accuracy
          number rather than in its own wide column. */}
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Attempt</TableHead>
            <TableHead className="text-right">Accuracy</TableHead>
            <TableHead>Mode</TableHead>
            <TableHead>Decision</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <Fragment key={row.key}>
              <LedgerTableRow row={row} />
              {row.divergenceReason && (
                <TableRow className="hover:bg-transparent">
                  <TableCell colSpan={4} className="pt-0">
                    <p className="flex items-start gap-1.5 text-xs text-amber-600">
                      <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                      <span>
                        Highest accuracy, but not the champion: {row.divergenceReason}
                      </span>
                    </p>
                  </TableCell>
                </TableRow>
              )}
            </Fragment>
          ))}
        </TableBody>
      </Table>
    </div>
  )
}

function LedgerTableRow({ row }: { row: LedgerRow }) {
  const deltaClass =
    row.deltaVsBaseline == null || row.deltaVsBaseline === 0
      ? "text-muted"
      : row.deltaVsBaseline > 0
        ? "text-emerald-600 dark:text-emerald-400"
        : "text-red-500"
  return (
    <TableRow
      // Semi-transparent tint (not bg-amber-50): this project drives dark mode
      // via CSS-variable tokens with NO Tailwind `dark:` variant, so a solid
      // light shade would stay light — and unreadable — in dark mode. An
      // alpha tint layers over whatever surface is beneath it, working in both.
      className={row.isHighest ? "bg-amber-500/10" : undefined}
      data-highest={row.isHighest || undefined}
    >
      {/* Attempt: label + sublabel stacked (never wraps mid-badge), with the
          champion ★ and HIGHEST marker inline — no separate columns for them. */}
      <TableCell>
        <div className="flex items-start gap-1.5">
          {row.isChampion && (
            <span title="Champion configuration" className="mt-0.5 shrink-0 text-sm leading-none text-indigo-500">
              ★
            </span>
          )}
          <div className="min-w-0">
            <div className="flex items-center gap-1.5">
              <span className="text-sm font-medium text-primary">{row.label}</span>
              {row.isHighest && (
                <span className="shrink-0 rounded-full bg-amber-500/15 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wide text-amber-600">
                  highest
                </span>
              )}
            </div>
            {row.sublabel && <span className="block text-xs text-muted">{row.sublabel}</span>}
          </div>
        </div>
      </TableCell>
      {/* Accuracy with Δ-vs-baseline folded underneath (own column removed). */}
      <TableCell className="text-right">
        <span className="block font-medium text-primary">{fmtPct(row.accuracy)}</span>
        <span className={`block text-xs ${deltaClass}`}>{fmtDelta(row.deltaVsBaseline)}</span>
      </TableCell>
      <TableCell>
        {row.mode === "baseline" ? (
          <span className="text-sm text-muted">—</span>
        ) : (
          <span className="flex items-center gap-1.5 text-sm text-primary">
            <span
              className="inline-block h-2.5 w-2.5 shrink-0 rounded-full"
              style={{ backgroundColor: attemptModeColor(row.mode) }}
            />
            {attemptModeLabel(row.mode)}
          </span>
        )}
      </TableCell>
      <TableCell>
        {row.mode === "baseline" ? (
          <span className="text-sm text-muted">—</span>
        ) : (
          <Badge variant={row.decisionTone}>{row.decisionDisplay}</Badge>
        )}
      </TableCell>
    </TableRow>
  )
}
