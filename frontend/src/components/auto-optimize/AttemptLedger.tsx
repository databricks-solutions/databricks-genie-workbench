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
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Attempt</TableHead>
            <TableHead className="text-right">Accuracy</TableHead>
            <TableHead className="text-right">Δ vs baseline</TableHead>
            <TableHead>Mode</TableHead>
            <TableHead>Decision</TableHead>
            <TableHead className="text-center">Champion</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <Fragment key={row.key}>
              <LedgerTableRow row={row} />
              {row.divergenceReason && (
                <TableRow className="hover:bg-transparent">
                  <TableCell colSpan={6} className="pt-0">
                    <p className="flex items-start gap-1.5 text-xs text-amber-600 dark:text-amber-400">
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
  return (
    <TableRow
      className={row.isHighest ? "bg-amber-50 dark:bg-amber-950/20" : undefined}
      data-highest={row.isHighest || undefined}
    >
      <TableCell>
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-primary">{row.label}</span>
          {row.sublabel && <span className="text-xs text-muted">{row.sublabel}</span>}
          {row.isHighest && (
            <span className="rounded-full bg-amber-100 px-1.5 py-0.5 text-[9px] font-semibold uppercase tracking-wide text-amber-700 dark:bg-amber-900/40 dark:text-amber-300">
              highest
            </span>
          )}
        </div>
      </TableCell>
      <TableCell className="text-right font-medium text-primary">{fmtPct(row.accuracy)}</TableCell>
      <TableCell
        className={`text-right ${
          row.deltaVsBaseline == null || row.deltaVsBaseline === 0
            ? "text-muted"
            : row.deltaVsBaseline > 0
              ? "text-emerald-600 dark:text-emerald-400"
              : "text-red-500"
        }`}
      >
        {fmtDelta(row.deltaVsBaseline)}
      </TableCell>
      <TableCell>
        {row.mode === "baseline" ? (
          <span className="text-sm text-muted">—</span>
        ) : (
          <span className="flex items-center gap-1.5 text-sm text-primary">
            <span
              className="inline-block h-2.5 w-2.5 rounded-full"
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
      <TableCell className="text-center">
        {row.isChampion ? (
          <span title="Champion configuration" className="text-base text-indigo-500">
            ★
          </span>
        ) : (
          <span className="text-muted">—</span>
        )}
      </TableCell>
    </TableRow>
  )
}
