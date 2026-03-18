import { useMemo } from "react"
import type { GSOIterationResult } from "@/types"

interface JudgePassRatesProps {
  iterations: GSOIterationResult[]
  baselineJudgeScores?: Record<string, number | null> | null
}

function parseScoresJson(json: string | Record<string, number> | null | undefined): Record<string, number> | null {
  if (!json) return null
  if (typeof json === "object") return json as Record<string, number>
  try {
    return JSON.parse(json)
  } catch {
    return null
  }
}

function toPct(v: number): string {
  const n = v <= 1 ? v * 100 : v
  return `${n.toFixed(1)}%`
}

export function JudgePassRates({ iterations, baselineJudgeScores }: JudgePassRatesProps) {
  const fullIterations = useMemo(
    () => iterations
      .filter((it) => String(it.eval_scope ?? "").toLowerCase() === "full" || it.iteration === 0)
      .sort((a, b) => a.iteration - b.iteration),
    [iterations]
  )

  // Find baseline scores: try iteration 0's scores_json first, then fall back to prop
  const baselineIter = fullIterations.find((it) => it.iteration === 0)
    ?? iterations.find((it) => it.iteration === 0)
  const iterBaselineScores = baselineIter?.scores_json ? parseScoresJson(baselineIter.scores_json) : null
  const baselineScores: Record<string, number> | null =
    iterBaselineScores
    ?? (baselineJudgeScores as Record<string, number> | null)
    ?? null

  const nonBaseline = fullIterations.filter((it) => it.iteration > 0)

  // Collect all judge names from all sources
  const allJudges = useMemo(() => {
    const judges = new Set<string>()
    if (baselineScores) Object.keys(baselineScores).forEach((j) => judges.add(j))
    for (const it of nonBaseline) {
      const scores = parseScoresJson(it.scores_json)
      if (scores) Object.keys(scores).forEach((j) => judges.add(j))
    }
    return Array.from(judges).sort()
  }, [baselineScores, nonBaseline])

  // Show per-iteration columns
  const iterColumns = fullIterations.filter((it) => it.iteration > 0)

  if (allJudges.length === 0) {
    return (
      <div className="rounded-xl border border-default p-6">
        <h3 className="text-sm font-semibold text-primary mb-3">Per-Judge Score Progression</h3>
        <p className="text-sm text-muted text-center py-6">No judge data available</p>
      </div>
    )
  }

  return (
    <div className="rounded-xl border border-default p-6">
      <h3 className="text-sm font-semibold text-primary mb-4">Per-Judge Score Progression</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-default">
              <th className="text-left px-4 py-2.5 text-xs font-medium text-muted">Judge</th>
              <th className="text-right px-4 py-2.5 text-xs font-medium text-muted">Baseline</th>
              {iterColumns.map((it) => (
                <th key={it.iteration} className="text-right px-4 py-2.5 text-xs font-medium text-muted">
                  Iter {it.iteration}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {allJudges.map((judge) => {
              const baseVal = baselineScores?.[judge] ?? null
              return (
                <tr key={judge} className="border-b border-default last:border-0">
                  <td className="px-4 py-2.5 text-primary font-medium">{judge}</td>
                  <td className="px-4 py-2.5 text-right text-muted font-mono">
                    {baseVal != null ? toPct(baseVal) : "\u2014"}
                  </td>
                  {iterColumns.map((it) => {
                    const iterScores = it.scores_json ? parseScoresJson(it.scores_json) : null
                    const iterVal = iterScores?.[judge] ?? null
                    const delta = iterVal != null && baseVal != null
                      ? (iterVal <= 1 ? iterVal * 100 : iterVal) - (baseVal <= 1 ? baseVal * 100 : baseVal)
                      : null
                    return (
                      <td key={it.iteration} className="px-4 py-2.5 text-right font-mono">
                        <span className="text-primary">{iterVal != null ? toPct(iterVal) : "\u2014"}</span>
                        {delta != null && delta !== 0 && (
                          <span className={`ml-1.5 text-xs ${delta > 0 ? "text-emerald-500" : "text-red-500"}`}>
                            ({delta > 0 ? "+" : ""}{delta.toFixed(1)})
                          </span>
                        )}
                      </td>
                    )
                  })}
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>
    </div>
  )
}
