/**
 * PR #332 M7d fidelity frame (MV-D121) — the run headline for a finished run whose
 * iteration-0 enrichment won: the REAL ScoreSummary and convergence-reason copy show the
 * gain, and no "Baseline retained". m6c-c stays the control for a full-scope iteration 0.
 */
import { ScoreSummary } from "../ScoreSummary"
import { convergenceReasonText } from "@/lib/score-display"

// MvM6cFidelityFrames' Headline (not exported there), so this frame matches m6c-a.
function Headline(props: { status: string; bestEvalScope: string; optimizedScore: number }) {
  const reason = convergenceReasonText({
    baselineScore: 86.67,
    optimizedScore: props.optimizedScore,
    bestIteration: 0,
    status: props.status,
    bestEvalScope: props.bestEvalScope,
    convergenceReason: null,
  })
  return (
    <div className="max-w-3xl space-y-2 p-6">
      <ScoreSummary
        baselineScore={86.67}
        optimizedScore={props.optimizedScore}
        bestIteration={0}
        bestEvalScope={props.bestEvalScope}
        status={props.status}
      />
      <p className="text-sm text-muted-foreground">{reason ?? "(no reason line)"}</p>
    </div>
  )
}

export const EnrichmentWinTerminalFrame = () => (
  <Headline status="CONVERGED" bestEvalScope="enrichment" optimizedScore={90.0} />
)
