/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * Frames 4–5 (the run output / results screen panels) have GRADUATED to
 * production and were removed here by Prompt 13 — they now live as
 * MvSuggestOnlyPanel / MvCreateAttachPanel, composed into RunDetailView via
 * MvRunOutputSection. What remains is LiftNotMeasuredLabel, still imported by the
 * BYO-registration frame (frame 8); it is disposed with that frame at Prompt 13.5.
 */
import { ShieldAlert } from "lucide-react"

// POV §7.5 — verbatim. The production copy lives in mvFormat.LIFT_NOT_MEASURED;
// this mockup copy is retained only for the frame-8 (BYO) scaffold.
const LIFT_NOT_MEASURED = "Lift not measured — this metric view was not created or attached during this run."

export function LiftNotMeasuredLabel() {
  return (
    <div className="flex items-start gap-1.5 rounded-lg border border-default bg-elevated px-3 py-2 text-xs text-secondary">
      <ShieldAlert className="mt-0.5 h-3.5 w-3.5 shrink-0 text-muted" />
      <span>{LIFT_NOT_MEASURED}</span>
    </div>
  )
}
