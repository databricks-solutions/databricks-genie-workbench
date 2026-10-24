/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * Frame 7 (MV-D23): the advisory metric-view section that IQScoreTab.tsx gains
 * below the 12 checks — the pre-optimization surface, with NO run context. It
 * renders the SAME MvProposalCard as frame 4 but passes NEITHER the liftLabel
 * NOR the [Re-run] action (nothing was run to measure, no run to re-run); the
 * primary action opens the consent flow directly. Prompt 13.5 builds this for
 * real and deletes this file.
 *
 * Three frames: (a) proposals found, (b) MV-D15 empty state, (c) not-entitled.
 *
 * ⚠ COPY REVIEW REQUIRED — frame (b): the empty-state copy below is AUTHORED
 * NEW for this branch (it was not sourced verbatim from any doc). It must be
 * signed off before ship. Draft goals: name what the scan actually read so the
 * reader trusts the check ran; say what would change the answer; never read as
 * an error or as the feature being unavailable.
 *
 * Query history is deliberately OMITTED from the "what was read" list, and the
 * omission is correct (not merely cautious): query history is not in the
 * candidate corpus AT ALL. `corpus_scan` reads generated + curated SQL, never
 * `statement_text` from `system.query.history`, so naming it would overclaim a
 * source the scan never touched.
 *   DRIFT WATCH: POV "Delta 9" lists query history as a *potential future*
 *   corpus source. If Prompt 13.5 ever mines `system.query.history` into the
 *   fingerprint corpus, this copy MUST gain a "query history" clause in the
 *   same change — otherwise the message and the corpus silently diverge. See
 *   the inline comment on the copy below.
 */
import { Sparkles } from "lucide-react"
import { Button } from "@/components/ui/button"
import { MvProposalCard } from "./MvProposalCard"
import { DenialBanner } from "./MvRunConfigMockups"
import { ddlRevenue, proposalRevenue } from "./mvMockData"

export function AdvisorySection({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-xl border border-default bg-surface p-5">
      <h3 className="mb-4 flex items-center gap-2 text-sm font-semibold uppercase tracking-wide text-secondary">
        <Sparkles className="h-4 w-4 text-accent" />
        Metric view suggestions
      </h3>
      {children}
    </div>
  )
}

// ── Frame 7a — proposals found ──────────────────────────────────────────────
export function IqScanAdvisoryFoundFrame() {
  return (
    <AdvisorySection>
      <MvProposalCard
        proposal={proposalRevenue}
        ddl={ddlRevenue}
        actions={<Button size="sm">Review and create metric view</Button>}
      />
    </AdvisorySection>
  )
}

// ── Frame 7b — MV-D15 empty state (AUTHORED COPY — see header, needs review) ─
export function IqScanAdvisoryEmptyFrame() {
  return (
    <AdvisorySection>
      <div className="rounded-xl border border-default bg-elevated px-4 py-6 text-center">
        <p className="text-sm font-medium text-primary">No recurring measures to propose yet</p>
        {/* DRIFT WATCH (see header): the "what was read" list names only the
            curated + generated SQL corpus_scan actually reads. Add "query
            history" here ONLY if/when Prompt 13.5 mines system.query.history
            into the fingerprint corpus (POV Delta 9). */}
        <p className="mx-auto mt-2 max-w-prose text-sm text-muted">
          The scan read this Agent&rsquo;s example question SQL, saved SQL snippets, and benchmark answers, and found
          no measure that recurs often enough to justify a governed metric view. That&rsquo;s a clean result — the
          scan ran and looked; it simply found nothing recurring. As this Agent gains more questions and its SQL
          builds up repeated aggregations, re-run the scan and any proposals will appear here.
        </p>
      </div>
    </AdvisorySection>
  )
}

// ── Frame 7c — not-entitled (reuses frame 3's denial banner, unchanged) ─────
export function IqScanAdvisoryNotEntitledFrame() {
  return (
    <AdvisorySection>
      <DenialBanner target="finance.sales" />
    </AdvisorySection>
  )
}
