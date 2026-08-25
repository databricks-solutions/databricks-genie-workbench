/**
 * Advisory metric-view section for the IQ Scan surface (Prompt 13.5, MV-D23/D24).
 *
 * The production build of mockup frames 7 (advisory) and 8 (bring-your-own),
 * which this replaces and deletes. It lives below the 12 checks in IQScoreTab —
 * the pre-optimization surface, with NO run context — and asks the advisor to
 * score the space on demand (`POST /spaces/{id}/mv/suggest`, a born-terminal
 * sentinel advice run). The response carries the SAME `MvProposal` shape the
 * run-keyed and space-scoped lists return, so this mounts `MvProposalCard`
 * (MvSuggestOnlyPanel's card) from a space-scoped source with NO component
 * change — the MV-D23 prop-driven payoff.
 *
 * One `MvSuggestResponse` distinguishes the three advisory states so the panel
 * never infers intent from an empty list:
 *   - found    — COMPLETE with proposals → proposal cards
 *   - EMPTY    — SKIPPED / COMPLETE-no-proposals → the "clean result" copy
 *   - couldn't-run — FAILED → an honest error, not a silent empty
 *
 * Frame 8 (MV-D24) closes the copied-DDL one-way exit: a free-standing register
 * input and a per-card "I created this myself" both `POST …/mv/register`, and
 * the one `MvRegisterResponse` shape renders verified (USER_CREATED, NO drop
 * action — invariant 1) or refused (the reason, nothing recorded — invariant 2).
 */
import { useState } from "react"
import { ArrowUpRight, Check, RefreshCw, Sparkles, AlertTriangle } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { MvProposalCard } from "@/components/auto-optimize/MvProposalCard"
import { suggestSpaceMv, registerSpaceMv, getMvDdl } from "@/lib/api"
import type { MvProposal, MvSuggestResponse, MvRegisterResponse, MvDdlArtifact } from "@/types"

// VERBATIM from the Prompt 10 review (mockup frame 8b) — do not paraphrase.
const REGISTERED_COPY =
  "Registered. It will be attached and measured on the next optimization run. " +
  "The app never drops views it didn't create — dropping this one stays in your hands."

interface MvIqScanAdvisorySectionProps {
  spaceId: string
  /** Opens the run config in create_and_attach mode (MV-D1). Also the frame-8b
      "Start an optimization run" affordance for a never-optimized user. */
  onReviewCreate?: (proposal: MvProposal | null) => void
}

export function MvIqScanAdvisorySection({ spaceId, onReviewCreate }: MvIqScanAdvisorySectionProps) {
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<MvSuggestResponse | null>(null)
  const [error, setError] = useState<string | null>(null)
  // Prompt 15.1: an advice run writes no DDL artifact, so DDL is fetched per
  // proposal from route 7's candidate fallback (pinned by suggestion_id).
  const [ddlById, setDdlById] = useState<Record<string, MvDdlArtifact>>({})

  const [registerValue, setRegisterValue] = useState("")
  const [claimId, setClaimId] = useState<string | null>(null)
  const [registerBusy, setRegisterBusy] = useState(false)
  const [registerResult, setRegisterResult] = useState<MvRegisterResponse | null>(null)
  const [registerError, setRegisterError] = useState<string | null>(null)

  async function runSuggest() {
    setLoading(true)
    setError(null)
    setDdlById({})
    try {
      const res = await suggestSpaceMv(spaceId)
      setResult(res)
      // Fetch each proposal's copy-ready DDL best-effort — a card whose DDL fetch
      // fails still renders its evidence, so this never blocks the scan result.
      const props = res.proposals ?? []
      if (res.run_id && props.length > 0) {
        const settled = await Promise.allSettled(
          props.map((p) => getMvDdl(res.run_id, p.suggestion_id)),
        )
        const map: Record<string, MvDdlArtifact> = {}
        settled.forEach((s, i) => {
          if (s.status === "fulfilled") map[props[i].suggestion_id] = s.value
        })
        setDdlById(map)
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not run the metric-view scan.")
    } finally {
      setLoading(false)
    }
  }

  async function runRegister() {
    const full_name = registerValue.trim()
    if (!full_name) return
    setRegisterBusy(true)
    setRegisterError(null)
    setRegisterResult(null)
    try {
      setRegisterResult(
        await registerSpaceMv(spaceId, { full_name, suggestion_id: claimId }),
      )
    } catch (e) {
      setRegisterError(e instanceof Error ? e.message : "Registration failed; please retry.")
    } finally {
      setRegisterBusy(false)
    }
  }

  function claimFromCard(proposal: MvProposal) {
    setClaimId(proposal.suggestion_id)
    setRegisterValue(proposal.proposed_object ?? "")
    setRegisterResult(null)
    setRegisterError(null)
  }

  const proposals = result?.proposals ?? []
  const failed = result?.status === "FAILED"

  return (
    <div className="bg-surface border border-default rounded-xl p-5 space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="flex items-center gap-2 text-sm font-semibold uppercase tracking-wide text-secondary">
          <Sparkles className="h-4 w-4 text-accent" />
          Metric view suggestions
        </h3>
        <button
          onClick={runSuggest}
          disabled={loading}
          className="flex items-center gap-1 text-xs text-muted hover:text-accent transition-colors disabled:opacity-50"
          title="Scan this Agent's SQL for un-governed measures"
        >
          <RefreshCw className={`h-3 w-3 ${loading ? "animate-spin" : ""}`} />
          {loading ? "Scanning…" : result ? "Re-scan" : "Scan for suggestions"}
        </button>
      </div>

      {error && (
        <p className="text-xs text-danger">{error}</p>
      )}

      {result && (
        failed ? (
          <MvAdvisoryCouldNotRun reason={result.error} onRetry={runSuggest} />
        ) : proposals.length > 0 ? (
          <div className="space-y-4">
            {proposals.map((proposal) => (
              <MvProposalCard
                key={proposal.suggestion_id}
                proposal={proposal}
                ddl={ddlById[proposal.suggestion_id]}
                actions={
                  <>
                    <Button size="sm" onClick={() => onReviewCreate?.(proposal)}>
                      Review and create metric view
                    </Button>
                    {/* tertiary — the MV-D24 affordance for the copied-DDL path */}
                    <Button size="sm" variant="ghost" onClick={() => claimFromCard(proposal)}>
                      I created this myself
                    </Button>
                  </>
                }
              />
            ))}
          </div>
        ) : (
          <MvAdvisoryEmpty />
        )
      )}

      {/* Frame 8a — the free-standing register input, always available (a user
          may have created a view the scan never proposed). Verified/refused
          render below it from the one MvRegisterResponse shape. */}
      <MvRegisterInput
        value={registerValue}
        onChange={(v) => { setRegisterValue(v); setClaimId(null) }}
        busy={registerBusy}
        onRegister={runRegister}
        claimId={claimId}
      />
      {registerError && <p className="text-xs text-danger">{registerError}</p>}
      {registerResult && (
        registerResult.registered ? (
          <MvRegisterVerified result={registerResult} onStartRun={() => onReviewCreate?.(null)} />
        ) : (
          <MvRegisterRefused result={registerResult} />
        )
      )}
    </div>
  )
}

// ── EMPTY (MV-D15) — AUTHORED COPY, graduated verbatim from mockup frame 7b ──
export function MvAdvisoryEmpty() {
  return (
    <div className="rounded-xl border border-default bg-elevated px-4 py-6 text-center">
      <p className="text-sm font-medium text-primary">No recurring measures to propose yet</p>
      <p className="mx-auto mt-2 max-w-prose text-sm text-muted">
        The scan read this Agent&rsquo;s example question SQL, saved SQL snippets, and benchmark answers, and found
        no measure that recurs often enough to justify a governed metric view. That&rsquo;s a clean result — the
        scan ran and looked; it simply found nothing recurring. As this Agent gains more questions and its SQL
        builds up repeated aggregations, re-run the scan and any proposals will appear here.
      </p>
    </div>
  )
}

// ── couldn't-run — a FAILED advisor is an honest error, never a silent empty ─
export function MvAdvisoryCouldNotRun({ reason, onRetry }: { reason: string | null; onRetry?: () => void }) {
  return (
    <div className="space-y-3 rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-3">
      <div className="flex items-start gap-1.5 text-sm text-amber-700 dark:text-amber-300">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
        <span>
          <span className="font-medium">The metric-view scan didn&rsquo;t complete.</span>{" "}
          {reason || "Something went wrong reading this Agent's SQL. No suggestions were recorded."}
        </span>
      </div>
      {onRetry && (
        <Button size="sm" variant="secondary" onClick={onRetry}>
          Try again
        </Button>
      )}
    </div>
  )
}

// ── Frame 8a — register input ───────────────────────────────────────────────
export function MvRegisterInput({
  value,
  onChange,
  busy,
  onRegister,
  claimId,
}: {
  value: string
  onChange: (v: string) => void
  busy: boolean
  onRegister: () => void
  claimId: string | null
}) {
  return (
    <div className="space-y-2 rounded-xl border border-default bg-surface-subtle p-4">
      <p className="text-sm font-medium text-primary">Register an existing metric view</p>
      <p className="text-xs text-muted">
        Already created a metric view yourself? Point us at it and we&rsquo;ll verify it under your identity and
        attach it on the next optimization run.
        {claimId && " (Linked to the suggestion you selected.)"}
      </p>
      <div className="flex items-center gap-2">
        <input
          type="text"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder="catalog.schema.metric_view"
          className="flex-1 rounded-md border border-default bg-surface px-2.5 py-1.5 font-mono text-sm text-primary"
        />
        <Button size="sm" disabled={busy || !value.trim()} onClick={onRegister}>
          {busy ? "Verifying…" : "Register"}
        </Button>
      </div>
    </div>
  )
}

// ── Frame 8b — verified / registered (NO [Drop view], MV-D24 invariant 1) ────
export function MvRegisterVerified({
  result,
  onStartRun,
}: {
  result: MvRegisterResponse
  onStartRun?: () => void
}) {
  return (
    <div className="space-y-4 rounded-xl border border-success/30 bg-success/10 p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <a
          className="inline-flex items-center gap-1 font-mono text-sm font-medium text-accent hover:underline"
          href="#"
        >
          {result.full_name}
          <ArrowUpRight className="h-3.5 w-3.5" />
        </a>
        <Badge variant="info">{result.provenance}</Badge>
      </div>

      <div className="space-y-1.5">
        <div className="flex items-center gap-1.5 text-sm text-success-foreground">
          <Check className="h-3.5 w-3.5 shrink-0 text-success" />
          Type: <span className="font-mono">METRIC_VIEW</span> confirmed
        </div>
        <div className="flex items-center gap-1.5 text-sm text-success-foreground">
          <Check className="h-3.5 w-3.5 shrink-0 text-success" />
          Validation passed
        </div>
      </div>

      {result.warnings.length > 0 && (
        <ul className="list-disc space-y-0.5 pl-5 text-xs text-muted">
          {result.warnings.map((w, i) => (
            <li key={i}>{w}</li>
          ))}
        </ul>
      )}

      <div className="rounded-lg border border-success/30 bg-success/10 px-3 py-2 text-sm text-success-foreground">
        {REGISTERED_COPY}
      </div>

      {/* The IQ Scan surface is where a never-optimized user meets this, so
          offer a run — otherwise the BYO path strands them (the same one-way
          exit MV-D24 closes, moved one step later). NO [Drop view]: dropping a
          USER_CREATED view stays with the user (invariant 1). */}
      {onStartRun && (
        <div className="flex flex-wrap items-center gap-2 pt-1">
          <Button size="sm" onClick={onStartRun}>Start an optimization run</Button>
          <span className="text-xs text-muted">Or approve it and it attaches on your next run.</span>
        </div>
      )}
    </div>
  )
}

// ── Frame 8c — refused (the reason; nothing recorded, MV-D24 invariant 2) ────
export function MvRegisterRefused({ result }: { result: MvRegisterResponse }) {
  return (
    <div className="space-y-2 rounded-xl border border-amber-500/30 bg-amber-500/10 px-4 py-3">
      <div className="flex items-start gap-1.5 text-sm text-amber-700 dark:text-amber-300">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
        <span>
          <span className="font-medium">We couldn&rsquo;t register that view.</span>{" "}
          {result.reason || "The identifier could not be verified."} Nothing was recorded.
        </span>
      </div>
    </div>
  )
}
