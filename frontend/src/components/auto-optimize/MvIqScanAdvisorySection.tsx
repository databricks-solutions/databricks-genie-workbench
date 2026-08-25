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
import { useEffect, useRef, useState } from "react"
import { ArrowUpRight, Check, CheckCircle2, ChevronDown, Circle, Loader2, RefreshCw, ShieldCheck, Sparkles, AlertTriangle } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { MvProposalCard } from "@/components/auto-optimize/MvProposalCard"
import { splitProposalsByConfidence } from "@/components/auto-optimize/mvFormat"
import { streamSpaceMvSuggest, fetchSpaceMvProposals, registerSpaceMv, getMvDdl } from "@/lib/api"
import type { MvProposal, MvSuggestResponse, MvRegisterResponse, MvDdlArtifact, MvLastScan } from "@/types"

// The four honest scan stages, in entry order — the literal labels the backend
// emits over SSE (MV_ADVISOR_STAGES). Named for where the wall time goes, not by
// module: SCORING predictably holds for most of a multi-minute scan, so its
// label says what it waits on. Kept here so the progress checklist can show all
// four (entered vs pending) rather than a bare spinner (finding 2).
const SCAN_STAGES = [
  "reading curated SQL",
  "scanning for recurring measures",
  "scoring candidates (embeddings + usage signals)",
  "rendering DDL",
] as const

function formatDuration(seconds: number | null | undefined): string | null {
  if (seconds == null || seconds <= 0) return null
  const mins = Math.floor(seconds / 60)
  const secs = Math.round(seconds % 60)
  return mins > 0 ? `${mins}m ${secs}s` : `${secs}s`
}

function formatRelative(iso: string | null | undefined): string | null {
  if (!iso) return null
  const parsed = Date.parse(iso)
  if (Number.isNaN(parsed)) return null
  const mins = Math.floor((Date.now() - parsed) / 60000)
  if (mins < 1) return "just now"
  if (mins < 60) return `${mins}m ago`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs}h ago`
  return `${Math.floor(hrs / 24)}d ago`
}

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
  // MV-D31: the stages entered so far (SSE, on entry) drive the progress
  // checklist; last_scan hydrates the panel on mount; hydrating gates the mount
  // read so a never-scanned space shows a first-class Scan affordance, not empty.
  const [stages, setStages] = useState<string[]>([])
  const [lastScan, setLastScan] = useState<MvLastScan | null>(null)
  const [hydrating, setHydrating] = useState(true)
  const abortRef = useRef<(() => void) | null>(null)

  const [registerValue, setRegisterValue] = useState("")
  const [claimId, setClaimId] = useState<string | null>(null)
  const [registerBusy, setRegisterBusy] = useState(false)
  const [registerResult, setRegisterResult] = useState<MvRegisterResponse | null>(null)
  const [registerError, setRegisterError] = useState<string | null>(null)
  // MV-D30 surfacing: LOW-confidence proposals hide behind an explicit disclosure.
  const [showLow, setShowLow] = useState(false)

  // Best-effort per-proposal DDL fetch (route 7 candidate fallback, by run_id).
  // A card whose DDL fetch fails still renders its evidence, so this never
  // blocks the result. Proposals can span advice runs (hydration), so group by
  // each proposal's own run_id rather than assuming a single run.
  async function loadDdl(props: MvProposal[]) {
    const withRun = props.filter((p) => p.run_id)
    if (withRun.length === 0) {
      setDdlById({})
      return
    }
    const settled = await Promise.allSettled(
      withRun.map((p) => getMvDdl(p.run_id as string, p.suggestion_id)),
    )
    const map: Record<string, MvDdlArtifact> = {}
    settled.forEach((s, i) => {
      if (s.status === "fulfilled") map[withRun[i].suggestion_id] = s.value
    })
    setDdlById(map)
  }

  // Hydrate-on-mount (MV-D31, finding 8): the panel opens from persisted state —
  // "last scanned … — N proposals" — without re-running a multi-minute scan.
  // Proposals were persisted all along (fetchSpaceMvProposals); a prior EMPTY /
  // SKIP hydrates too, from last_scan's derived skip_reason / measures_found.
  useEffect(() => {
    let alive = true
    setHydrating(true)
    fetchSpaceMvProposals(spaceId)
      .then((res) => {
        if (!alive) return
        const scan = res.last_scan ?? null
        setLastScan(scan)
        const props = res.proposals ?? []
        if (props.length > 0 || scan) {
          // Reconstruct the advisor's own shape from persisted state so the same
          // found / EMPTY / (never FAILED — a failure is not hydrated) rendering
          // runs, keyed off last_scan's derived skip_reason + measures_found.
          setResult({
            space_id: spaceId,
            run_id: props[0]?.run_id ?? "",
            status: scan?.status ?? (props.length > 0 ? "COMPLETE" : "SKIPPED"),
            skip_reason: scan?.skip_reason ?? null,
            measures_found: scan?.measures_found ?? null,
            error: null,
            proposals: props,
          })
          if (props.length > 0) void loadDdl(props)
        }
      })
      .catch(() => { /* hydration is best-effort; the Scan affordance still works */ })
      .finally(() => { if (alive) setHydrating(false) })
    return () => { alive = false }
  }, [spaceId])

  // Cancel any in-flight scan stream when the component unmounts.
  useEffect(() => () => abortRef.current?.(), [])

  function runSuggest() {
    abortRef.current?.()
    setLoading(true)
    setError(null)
    setDdlById({})
    setShowLow(false)
    setStages([])
    abortRef.current = streamSpaceMvSuggest(spaceId, {
      // Emit on entry: append each stage as the backend reaches it, so the
      // checklist advances live instead of sitting on one label for 90% of the
      // wait. Dedupe defensively (a stage should arrive once).
      onStage: (stage) =>
        setStages((prev) => (prev.includes(stage) ? prev : [...prev, stage])),
      onResult: (res) => {
        setResult(res)
        setLastScan({
          scanned_at: new Date().toISOString(),
          duration_seconds: null,
          status: res.status,
          skip_reason: res.skip_reason,
          measures_found: res.measures_found,
          proposal_count: (res.proposals ?? []).length,
        })
        void loadDdl(res.proposals ?? [])
        setLoading(false)
        abortRef.current = null
      },
      onError: (message) => {
        setError(message)
        setLoading(false)
        abortRef.current = null
      },
    })
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
  // Blank-never-renders + MEDIUM+ floor (MV-D30): a proposal with no
  // proposed_object is dropped, MEDIUM+ surface by default, LOW is disclosed.
  const { primary, low } = splitProposalsByConfidence(proposals)
  const hasRenderable = primary.length > 0 || low.length > 0
  // Never-scanned = mount finished, no prior scan recorded, and nothing is
  // running. That state earns a first-class Scan affordance, not a bare result.
  const neverScanned = !hydrating && !loading && !result && !lastScan
  const scannedAt = formatRelative(lastScan?.scanned_at)
  const lastDuration = formatDuration(lastScan?.duration_seconds)

  return (
    <div className="bg-surface border border-default rounded-xl p-5 space-y-4">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="flex items-center gap-2 text-sm font-semibold uppercase tracking-wide text-secondary">
            <Sparkles className="h-4 w-4 text-accent" />
            Metric view suggestions
          </h3>
          {/* Hydrated framing (finding 8): what the panel is showing and when it
              was last scanned — so results are not silently lost between visits. */}
          {!loading && !neverScanned && lastScan && (
            <p className="mt-1 text-xs text-muted">
              {scannedAt ? `Last scanned ${scannedAt}` : "Last scanned"}
              {typeof lastScan.proposal_count === "number" &&
                ` · ${lastScan.proposal_count} ${lastScan.proposal_count === 1 ? "proposal" : "proposals"}`}
              {lastDuration && ` · took ${lastDuration}`}
            </p>
          )}
        </div>
        {/* Re-scan stays a light affordance once a result exists; the first-run
            Scan is promoted to a first-class action below (finding 1). */}
        {!neverScanned && (
          <button
            onClick={runSuggest}
            disabled={loading}
            className="flex shrink-0 items-center gap-1 text-xs text-muted hover:text-accent transition-colors disabled:opacity-50"
            title="Re-scan this Agent's SQL for un-governed measures"
          >
            <RefreshCw className={`h-3 w-3 ${loading ? "animate-spin" : ""}`} />
            {loading ? "Scanning…" : "Re-scan"}
          </button>
        )}
      </div>

      {/* Finding 1 — a first-class Scan action for a never-scanned space: names
          what it reads and sets an honest expectation for how long it takes. */}
      {neverScanned && (
        <div className="rounded-xl border border-dashed border-default bg-elevated px-4 py-6 text-center">
          <Sparkles className="mx-auto h-5 w-5 text-accent" />
          <p className="mt-2 text-sm font-medium text-primary">
            Scan for metric-view suggestions
          </p>
          <p className="mx-auto mt-1 mb-4 max-w-prose text-xs text-muted">
            Reads this Agent&rsquo;s example question SQL, saved SQL snippets, and benchmark answers, and proposes
            governed metric views for the measures that recur. This can take a few minutes.
          </p>
          <Button size="sm" onClick={runSuggest}>
            <Sparkles className="mr-1.5 h-3.5 w-3.5" />
            Scan for suggestions
          </Button>
        </div>
      )}

      {/* Finding 2 — staged progress, not a bare spinner. Each stage is shown on
          entry; the active one spins, entered ones check, later ones wait. An
          honest label on the long SCORING stage is what makes the wait tolerable. */}
      {loading && (
        <ScanProgress stages={stages} lastDuration={lastDuration} />
      )}

      {error && !loading && (
        <p className="text-xs text-danger">{error}</p>
      )}

      {!loading && result && (
        failed ? (
          <MvAdvisoryCouldNotRun reason={result.error} onRetry={runSuggest} />
        ) : hasRenderable ? (
          <div className="space-y-4">
            {primary.map((proposal) => (
              <ScanProposalCard
                key={proposal.suggestion_id}
                proposal={proposal}
                ddl={ddlById[proposal.suggestion_id]}
                onReviewCreate={onReviewCreate}
                onClaim={claimFromCard}
              />
            ))}

            {low.length > 0 && (
              <div className="space-y-4">
                {!showLow ? (
                  <button
                    onClick={() => setShowLow(true)}
                    className="flex w-full items-center justify-center gap-1.5 rounded-lg border border-dashed border-default px-3 py-2 text-xs text-muted transition-colors hover:text-accent"
                  >
                    <ChevronDown className="h-3.5 w-3.5" />
                    Show {low.length} low-confidence {low.length === 1 ? "suggestion" : "suggestions"}
                    {primary.length === 0 && " (nothing scored MEDIUM or higher)"}
                  </button>
                ) : (
                  <>
                    <p className="text-xs text-muted">
                      Low-confidence suggestions — weaker recurrence or thinner evidence. Review before creating.
                    </p>
                    {low.map((proposal) => (
                      <ScanProposalCard
                        key={proposal.suggestion_id}
                        proposal={proposal}
                        ddl={ddlById[proposal.suggestion_id]}
                        onReviewCreate={onReviewCreate}
                        onClaim={claimFromCard}
                      />
                    ))}
                  </>
                )}
              </div>
            )}
          </div>
        ) : (
          <MvAdvisoryEmpty skipReason={result.skip_reason} measuresFound={result.measures_found} />
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

// A scan proposal card with the two IQ-Scan actions, shared by the primary and
// low-confidence lists so both render identically (only their placement differs).
function ScanProposalCard({
  proposal,
  ddl,
  onReviewCreate,
  onClaim,
}: {
  proposal: MvProposal
  ddl: MvDdlArtifact | undefined
  onReviewCreate?: (proposal: MvProposal | null) => void
  onClaim: (proposal: MvProposal) => void
}) {
  return (
    <MvProposalCard
      proposal={proposal}
      ddl={ddl}
      actions={
        <>
          {/* Finding 7 tail — the CTA now names where it goes. "Review and create"
              read as an in-place create; it actually opens the run setup in
              create-and-attach mode, so say so rather than surprise the user. */}
          <Button size="sm" onClick={() => onReviewCreate?.(proposal)}>
            Review in run setup
            <ArrowUpRight className="ml-1.5 h-3.5 w-3.5" />
          </Button>
          {/* tertiary — the MV-D24 affordance for the copied-DDL path */}
          <Button size="sm" variant="ghost" onClick={() => onClaim(proposal)}>
            I created this myself
          </Button>
        </>
      }
    />
  )
}

// ── Staged progress (MV-D31, finding 2) ─────────────────────────────────────
// The four honest stages as a live checklist. Emitted on entry, so the last
// stage received is the one running NOW: it spins; earlier ones are done; later
// ones wait. This replaces the bare "Scanning…" spinner that sat unmoving for
// minutes and left the user unsure it was working at all.
export function ScanProgress({
  stages,
  lastDuration,
}: {
  stages: string[]
  lastDuration: string | null
}) {
  // Index of the stage currently running = the last one entered. Before the
  // first event arrives, treat stage 0 as active (the request is in flight).
  const currentIdx = stages.length > 0 ? SCAN_STAGES.indexOf(stages[stages.length - 1] as (typeof SCAN_STAGES)[number]) : 0

  return (
    <div className="space-y-2 rounded-xl border border-default bg-elevated px-4 py-3">
      <p className="text-xs font-medium text-secondary">Scanning this Agent&rsquo;s SQL…</p>
      <ul className="space-y-1.5">
        {SCAN_STAGES.map((stage, i) => {
          const done = i < currentIdx
          const active = i === currentIdx
          return (
            <li key={stage} className="flex items-center gap-2 text-xs">
              {done ? (
                <CheckCircle2 className="h-3.5 w-3.5 shrink-0 text-success" />
              ) : active ? (
                <Loader2 className="h-3.5 w-3.5 shrink-0 animate-spin text-accent" />
              ) : (
                <Circle className="h-3.5 w-3.5 shrink-0 text-muted opacity-40" />
              )}
              <span className={done ? "text-muted" : active ? "text-primary" : "text-muted opacity-60"}>
                {stage}
              </span>
            </li>
          )
        })}
      </ul>
      <p className="text-xs text-muted">
        {lastDuration
          ? `The last scan took ${lastDuration}. You can leave this open — it keeps running.`
          : "This can take a few minutes. You can leave this open — it keeps running."}
      </p>
    </div>
  )
}

// ── EMPTY (MV-D15/D30) — three variants keyed on the governance ladder ───────
//
// A single "nothing to propose" copy misread three distinct states as one. The
// advisor's skip_reason + measures_found distinguish them, and each earns its
// own honest copy (Prompt 15.3, finding 3):
//   - NO_PARSEABLE_SQL          → no curated SQL to read yet (add example
//                                  questions / SQL snippets, then re-scan)
//   - NO_CANDIDATES, found == 0 → the scan looked and found nothing recurring
//                                  (the original clean-result copy)
//   - NO_CANDIDATES, found > 0  → every recurring measure is ALREADY governed —
//                                  the "you're in good shape" confidence empty
// Any other/absent reason falls back to the found-nothing copy (a clean empty is
// the safe default; we never imply a failure the advisor didn't report).
export function MvAdvisoryEmpty({
  skipReason,
  measuresFound,
}: {
  skipReason?: string | null
  measuresFound?: number | null
}) {
  if (skipReason === "NO_PARSEABLE_SQL") {
    return (
      <div className="rounded-xl border border-default bg-elevated px-4 py-6 text-center">
        <p className="text-sm font-medium text-primary">No SQL to scan yet</p>
        <p className="mx-auto mt-2 max-w-prose text-sm text-muted">
          A metric-view scan reads this Agent&rsquo;s example question SQL, saved SQL snippets, and benchmark
          answers &mdash; and there&rsquo;s no parseable SQL here to read. Add a few example questions with SQL
          answers or attach SQL snippets, then re-scan and any recurring measures will surface as proposals.
        </p>
      </div>
    )
  }

  if (skipReason === "NO_CANDIDATES" && (measuresFound ?? 0) > 0) {
    const n = measuresFound ?? 0
    return (
      <div className="rounded-xl border border-success/30 bg-success/10 px-4 py-6 text-center">
        <ShieldCheck className="mx-auto h-5 w-5 text-success" />
        <p className="mt-2 text-sm font-medium text-primary">You&rsquo;re in good shape &mdash; already governed</p>
        <p className="mx-auto mt-2 max-w-prose text-sm text-muted">
          The scan found {n} recurring {n === 1 ? "measure" : "measures"} in this Agent&rsquo;s SQL, and every one
          is already defined in a governed metric view. There&rsquo;s nothing new to propose &mdash; the measures
          your questions rely on are already governed. Re-scan after adding new SQL and any un-governed measures
          will appear here.
        </p>
      </div>
    )
  }

  return (
    <div className="rounded-xl border border-default bg-elevated px-4 py-6 text-center">
      <p className="text-sm font-medium text-primary">No recurring measures to propose yet</p>
      <p className="mx-auto mt-2 max-w-prose text-sm text-muted">
        The scan read this Agent&rsquo;s example question SQL, saved SQL snippets, and benchmark answers, and found
        no measure that recurs often enough to justify a governed metric view. That&rsquo;s a clean result &mdash; the
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
