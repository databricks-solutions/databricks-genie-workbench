/**
 * The ONE accept flow (MV-D34, Prompt 15.8), consumed by BOTH suggestion
 * surfaces (MvIqScanAdvisorySection and the run-output panels). Divergence
 * between the two surfaces is the defect class Prompt 15.8 exists to end, so the
 * primary [Create this metric view] action, its probe → consent → create state
 * machine, and the degrade-to-[Approve for later] fallback all live here once
 * and are rendered identically wherever a proposal card's footer needs them.
 *
 * The flow REUSES the create machinery rather than forking it: the OBO probe
 * route that already exists (`probeMvEntitlement`), the consent recorded on that
 * route, and the create-at-approval route that runs the same `mv_create` seam
 * under the user's identity and records an `OBO_CREATED` ledger row on the
 * space's advice run — the BYO-register rails, so attach-and-lift run on the
 * next optimization run unchanged (MV-D24/MV-D16). Nothing here talks to the SP.
 *
 * Three terminal outcomes, never a dead end (MV-D34.c):
 *   - created   → "Created · will be attached and measured on the next run"
 *                 + Catalog Explorer link + [Start an optimization run].
 *   - degraded  → the fresh probe fell below SUFFICIENT; the button becomes
 *                 [Approve for later] (the classic MV-D1 path) with the
 *                 remediation GRANT shown copy-ready.
 *   - approved  → the classic approve-for-rerun decision was recorded.
 */
import { useState } from "react"
import { CheckCircle2, ExternalLink, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogFooter,
  AlertDialogHeader,
  AlertDialogTitle,
} from "@/components/ui/alert-dialog"
import { createMvAtApproval, decideMvProposal, probeMvEntitlement } from "@/lib/api"
import type { MvProbeResult, MvProposal } from "@/types"

export interface MvAcceptFlowProps {
  proposal: MvProposal
  /** Present on the run-output surface; recorded on the approve-for-later path. */
  runId?: string | null
  /** Databricks host, for the Catalog Explorer deep link on the success state. */
  databricksHost?: string | null
  /** Opens the run setup (create-at-trigger) — the [Start an optimization run]
      success CTA and the degrade path both offer it. */
  onStartRun?: (proposal: MvProposal) => void
  /** Fired after a successful create so the surface can refresh its counts. */
  onCreated?: (proposal: MvProposal) => void
  /** Fired after a successful approve-for-later decision. */
  onApprovedForLater?: (proposal: MvProposal) => void
}

type FlowStatus =
  | "idle"
  | "probing"
  | "consent"
  | "creating"
  | "created"
  | "degraded"
  | "approving"
  | "approved"
  | "error"

// The proposed identifier is `catalog.schema.name`; the probe targets the
// schema the view will be created in, and the create route re-derives the same
// from the consent, so this parse is display/probe-only.
export function proposalTarget(
  proposal: MvProposal,
): { catalog: string; schema: string } | null {
  const parts = (proposal.proposed_object ?? "").split(".")
  if (parts.length < 3) return null
  return { catalog: parts[0], schema: parts[1] }
}

function sourceTablesOf(proposal: MvProposal): string[] {
  const raw = proposal.evidence?.source_tables
  return Array.isArray(raw) ? raw.map(String) : []
}

// The GRANT the view's audience needs to query it, prefilled from the ACL
// (fix #3). With a real grantee it names them; empty ACL keeps the honest
// `<grantee>` placeholder rather than inventing a principal.
function grantPreview(fullName: string, grantee: string | null): string {
  const who = grantee ? `\`${grantee}\`` : "`<grantee>`"
  return `GRANT SELECT ON VIEW ${fullName} TO ${who}`
}

function catalogExplorerUrl(host: string | null | undefined, fullName: string): string | null {
  if (!host) return null
  const base = host.replace(/\/+$/, "")
  const path = fullName.split(".").map(encodeURIComponent).join("/")
  return `${base}/explore/data/${path}`
}

export function MvAcceptFlow({
  proposal,
  runId,
  databricksHost,
  onStartRun,
  onCreated,
  onApprovedForLater,
}: MvAcceptFlowProps) {
  const alreadyApproved = proposal.approved_for_rerun
  const [status, setStatus] = useState<FlowStatus>(alreadyApproved ? "approved" : "idle")
  const [probe, setProbe] = useState<MvProbeResult | null>(null)
  const [grantee, setGrantee] = useState<string | null>(null)
  const [createdName, setCreatedName] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  const target = proposalTarget(proposal)
  const fullName = proposal.proposed_object ?? ""

  // Click [Create this metric view] → fresh probe (no consent yet). SUFFICIENT
  // opens the consent modal; anything else degrades to approve-for-later.
  async function handleCreateClick() {
    if (!target) {
      setStatus("degraded")
      setError("This proposal has no resolvable target schema.")
      return
    }
    setStatus("probing")
    setError(null)
    try {
      const res = await probeMvEntitlement({
        catalog: target.catalog,
        schema: target.schema,
        space_id: proposal.target_space_id,
        source_tables: sourceTablesOf(proposal),
      })
      setProbe(res)
      setGrantee(res.audience_grantees[0] ?? null)
      if (res.verdict === "SUFFICIENT") setStatus("consent")
      else setStatus("degraded")
    } catch (e) {
      setStatus("degraded")
      setError(e instanceof Error ? e.message : "Entitlement probe failed.")
    }
  }

  // Explicit consent confirm → re-probe with materialize_consented (records the
  // consent, MV-D16), then create under OBO through the mv_create seam.
  async function handleConfirmCreate() {
    if (!target) return
    setStatus("creating")
    setError(null)
    try {
      const consented = await probeMvEntitlement({
        catalog: target.catalog,
        schema: target.schema,
        space_id: proposal.target_space_id,
        source_tables: sourceTablesOf(proposal),
        materialize_consented: true,
      })
      if (consented.verdict !== "SUFFICIENT") {
        setProbe(consented)
        setStatus("degraded")
        return
      }
      const res = await createMvAtApproval(proposal.target_space_id, {
        suggestion_id: proposal.suggestion_id,
        probe_id: consented.probe_id,
      })
      if (res.created) {
        setCreatedName(res.full_name)
        setStatus("created")
        onCreated?.(proposal)
      } else if (res.degraded) {
        setProbe((p) => (p ? { ...p, remediation_sql: res.remediation_sql } : p))
        setStatus("degraded")
        setError(res.reason)
      } else {
        setStatus("error")
        setError(res.reason ?? "Create failed.")
      }
    } catch (e) {
      setStatus("error")
      setError(e instanceof Error ? e.message : "Create failed.")
    }
  }

  // The classic MV-D1 path, retained on BOTH surfaces: record the approval so
  // create-at-trigger can pick it up on the next run.
  async function handleApproveForLater() {
    setStatus("approving")
    setError(null)
    try {
      const res = await decideMvProposal(proposal.suggestion_id, {
        space_id: proposal.target_space_id,
        run_id: runId ?? undefined,
        decision: "approved",
      })
      if (res.approved_for_rerun) {
        setStatus("approved")
        onApprovedForLater?.(proposal)
      } else {
        setStatus("error")
        setError("The approval was not recorded.")
      }
    } catch (e) {
      setStatus("error")
      setError(e instanceof Error ? e.message : "Could not record the decision.")
    }
  }

  // ── Terminal: created ─────────────────────────────────────────────────────
  if (status === "created") {
    const url = catalogExplorerUrl(databricksHost, createdName ?? fullName)
    return (
      <div className="space-y-2">
        <span className="inline-flex items-center gap-1 text-xs font-medium text-success">
          <CheckCircle2 className="h-3.5 w-3.5" />
          Created · will be attached and measured on the next run
        </span>
        <div className="flex flex-wrap items-center gap-2">
          {url && (
            <a
              href={url}
              target="_blank"
              rel="noreferrer"
              className="inline-flex items-center gap-1 text-xs font-medium text-accent hover:underline"
            >
              View in Catalog Explorer
              <ExternalLink className="h-3 w-3" />
            </a>
          )}
          <Button size="sm" onClick={() => onStartRun?.(proposal)}>
            Start an optimization run
          </Button>
        </div>
      </div>
    )
  }

  // ── Terminal: approved-for-later ──────────────────────────────────────────
  if (status === "approved") {
    return (
      <span className="inline-flex items-center gap-1 text-xs font-medium text-success">
        <CheckCircle2 className="h-3.5 w-3.5" />
        Approved for the next run
      </span>
    )
  }

  const busy = status === "probing" || status === "creating" || status === "approving"

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        {status === "degraded" || status === "approving" ? (
          <Button size="sm" disabled={busy} onClick={handleApproveForLater}>
            {status === "approving" ? "Approving…" : "Approve for later"}
          </Button>
        ) : (
          <Button size="sm" disabled={busy} onClick={handleCreateClick}>
            {status === "probing" ? (
              <>
                <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
                Checking your access…
              </>
            ) : (
              "Create this metric view"
            )}
          </Button>
        )}
        {onStartRun && (
          <Button size="sm" variant="ghost" onClick={() => onStartRun(proposal)}>
            Review in run setup
          </Button>
        )}
      </div>

      {/* Degrade (MV-D34.c): never a dead end — show the exact GRANT that would
          make create-at-approval succeed, copy-ready, alongside the retained
          approve-for-later path above. */}
      {status === "degraded" && probe?.remediation_sql && (
        <div className="rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2">
          <p className="text-xs text-amber-700 dark:text-amber-300">
            Your access to <span className="font-mono">{target?.schema}</span> isn&rsquo;t
            sufficient to create this yet. Ask an admin to run:
          </p>
          <pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-all font-mono text-[11px] text-amber-800 dark:text-amber-200">
            {probe.remediation_sql}
          </pre>
        </div>
      )}
      {error && status !== "degraded" && <p className="text-xs text-danger">{error}</p>}

      {/* Consent modal (MV-D34.b): target shown, GRANT preview with the ACL-
          derived grantee, an explicit confirm that records consent and creates. */}
      <AlertDialog open={status === "consent"} onOpenChange={(o) => !o && setStatus("idle")}>
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Create this metric view?</AlertDialogTitle>
            <AlertDialogDescription>
              This creates <span className="font-mono text-secondary">{fullName}</span> under
              your identity, now. It will be attached and measured on the next optimization run.
            </AlertDialogDescription>
          </AlertDialogHeader>

          <div className="mt-3 space-y-3">
            {probe && probe.audience_grantees.length > 1 && (
              <label className="block text-xs text-muted">
                Grant query access to
                <select
                  className="mt-1 w-full rounded-md border border-default bg-surface px-2 py-1.5 text-xs text-primary"
                  value={grantee ?? ""}
                  onChange={(e) => setGrantee(e.target.value || null)}
                >
                  {probe.audience_grantees.map((g) => (
                    <option key={g} value={g}>
                      {g}
                    </option>
                  ))}
                </select>
              </label>
            )}
            <div>
              <p className="text-xs text-secondary">Audience GRANT (copy-ready — the app never runs it):</p>
              <pre className="mt-1 overflow-x-auto whitespace-pre-wrap break-all rounded-md border border-default bg-elevated px-2 py-1.5 font-mono text-[11px] text-muted">
                {grantPreview(fullName, grantee)}
              </pre>
            </div>
          </div>

          <AlertDialogFooter>
            <AlertDialogCancel onClick={() => setStatus("idle")}>Cancel</AlertDialogCancel>
            <AlertDialogAction disabled={status === "creating"} onClick={handleConfirmCreate}>
              {status === "creating" ? "Creating…" : "Create metric view"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
