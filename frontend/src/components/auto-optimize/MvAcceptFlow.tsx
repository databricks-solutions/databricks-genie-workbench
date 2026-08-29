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
 * under the user's identity. That route now ALSO attaches the view to the Agent
 * config under OBO (MV-D34 attach-at-approval) — the config is the source of
 * truth, so the semantic model and suggestions reflect it immediately, with no
 * "attach on some later run" indirection. Nothing here talks to the SP.
 *
 * Terminal outcomes, never a dead end (MV-D34.c):
 *   - created   → "Created & attached to your Agent" + the one SP grant a run
 *                 needs to read it + Catalog Explorer + [Start an optimization
 *                 run]. If the config PATCH failed (no CAN EDIT), it degrades to
 *                 "Created — not yet attached" with how to attach it in Genie.
 *   - attached  → opened this way from the list for a proposal already on the
 *                 config; states it plainly and offers the grant.
 *   - degraded  → the fresh probe fell below SUFFICIENT; the button becomes
 *                 [Approve for later] (the classic MV-D1 path) with the
 *                 remediation GRANT shown copy-ready.
 *   - approved  → the classic approve-for-rerun decision was recorded.
 */
import { useState } from "react"
import { CheckCircle2, ExternalLink, Link2, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { SqlCodeBlock } from "@/components/SqlCodeBlock"
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
  /** Opens the run setup — offered ONLY by the [Start an optimization run] CTA on
      the created terminal (the endorsed create→optimize flow). The pre-create
      "[Review in run setup]" jump was removed (deployed review): from an un-created
      proposal it led to an empty run gate, so IQ scan stays self-contained. */
  onStartRun?: (proposal: MvProposal) => void
  /** Fired after a successful create so the surface can refresh its counts. */
  onCreated?: (proposal: MvProposal) => void
  /** Fired after a successful approve-for-later decision. */
  onApprovedForLater?: (proposal: MvProposal) => void
  /** The resolved, copy-ready `GRANT SELECT … TO <optimizer SP>` (from the
      proposal's DDL artifact). Option A: the grant is a POST-create step — you
      can't grant on a view that doesn't exist yet — so it is surfaced in the
      CREATED terminal, not in the pre-create consent modal. Absent when the
      card's best-effort DDL fetch hasn't resolved; the terminal then points to
      the card's "Show detail" for it. */
  grantSql?: string | null
  /** Tertiary affordance (the MV-D24 "I created this myself" claim) rendered
      by the flow so it lives inside the flow's state machine: shown in the
      pre-create states (action / degraded / approved), and HIDDEN once the view
      is created or attached — a created-and-attached view is not something the
      user still needs to claim. Absent on surfaces that don't offer the claim. */
  claimAffordance?: React.ReactNode
}

type FlowStatus =
  | "idle"
  | "probing"
  | "consent"
  | "creating"
  | "created"
  | "attached"
  | "degraded"
  | "approving"
  | "approved"
  | "error"

// The proposed identifier is `catalog.schema.name`; the probe targets the
// schema the view will be created in, and the create route re-derives the same
// from the consent, so this parse is display/probe-only.
function proposalTarget(
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
  grantSql,
  claimAffordance,
}: MvAcceptFlowProps) {
  // TERMINAL-STATES-MUST-BE-EARNED (Prompt 15.9): the approved terminal
  // initializes ONLY from a persisted record of the user's OWN action — a
  // recorded `decision === "approved"` — never from the derived
  // `approved_for_rerun` gate. That gate is a re-run eligibility flag another
  // principal or a coverage read can set, and a stringified-boolean coercion
  // (`bool("false")`) once forced it true for EVERY row, opening this card on
  // the approved terminal and masking [Create this metric view] on first paint
  // (the MV-D34-invisible bug). An un-acted proposal has no decision, so it
  // opens on the ACTION state.
  // MV-D34 attach-at-approval: a proposal already shelved on the Agent config
  // (the source of truth) opens on the ATTACHED terminal — it is not something
  // to create again. This wins over the approved terminal (attached is the
  // stronger, later state) and over the action state.
  const alreadyAttached = proposal.attached === true
  const alreadyApproved = proposal.decision === "approved"
  const [status, setStatus] = useState<FlowStatus>(
    alreadyAttached ? "attached" : alreadyApproved ? "approved" : "idle",
  )
  const [probe, setProbe] = useState<MvProbeResult | null>(null)
  const [createdName, setCreatedName] = useState<string | null>(null)
  // Whether the create call also attached the view to the Agent config, and the
  // grant it returned. `grant_sql` from the response is preferred over the DDL-
  // artifact prop so the terminal shows a grant even when the card's best-effort
  // DDL fetch never resolved.
  const [attached, setAttached] = useState<boolean>(false)
  // MV-D34 idempotent re-approval: the view already existed and this call only
  // (re)attached it, so the terminal says "attached an existing view" rather than
  // claiming a fresh create.
  const [alreadyExisted, setAlreadyExisted] = useState<boolean>(false)
  const [createdGrantSql, setCreatedGrantSql] = useState<string | null>(null)
  // Workspace URL the create response resolved, so the created terminal can
  // deep-link the new view in Catalog Explorer even when no `databricksHost`
  // prop was threaded down to this surface (the common case on the IQ scan).
  const [createdWorkspaceHost, setCreatedWorkspaceHost] = useState<string | null>(null)
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
        setAttached(res.attached)
        setAlreadyExisted(res.already_existed)
        setCreatedGrantSql(res.grant_sql)
        setCreatedWorkspaceHost(res.workspace_host)
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

  const busy = status === "probing" || status === "creating" || status === "approving"
  // Prefer the grant the create response resolved; fall back to the proposal's
  // DDL-artifact prop (present on the card even before any create).
  const effectiveGrantSql = createdGrantSql ?? grantSql

  // ── Terminal: created ─────────────────────────────────────────────────────
  // MV-D34 attach-at-approval: create now ALSO shelves the view on the Agent
  // config, so the terminal leads with the attach outcome. Attached: the Agent
  // config (the source of truth) already reflects it, so the semantic model and
  // these suggestions update with no further action — the one remaining step is
  // the SP grant so a later optimization run can READ and measure it. Created-
  // not-attached (PATCH failed, e.g. no CAN EDIT): the view exists but is not on
  // the Agent, so the copy says so honestly and points to adding it in Genie.
  if (status === "created") {
    // Prefer the host the create response resolved; fall back to the prop.
    const url = catalogExplorerUrl(createdWorkspaceHost ?? databricksHost, createdName ?? fullName)
    return (
      // w-full min-w-0 so this terminal fills the card's flex `actions` row and
      // the long GRANT scrolls WITHIN the SQL block instead of forcing the card
      // to overflow (a flex item's default min-width:auto refused to shrink).
      <div className="w-full min-w-0 space-y-2.5">
        {attached ? (
          <span className="inline-flex items-center gap-1 text-xs font-medium text-success">
            <CheckCircle2 className="h-3.5 w-3.5" />
            {alreadyExisted
              ? "Attached to your Agent (view already existed)"
              : "Created \u0026 attached to your Agent"}
          </span>
        ) : (
          <span className="inline-flex items-center gap-1 text-xs font-medium text-amber-600 dark:text-amber-400">
            <CheckCircle2 className="h-3.5 w-3.5" />
            {alreadyExisted ? "View exists — not yet attached" : "Created — not yet attached"}
          </span>
        )}
        <div className="rounded-lg border border-default bg-elevated px-3 py-2.5 space-y-2">
          {attached ? (
            <p className="text-xs text-secondary">
              It&rsquo;s on your Agent&rsquo;s metric views now, so the semantic model and these
              suggestions already reflect it.{" "}
              <span className="font-medium text-primary">One step left:</span> the optimizer runs as a
              separate service principal, so grant it
              <span className="font-mono"> SELECT</span> to let an optimization run read and measure it.
            </p>
          ) : (
            <p className="text-xs text-secondary">
              <span className="font-medium text-primary">The view exists, but couldn&rsquo;t be
              added to the Agent automatically</span> — you may not have edit access. Add it to the
              Agent&rsquo;s metric views in Genie (or ask an editor), then grant the optimizer
              <span className="font-mono"> SELECT</span> so a run can read it.
            </p>
          )}
          {effectiveGrantSql ? (
            <SqlCodeBlock code={effectiveGrantSql} />
          ) : (
            <p className="text-[11px] text-muted">
              The copy-ready <span className="font-mono">GRANT</span> statement is on the proposal
              card under &ldquo;Show detail&rdquo;. The app never runs it.
            </p>
          )}
        </div>
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

  // ── Terminal: already attached (opened this way from the list) ─────────────
  // A proposal the space-scoped list marked as on the Agent config. It is not a
  // create action — state it plainly and offer the grant (from the DDL prop) so
  // the optimizer can read it, plus the Catalog Explorer jump.
  if (status === "attached") {
    const url = catalogExplorerUrl(databricksHost, fullName)
    return (
      <div className="w-full min-w-0 space-y-2.5">
        <span className="inline-flex items-center gap-1 text-xs font-medium text-success">
          <Link2 className="h-3.5 w-3.5" />
          Attached to your Agent
        </span>
        <p className="text-xs text-secondary">
          This metric view is on your Agent&rsquo;s config, so the semantic model and these
          suggestions already reflect it. To have an optimization run measure it, grant the optimizer
          service principal <span className="font-mono">SELECT</span>.
        </p>
        {effectiveGrantSql && <SqlCodeBlock code={effectiveGrantSql} />}
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
      </div>
    )
  }

  // ── Terminal: approved-for-later ──────────────────────────────────────────
  // Not a dead end (MV-D34.c / Prompt 15.9): an approved-for-later proposal can
  // still be created now if entitlement allows, so the terminal keeps offering
  // [Create it now] — it runs the same probe → consent → create flow, which
  // moves status off "approved" and renders the live action/consent UI below.
  if (status === "approved") {
    return (
      <div className="space-y-2">
        <span className="inline-flex items-center gap-1 text-xs font-medium text-success">
          <CheckCircle2 className="h-3.5 w-3.5" />
          Approved for the next run
        </span>
        <div className="flex flex-wrap items-center gap-2">
          <Button size="sm" onClick={handleCreateClick}>
            Create it now
          </Button>
          {claimAffordance}
        </div>
      </div>
    )
  }

  return (
    <div className="w-full min-w-0 space-y-2">
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
            ) : status === "creating" ? (
              <>
                <Loader2 className="mr-1.5 h-3.5 w-3.5 animate-spin" />
                Creating…
              </>
            ) : (
              "Create this metric view"
            )}
          </Button>
        )}
        {/* Tertiary claim, HIDDEN in the created/attached terminals above — a
            created-and-attached view is not something to still "create myself". */}
        {claimAffordance}
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

      {/* Consent modal (MV-D34.b): create-and-attach under your identity. The
          copy now states attach happens NOW (create-and-attach-at-approval), not
          on a later run — the config is the source of truth, so the semantic
          model reflects it immediately; the one post-create step is the SP grant,
          surfaced in the CREATED terminal (it isn't runnable until the view
          exists). The modal stays OPEN through "creating" so the confirm button's
          "Creating…" is visible — previously status flipped to "creating", the
          modal (keyed to "consent") closed, and the user was left on a disabled,
          status-less button that looked hung (deployed review). */}
      <AlertDialog
        open={status === "consent" || status === "creating"}
        onOpenChange={(o) => {
          // Never dismiss mid-create; a click-away/ESC while creating must not
          // strand the flow or fire a second create.
          if (!o && status !== "creating") setStatus("idle")
        }}
      >
        <AlertDialogContent>
          <AlertDialogHeader>
            <AlertDialogTitle>Create and attach this metric view?</AlertDialogTitle>
            <AlertDialogDescription>
              Runs under your identity. This will:
            </AlertDialogDescription>
            {/* Bulleted, so the three concrete effects read at a glance instead of
                a run-on sentence (deployed review). Kept OUTSIDE the description's
                <p> — a <ul> nested in a <p> is invalid markup. */}
            <ul className="mt-1 space-y-1.5 text-xs text-secondary">
              {[
                "Create the metric view in Unity Catalog, owned by you.",
                "Attach it to this Agent\u2019s config \u2014 the source of truth, so the semantic model and suggestions update immediately.",
                "Return one GRANT to run afterward, so an optimization run can read and measure it.",
              ].map((line) => (
                <li key={line} className="flex gap-2">
                  <span className="mt-1.5 h-1 w-1 shrink-0 rounded-full bg-accent/60" aria-hidden />
                  <span className="min-w-0">{line}</span>
                </li>
              ))}
            </ul>
            {/* The FQN is long and has no spaces to wrap on; render it as its own
                break-all block so it never overflows the modal (deployed review). */}
            <p className="mt-2 break-all rounded-md border border-default bg-elevated px-2 py-1.5 font-mono text-xs text-secondary">
              {fullName}
            </p>
          </AlertDialogHeader>

          {status === "creating" && (
            <p className="mt-3 inline-flex items-center gap-1.5 text-xs text-secondary">
              <Loader2 className="h-3.5 w-3.5 animate-spin text-accent" />
              Creating the view and attaching it to your Agent…
            </p>
          )}

          <AlertDialogFooter>
            <AlertDialogCancel disabled={status === "creating"} onClick={() => setStatus("idle")}>
              Cancel
            </AlertDialogCancel>
            <AlertDialogAction disabled={status === "creating"} onClick={handleConfirmCreate}>
              {status === "creating" ? "Creating…" : "Create and attach"}
            </AlertDialogAction>
          </AlertDialogFooter>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  )
}
