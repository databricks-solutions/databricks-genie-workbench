import { AlertTriangle, Check, Copy, Loader2, Sparkles } from "lucide-react"
import { Checkbox } from "@/components/ui/checkbox"
import { Button } from "@/components/ui/button"
import type { MvProposal, MvProbeResult } from "@/types"

// The "Suggest metric views" run-config section (Prompt 11, MV-D1/D23) —
// mockups 1–3 implemented for real (docs/design/mockups/1..3, POV §7.3). Purely
// presentational: OptimizationConfig owns the state, the space-scoped proposals
// fetch, and the OBO probe; this component only renders and reports intent, so
// every state (first-run / re-run granted / re-run denied / probing) is testable
// with `renderToStaticMarkup` and fixture props, matching the repo's test idiom.
//
// There is deliberately NO "Also materialize" control: the materialization path
// is unbuilt (mv-advisor-gap-report.md:1526), and a disabled control for an
// unbuilt feature would advertise vapor. The field stays plumbed through
// buildOptimizationTriggerRequest; a later prompt adds the control.

export interface MvSuggestSectionProps {
  enabled: boolean
  onToggle: (enabled: boolean) => void
  /** loading || hasActiveRun — mirrors the rest of the form's disabled gate. */
  disabled?: boolean
  proposalsLoading: boolean
  /** Space-scoped, approved-for-rerun proposals (MV-D23). Empty ⇒ first-run. */
  proposals: MvProposal[]
  selectedProposalIds: Set<string>
  onToggleProposal: (suggestionId: string) => void
  mode: "suggest_only" | "create_and_attach"
  onModeChange: (mode: "suggest_only" | "create_and_attach") => void
  /** catalog.schema derived from the approved proposals (deriveMvTarget). */
  target: { catalog: string; schema: string } | null
  probe: MvProbeResult | null
  probeLoading: boolean
  probeError: string | null
  /** Copies probe.remediation_sql to the clipboard (owned by the parent). */
  onCopyGrant: () => void
}

function targetLabel(target: { catalog: string; schema: string } | null): string {
  return target ? `${target.catalog}.${target.schema}` : ""
}

export function MvSuggestSection(props: MvSuggestSectionProps) {
  const {
    enabled,
    onToggle,
    disabled,
    proposalsLoading,
    proposals,
    selectedProposalIds,
    onToggleProposal,
    mode,
    onModeChange,
    target,
    probe,
    probeLoading,
    probeError,
    onCopyGrant,
  } = props

  const isFirstRun = proposals.length === 0
  const granted = probe?.verdict === "SUFFICIENT"
  const label = targetLabel(target)

  return (
    <div className="space-y-4 rounded-lg border border-default bg-surface-subtle px-4 py-3">
      <label className="flex cursor-pointer items-start gap-2">
        <Checkbox
          checked={enabled}
          onCheckedChange={(checked) => onToggle(checked === true)}
          disabled={disabled}
          className="mt-0.5"
        />
        <span>
          <span className="flex items-center gap-1.5 text-sm font-medium text-primary">
            <Sparkles className="h-3.5 w-3.5 text-accent" />
            Suggest metric views
          </span>
          <span className="mt-0.5 block text-xs text-muted">
            The optimizer will look for un-governed measures in this space&rsquo;s generated SQL and
            propose metric views for them.
          </span>
        </span>
      </label>

      {enabled && (
        <div className="space-y-4 border-t border-default pt-3">
          {proposalsLoading ? (
            <p className="flex items-center gap-1.5 text-xs text-muted">
              <Loader2 className="h-3 w-3 animate-spin" />
              Checking this Agent for previously approved proposals&hellip;
            </p>
          ) : isFirstRun ? (
            <FirstRunModes disabled={disabled} mode={mode} onModeChange={onModeChange} />
          ) : (
            <>
              <div className="space-y-2">
                <p className="text-xs font-medium text-secondary">Approved for this Agent</p>
                {proposals.map((proposal) => (
                  <label key={proposal.suggestion_id} className="flex items-start gap-2">
                    <Checkbox
                      checked={selectedProposalIds.has(proposal.suggestion_id)}
                      onCheckedChange={() => onToggleProposal(proposal.suggestion_id)}
                      disabled={disabled}
                      className="mt-0.5"
                    />
                    <span className="font-mono text-sm text-primary">
                      {proposal.proposed_object ?? proposal.suggestion_id}
                    </span>
                  </label>
                ))}
              </div>

              {label && (
                <p className="text-xs font-medium text-secondary">
                  Target: <span className="font-mono text-primary">{label}</span>
                </p>
              )}

              {probeLoading ? (
                <p className="text-xs text-muted">
                  Checking your permissions{label ? ` in ${label}` : ""}&hellip;
                </p>
              ) : probeError ? (
                <p className="text-xs text-danger">{probeError}</p>
              ) : granted ? (
                <div className="flex items-start gap-1.5 rounded-lg border border-success/30 bg-success/10 px-3 py-2 text-xs text-success-foreground">
                  <Check className="mt-0.5 h-3.5 w-3.5 shrink-0 text-success" />
                  <span>
                    You can create metric views in <span className="font-mono">{label}</span>.{" "}
                    {probe?.checked_as && (
                      <span className="text-muted">(Checked as {probe.checked_as})</span>
                    )}
                  </span>
                </div>
              ) : probe ? (
                <MvDenialBanner probe={probe} target={label} onCopyGrant={onCopyGrant} />
              ) : null}

              <RerunModes
                disabled={disabled}
                mode={mode}
                onModeChange={onModeChange}
                createEnabled={granted}
                target={label}
              />
            </>
          )}
        </div>
      )}
    </div>
  )
}

function FirstRunModes({
  disabled,
  mode,
  onModeChange,
}: {
  disabled?: boolean
  mode: "suggest_only" | "create_and_attach"
  onModeChange: (mode: "suggest_only" | "create_and_attach") => void
}) {
  return (
    <fieldset className="space-y-2">
      <label className="flex items-start gap-2 text-sm text-primary">
        <input
          type="radio"
          name="mv-mode"
          checked={mode === "suggest_only"}
          onChange={() => onModeChange("suggest_only")}
          disabled={disabled}
          className="mt-0.5"
        />
        <span>
          <span className="font-medium">Suggest only.</span>{" "}
          <span className="text-muted">Show me the DDL in the run output. Nothing is created.</span>
        </span>
      </label>
      {/* Create and attach is disabled on the first run because it becomes
          available through something the user can do — approve a proposal
          (MV-D1). That is why this control is disabled-with-rationale and the
          absent materialize control is not: this feature exists. */}
      <label className="flex items-start gap-2 text-sm text-muted opacity-60" aria-disabled>
        <input type="radio" name="mv-mode" disabled className="mt-0.5" />
        <span>
          <span className="font-medium">Create and attach.</span>{" "}
          <span>Available after this run produces proposals you approve.</span>
        </span>
      </label>
    </fieldset>
  )
}

function RerunModes({
  disabled,
  mode,
  onModeChange,
  createEnabled,
  target,
}: {
  disabled?: boolean
  mode: "suggest_only" | "create_and_attach"
  onModeChange: (mode: "suggest_only" | "create_and_attach") => void
  createEnabled: boolean
  target: string
}) {
  return (
    <fieldset className="space-y-2 border-t border-default pt-3">
      <label className="flex items-start gap-2 text-sm text-primary">
        <input
          type="radio"
          name="mv-mode"
          checked={mode === "suggest_only"}
          onChange={() => onModeChange("suggest_only")}
          disabled={disabled}
          className="mt-0.5"
        />
        <span>
          <span className="font-medium">Suggest only.</span>{" "}
          <span className="text-muted">Show me the DDL in the run output. Nothing is created.</span>
        </span>
      </label>
      <label
        className={`flex items-start gap-2 text-sm ${createEnabled ? "text-primary" : "text-muted opacity-60"}`}
        aria-disabled={!createEnabled}
      >
        <input
          type="radio"
          name="mv-mode"
          checked={mode === "create_and_attach"}
          onChange={() => onModeChange("create_and_attach")}
          disabled={disabled || !createEnabled}
          className="mt-0.5"
        />
        <span>
          <span className="font-medium">Create and attach, then optimize.</span>{" "}
          <span className={createEnabled ? "text-muted" : undefined}>
            {createEnabled
              ? `Create approved metric views in ${target}, add them to this Genie Agent, then optimize the space with them in place.`
              : "Available once you have permission to create metric views in the target schema."}
          </span>
        </span>
      </label>
    </fieldset>
  )
}

function MvDenialBanner({
  probe,
  target,
  onCopyGrant,
}: {
  probe: MvProbeResult
  target: string
  onCopyGrant: () => void
}) {
  const missing = probe.missing.length > 0 ? probe.missing.join(", ") : "the required privileges"
  return (
    <div className="space-y-3 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-3">
      <div className="flex items-start gap-1.5 text-sm text-amber-700 dark:text-amber-300">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
        <span>
          <span className="font-medium">
            You don&rsquo;t have permission to create metric views in{" "}
            <span className="font-mono">{target}</span>.
          </span>{" "}
          Missing: <span className="font-mono">{missing}</span>. The run will continue in{" "}
          <span className="font-medium">Suggest only</span> mode and show you the DDL at the end.
        </span>
      </div>
      {probe.remediation_sql && (
        <>
          <div className="flex flex-wrap gap-2">
            <Button size="sm" variant="secondary" onClick={onCopyGrant}>
              <Copy className="mr-1.5 h-3.5 w-3.5" />
              Copy grant request
            </Button>
          </div>
          <textarea
            readOnly
            aria-label="Metric view grant request"
            value={probe.remediation_sql}
            className="min-h-16 w-full resize-y rounded-md border border-default bg-surface p-2 font-mono text-xs text-primary"
          />
        </>
      )}
    </div>
  )
}
