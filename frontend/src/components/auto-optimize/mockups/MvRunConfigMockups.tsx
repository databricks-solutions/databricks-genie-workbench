/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * Frames 1–3: the "Suggest metric views" section of the Auto-Optimize run-config
 * panel. Extends the consent-checkbox-with-amber-warning idiom already in
 * OptimizationConfig.tsx:141-164. Copy is verbatim POV §7.3
 * (metric-view-suggestion-engine-pov.md:376-398); the first-run disabled
 * rationale is MV-D1 (mv-advisor-playbook.md:307). Prompt 11 implements these
 * for real and deletes this file.
 */
import { AlertTriangle, Check, Copy, Sparkles } from "lucide-react"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Checkbox } from "@/components/ui/checkbox"
import { Button } from "@/components/ui/button"
import { consentGranted, proposalMargin, proposalRevenue } from "./mvMockData"

function SectionShell({ children }: { children: React.ReactNode }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle className="flex items-center gap-2 text-base">
          <Sparkles className="h-4 w-4 text-accent" />
          Suggest metric views
        </CardTitle>
      </CardHeader>
      <CardContent className="space-y-4">{children}</CardContent>
    </Card>
  )
}

function ToggleRow() {
  return (
    <label className="flex cursor-pointer items-start gap-2">
      <Checkbox checked readOnly className="mt-0.5" />
      <span>
        <span className="block text-sm font-medium text-primary">Suggest metric views</span>
        <span className="mt-0.5 block text-xs text-muted">
          The optimizer will look for un-governed measures in this space&rsquo;s generated SQL and
          propose metric views for them.
        </span>
      </span>
    </label>
  )
}

function TargetPicker({ target }: { target: string }) {
  const [catalog, schema] = target.split(".")
  return (
    <div className="space-y-1.5">
      <p className="text-xs font-medium text-secondary">Where should metric views be created?</p>
      <div className="flex items-center gap-2 font-mono text-sm">
        <span className="rounded-md border border-default bg-surface px-2.5 py-1 text-primary">{catalog} &#9662;</span>
        <span className="text-muted">.</span>
        <span className="rounded-md border border-default bg-surface px-2.5 py-1 text-primary">{schema} &#9662;</span>
      </div>
    </div>
  )
}

// ── Frame 1 — first run: "Create and attach" DISABLED (MV-D1) ───────────────
export function FirstRunConfigFrame() {
  return (
    <SectionShell>
      <ToggleRow />
      <TargetPicker target="finance.sales" />
      <fieldset className="space-y-2 border-t border-default pt-3">
        <label className="flex items-start gap-2 text-sm text-primary">
          <input type="radio" name="mv-mode-1" checked readOnly className="mt-0.5" />
          <span>
            <span className="font-medium">Suggest only.</span>{" "}
            <span className="text-muted">Show me the DDL in the run output. Nothing is created.</span>
          </span>
        </label>
        <label className="flex items-start gap-2 text-sm text-muted opacity-60" aria-disabled>
          <input type="radio" name="mv-mode-1" disabled className="mt-0.5" />
          <span>
            <span className="font-medium">Create and attach.</span>{" "}
            <span>Available after this run produces proposals you approve.</span>
          </span>
        </label>
      </fieldset>
    </SectionShell>
  )
}

// ── Frame 2 — re-run: approved proposals for THIS AGENT (space-scoped) ───────
// Source label is intentionally "Approved for this Agent", never "from run …":
// the amended Prompt 11 reads approved proposals from a space-scoped route
// (GET /spaces/{space_id}/mv-proposals?approved_for_rerun=true), so a run label
// here would encode the data source that amendment removed (MV-D23).
export function RerunConfigFrame() {
  const approved = [proposalRevenue, proposalMargin]
  return (
    <SectionShell>
      <ToggleRow />
      <div className="space-y-2 border-t border-default pt-3">
        <p className="text-xs font-medium text-secondary">Approved for this Agent</p>
        {approved.map((p) => (
          <label key={p.suggestion_id} className="flex items-start gap-2">
            <Checkbox checked readOnly className="mt-0.5" />
            <span className="font-mono text-sm text-primary">{p.proposed_object}</span>
          </label>
        ))}
      </div>

      <TargetPicker target={consentGranted.target} />

      <div className="flex items-start gap-1.5 rounded-lg border border-success/30 bg-success/10 px-3 py-2 text-xs text-success-foreground">
        <Check className="mt-0.5 h-3.5 w-3.5 shrink-0 text-success" />
        <span>
          You can create metric views in <span className="font-mono">{consentGranted.target}</span>.{" "}
          <span className="text-muted">(Checked as {consentGranted.granted_by})</span>
        </span>
      </div>

      <fieldset className="space-y-2 border-t border-default pt-3">
        <label className="flex items-start gap-2 text-sm text-primary">
          <input type="radio" name="mv-mode-2" readOnly className="mt-0.5" />
          <span>
            <span className="font-medium">Suggest only.</span>{" "}
            <span className="text-muted">Show me the DDL in the run output. Nothing is created.</span>
          </span>
        </label>
        <label className="flex items-start gap-2 text-sm text-primary">
          <input type="radio" name="mv-mode-2" checked readOnly className="mt-0.5" />
          <span>
            <span className="font-medium">Create and attach, then optimize.</span>{" "}
            <span className="text-muted">
              Create approved metric views in <span className="font-mono">{consentGranted.target}</span>, add them to
              this Genie Agent, then optimize the space with them in place.
            </span>
          </span>
        </label>
      </fieldset>

      <label className="flex items-start gap-2 border-t border-default pt-3">
        <Checkbox className="mt-0.5" />
        <span>
          <span className="block text-sm text-primary">Also materialize</span>
          <span className="mt-1 flex items-start gap-1.5 text-xs text-amber-600 dark:text-amber-400">
            <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
            Starts a Lakeflow pipeline and incurs ongoing refresh cost. Off by default.
          </span>
        </span>
      </label>

      <div className="flex justify-end border-t border-default pt-3">
        <Button size="sm">Start optimization</Button>
      </div>
    </SectionShell>
  )
}

// The denial banner is extracted so frame 7c (IQ-Scan, not-entitled) can reuse
// it UNCHANGED, per the Prompt 10 spec ("reuses mockup 3's denial banner").
// Frame 8 (BYO refused, MV-D24) reuses the SAME amber banner with its own
// message/actions — no new visual language. Default (undefined) props render
// the entitlement copy byte-for-byte, so frames 3 and 7c are unaffected; pass
// `actions={null}` / `footnote={null}` to suppress those blocks.
export function DenialBanner({
  target = "finance.sales",
  message,
  actions,
  footnote,
}: {
  target?: string
  message?: React.ReactNode
  actions?: React.ReactNode | null
  footnote?: React.ReactNode | null
}) {
  const actionsContent =
    actions === undefined ? (
      <>
        <Button size="sm" variant="secondary">
          <Copy className="mr-1.5 h-3.5 w-3.5" />
          Copy grant request
        </Button>
        <Button size="sm" variant="outline">
          Choose a different schema
        </Button>
        <Button size="sm" variant="ghost">
          Continue in suggest-only mode
        </Button>
      </>
    ) : (
      actions
    )
  const footnoteContent =
    footnote === undefined ? (
      <>
        Copy grant request sends your admin the exact <span className="font-mono">GRANT</span> statement needed.
      </>
    ) : (
      footnote
    )
  return (
    <div className="space-y-3 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-3">
      <div className="flex items-start gap-1.5 text-sm text-amber-700 dark:text-amber-300">
        <AlertTriangle className="mt-0.5 h-4 w-4 shrink-0" />
        <span>
          {message ?? (
            <>
              <span className="font-medium">
                You don&rsquo;t have permission to create metric views in{" "}
                <span className="font-mono">{target}</span>.
              </span>{" "}
              Missing: <span className="font-mono">CREATE TABLE</span> on the schema. The run will continue in{" "}
              <span className="font-medium">Suggest only</span> mode and show you the DDL at the end.
            </>
          )}
        </span>
      </div>
      {actionsContent && <div className="flex flex-wrap gap-2">{actionsContent}</div>}
      {footnoteContent && <p className="text-xs text-muted">{footnoteContent}</p>}
    </div>
  )
}

// ── Frame 3 — denial (probe INSUFFICIENT) ───────────────────────────────────
export function DenialConfigFrame() {
  return (
    <SectionShell>
      <ToggleRow />
      <TargetPicker target="finance.sales" />
      <DenialBanner target="finance.sales" />
    </SectionShell>
  )
}
