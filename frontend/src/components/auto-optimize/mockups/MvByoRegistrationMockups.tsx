/**
 * MV-advisor mockups — REVIEW SCAFFOLD, NOT PRODUCTION CODE (see mvMockData.ts).
 *
 * Frame 8 (MV-D24 — bring-your-own registration): the path that stops copied
 * DDL from being a one-way exit. The suggest-only output hands the user a
 * copy-ready CREATE VIEW … WITH METRICS; the moment they run it themselves the
 * app goes blind — mv_attach skips any identifier without a CREATED ledger row
 * (mv_attach.py:478) and any row whose created_by mismatches the consent
 * (:484), and the ledger's only writer is the backend create path. Registration
 * closes this: the backend verifies the identifier under OBO (DESCRIBE EXTENDED
 * asserts Type: METRIC_VIEW, YAML recovered + mv_yaml.validate) and writes a
 * USER_CREATED ledger row so attach-and-lift picks it up. Prompt 13.5 builds
 * this for real and deletes this file.
 *
 * Three frames:
 *  (a) entry points — the tertiary [I created this myself] action on the
 *      suggest-only proposal card, plus the free-standing "Register an existing
 *      metric view" input on the IQ Scan advisory panel.
 *  (b) verified — Type: METRIC_VIEW confirmed, validation passed, USER_CREATED
 *      badge, and the registered copy. It renders NO [Drop view] action: the
 *      first MV-D24 invariant is that the app never drops a USER_CREATED view
 *      (drop refuses on provenance, not merely status).
 *  (c) refused — reuses frame 3's DenialBanner (no new visual language), two
 *      variants: "not a metric view" and "not visible to you". The second
 *      MV-D24 invariant applies to BOTH: an unverifiable identifier is refused
 *      with the reason, never recorded provisionally ("Nothing was recorded").
 *
 * Two things the real panel (Prompt 13.5) changes, noted so this mockup does
 * not become the spec:
 *   - The not-visible variant states the missing privileges (USE SCHEMA /
 *     SELECT) as PROSE. mv_entitlement.probe + _remediation_sql already render
 *     the exact GRANT, so Prompt 13.5 replaces the prose with a [Copy grant
 *     request] button — do not ship hand-written hints. It also does NOT offer
 *     "it may not exist": mv_entitlement resolves NOT_FOUND to DENIED (UC
 *     returns the same error for absent and invisible), so naming two
 *     hypotheses would send the user chasing the wrong one.
 *   - The verified state (b) carries a [Start an optimization run] affordance:
 *     the copy promises attach "on the next optimization run", and on the IQ
 *     Scan surface (where a never-optimized user meets this) there would
 *     otherwise be no way to start one — the same one-way-exit shape MV-D24
 *     exists to close, moved one step later.
 *
 * ⚠ COPY REVIEW REQUIRED: the verified sentence ("Registered. …") is VERBATIM
 * from the Prompt 10 review and must not drift. The input label/helper and both
 * refused-variant messages are AUTHORED NEW for this branch and need sign-off.
 */
import { ArrowUpRight, Check } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { MvProposalCard } from "./MvProposalCard"
import { LiftNotMeasuredLabel } from "./MvOutputMockups"
import { DenialBanner } from "./MvRunConfigMockups"
import { byoVerified, ddlRevenue, proposalRevenue } from "./mvMockData"

// VERBATIM from the Prompt 10 review — do not paraphrase.
const REGISTERED_COPY =
  "Registered. It will be attached and measured on the next optimization run. " +
  "The app never drops views it didn't create — dropping this one stays in your hands."

function RegisterInput() {
  return (
    <div className="space-y-2 rounded-xl border border-default bg-surface p-4">
      <p className="text-sm font-medium text-primary">Register an existing metric view</p>
      <p className="text-xs text-muted">
        Already created a metric view yourself? Point us at it and we&rsquo;ll verify it under your identity and
        attach it on the next optimization run.
      </p>
      <div className="flex items-center gap-2">
        <input
          type="text"
          readOnly
          placeholder="catalog.schema.metric_view"
          className="flex-1 rounded-md border border-default bg-surface px-2.5 py-1.5 font-mono text-sm text-primary"
        />
        <Button size="sm">Register</Button>
      </div>
    </div>
  )
}

// ── Frame 8a — registration entry points ────────────────────────────────────
export function ByoEntryPointsFrame() {
  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <p className="text-xs font-medium uppercase tracking-wide text-secondary">
          On the suggest-only card
        </p>
        <MvProposalCard
          proposal={proposalRevenue}
          ddl={ddlRevenue}
          liftLabel={<LiftNotMeasuredLabel />}
          actions={
            <>
              <Button size="sm" variant="secondary">Approve for re-run</Button>
              <Button size="sm">Re-run with this metric view</Button>
              {/* tertiary — the MV-D24 affordance */}
              <Button size="sm" variant="ghost">I created this myself</Button>
            </>
          }
        />
      </div>
      <div className="space-y-2">
        <p className="text-xs font-medium uppercase tracking-wide text-secondary">
          On the IQ Scan panel
        </p>
        <RegisterInput />
      </div>
    </div>
  )
}

// ── Frame 8b — verified / registered (no [Drop view], MV-D24 invariant 1) ────
export function ByoVerifiedFrame() {
  return (
    <div className="space-y-4 rounded-xl border border-default bg-surface p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div>
          <a className="inline-flex items-center gap-1 font-mono text-sm font-medium text-accent hover:underline" href="#">
            {byoVerified.identifier}
            <ArrowUpRight className="h-3.5 w-3.5" />
          </a>
          <p className="mt-0.5 text-xs text-muted">Verified under OBO as {byoVerified.verified_by}</p>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <Badge variant="info">USER_CREATED</Badge>
        </div>
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

      <div className="rounded-lg border border-success/30 bg-success/10 px-3 py-2 text-sm text-success-foreground">
        {REGISTERED_COPY}
      </div>

      {/* The registered copy promises attach "on the next optimization run".
          This surface is the IQ Scan panel, where a never-optimized user has no
          run to wait for — offer the affordance so the BYO path doesn't strand
          them (see header). Intentionally NO [Drop view]: dropping a
          USER_CREATED view stays with the user (MV-D24 invariant 1). */}
      <div className="flex flex-wrap items-center gap-2 pt-1">
        <Button size="sm">Start an optimization run</Button>
        <span className="text-xs text-muted">Or approve it and it attaches on your next run.</span>
      </div>
    </div>
  )
}

// ── Frame 8c — refused (reuses DenialBanner, MV-D24 invariant 2) ─────────────
export function ByoRefusedFrame() {
  return (
    <div className="space-y-4">
      <div className="space-y-2">
        <p className="text-xs font-medium uppercase tracking-wide text-secondary">
          Refused — not a metric view
        </p>
        <DenialBanner
          message={
            <>
              <span className="font-medium">That object isn&rsquo;t a metric view.</span>{" "}
              <span className="font-mono">finance.sales.orders_enriched</span> is a{" "}
              <span className="font-mono">VIEW</span>, not a metric view. Registration accepts only objects whose
              Type is <span className="font-mono">METRIC_VIEW</span>. Nothing was recorded.
            </>
          }
          actions={
            <Button size="sm" variant="outline">
              Enter a different identifier
            </Button>
          }
          footnote={null}
        />
      </div>
      <div className="space-y-2">
        <p className="text-xs font-medium uppercase tracking-wide text-secondary">
          Refused — not visible to you
        </p>
        <DenialBanner
          message={
            <>
              <span className="font-medium">We can&rsquo;t see that object as you.</span>{" "}
              <span className="font-mono">finance.sales.net_revenue</span> isn&rsquo;t visible under your identity —
              you may be missing <span className="font-mono">USE SCHEMA</span> /{" "}
              <span className="font-mono">SELECT</span>. Nothing was recorded.
            </>
          }
          actions={
            <Button size="sm" variant="outline">
              Enter a different identifier
            </Button>
          }
          footnote={null}
        />
      </div>
    </div>
  )
}
