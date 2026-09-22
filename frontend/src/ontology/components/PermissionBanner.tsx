// Access & sharing (MV-D107 Phase 4) — the preflight tiers grouped BY PURPOSE, neutral by
// default (only a blocked/degraded read tier warns; no always-amber header, no "N of M ready").
// Reading your estate (OBO) · Optional upgrades (SP grant, GRANT SQL behind Show-SQL) · Not used
// this release (locked write tier) · External sources (Stage C SourcePanel, unchanged). Pure
// bucketing lives in ../accessModel; driven by GET /api/ontology/preflight (no new API).
import { useState } from "react"
import { Check, Copy, Lock, Minus, ShieldAlert, ShieldCheck, X } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import type { OntologyPreflight, PermissionTier, SourceStatus, TierStatus } from "@/ontology/types"
import { bucketTiers, readingBlocked } from "@/ontology/accessModel"
import {
  copyButtonLabel,
  executeStatusToTier,
  grantCopyText,
  identityLabel,
} from "./permissionTiers"

function StatusPill({ status }: { status: TierStatus }) {
  if (status === "ok") {
    return (
      <span className="inline-flex h-6 w-6 items-center justify-center rounded-full bg-success/15 text-success-foreground">
        <Check className="h-3.5 w-3.5" />
      </span>
    )
  }
  if (status === "not_exercised") {
    return (
      <span className="inline-flex h-6 w-6 items-center justify-center rounded-full bg-elevated text-muted">
        <Minus className="h-3.5 w-3.5" />
      </span>
    )
  }
  // degraded | blocked
  return (
    <span className="inline-flex h-6 w-6 items-center justify-center rounded-full bg-warning/15 text-warning-foreground">
      <X className="h-3.5 w-3.5" />
    </span>
  )
}

function CopyGrantButton({ tier }: { tier: PermissionTier }) {
  const [copied, setCopied] = useState(false)
  const onClick = async () => {
    try {
      await navigator.clipboard?.writeText(grantCopyText(tier))
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      // Clipboard unavailable (permissions / insecure context) — no-op.
    }
  }
  return (
    <Button size="sm" variant="secondary" onClick={onClick} aria-live="polite">
      {copied ? (
        <Check className="mr-1 h-3 w-3 text-success-foreground" />
      ) : (
        <Copy className="mr-1 h-3 w-3" />
      )}
      {copied ? "Copied!" : copyButtonLabel(tier)}
    </Button>
  )
}

// A copy-to-clipboard button for one source's GRANT EXECUTE line (Stage C). Mirrors
// CopyGrantButton but copies a single source's grant rather than a tier's grant list.
function CopySourceGrant({ line }: { line: string }) {
  const [copied, setCopied] = useState(false)
  const onClick = async () => {
    try {
      await navigator.clipboard?.writeText(line)
      setCopied(true)
      setTimeout(() => setCopied(false), 1500)
    } catch {
      // Clipboard unavailable (permissions / insecure context) — no-op.
    }
  }
  return (
    <Button size="sm" variant="secondary" onClick={onClick} aria-live="polite">
      {copied ? (
        <Check className="mr-1 h-3 w-3 text-success-foreground" />
      ) : (
        <Copy className="mr-1 h-3 w-3" />
      )}
      {copied ? "Copied!" : "Copy GRANT EXECUTE"}
    </Button>
  )
}

// Phase 4 Stage C: the per-source sub-panel for the external-enrichment tier. Each row
// shows the source label, its class + provenance tier badges, the plain-language
// influence, an EXECUTE status pill, and a copy-ready GRANT EXECUTE when one is missing.
// Rendered only when the tier reports sources; otherwise the tier keeps its plain reason.
function SourcePanel({ sources }: { sources: SourceStatus[] }) {
  return (
    <div className="mt-2 space-y-2 border-t border-default pt-2">
      {sources.map((s) => (
        <div key={s.id} className="flex items-start gap-2.5">
          <StatusPill status={executeStatusToTier(s.execute_status)} />
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-1.5">
              <span className="text-sm font-medium text-primary">{s.label}</span>
              <Badge variant={s.klass === "external" ? "secondary" : "default"}>{s.klass}</Badge>
              <Badge variant="secondary">{s.provenance_tier}</Badge>
            </div>
            <p className="text-xs text-muted">May inform: {s.influence}</p>
            {s.reason && <p className="text-xs text-muted">{s.reason}</p>}
            {s.grant_line && (
              <div className="mt-1 space-y-1">
                <p className="break-all font-mono text-xs text-secondary">{s.grant_line}</p>
                <CopySourceGrant line={s.grant_line} />
              </div>
            )}
          </div>
        </div>
      ))}
    </div>
  )
}

// GRANT SQL is an implementation detail, not the primary read (MV-D23) — it stays behind a
// Show-SQL disclosure so the benefit leads and the SQL is one click away when wanted.
function ShowSqlDisclosure({ tier }: { tier: PermissionTier }) {
  const [open, setOpen] = useState(false)
  return (
    <div className="mt-1.5">
      <button
        type="button"
        onClick={() => setOpen((o) => !o)}
        aria-expanded={open}
        className="text-xs font-medium text-accent underline underline-offset-2 hover:opacity-80"
      >
        {open ? "Hide SQL" : "Show SQL"}
      </button>
      {open && (
        <div className="mt-1.5 space-y-1">
          {tier.grants.map((g) => (
            <p key={g} className="break-all font-mono text-xs text-secondary">
              {g}
            </p>
          ))}
          <CopyGrantButton tier={tier} />
        </div>
      )}
    </div>
  )
}

// A purpose group card. Neutral by default; `warn` (a blocked/degraded read) turns it amber.
function PurposeSection({
  title,
  subtitle,
  warn = false,
  children,
}: {
  title: string
  subtitle?: string
  warn?: boolean
  children: React.ReactNode
}) {
  return (
    <section
      className={`rounded-xl border px-4 py-3.5 ${
        warn ? "border-warning/40 bg-warning/5" : "border-default bg-surface"
      }`}
    >
      <div className="flex items-center gap-2">
        {warn ? (
          <ShieldAlert className="h-4 w-4 shrink-0 text-warning-foreground" aria-hidden="true" />
        ) : (
          <ShieldCheck className="h-4 w-4 shrink-0 text-accent" aria-hidden="true" />
        )}
        <h4 className="text-sm font-semibold text-primary">{title}</h4>
      </div>
      {subtitle && <p className="mt-1 max-w-prose text-xs text-secondary">{subtitle}</p>}
      <div className="mt-2.5 space-y-2.5">{children}</div>
    </section>
  )
}

// A plain status row (Reading / Not-used groups): pill · label (+lock) · reason · identity.
function TierRow({ tier }: { tier: PermissionTier }) {
  const locked = tier.id === "membership_write"
  return (
    <div className="flex items-start gap-2.5">
      <StatusPill status={tier.status} />
      <div className="min-w-0 flex-1">
        <p className="flex items-center gap-1.5 text-sm font-medium text-primary">
          {tier.label}
          {locked && <Lock className="h-3 w-3 text-muted" aria-hidden="true" />}
        </p>
        {tier.reason && <p className="text-xs text-muted">{tier.reason}</p>}
      </div>
      <Badge variant={tier.identity === "sp" ? "secondary" : "default"}>{identityLabel(tier)}</Badge>
    </div>
  )
}

// An optional-upgrade row: benefit-led copy with the GRANT SQL behind a Show-SQL disclosure.
function UpgradeRow({ tier }: { tier: PermissionTier }) {
  return (
    <div className="flex items-start gap-2.5">
      <StatusPill status={tier.status} />
      <div className="min-w-0 flex-1">
        <p className="text-sm font-medium text-primary">{tier.label}</p>
        <p className="text-xs text-muted">
          Optional — grant the app service principal for a shared, cross-user cache and
          consumer-safe serving. Not required to view.
        </p>
        <ShowSqlDisclosure tier={tier} />
      </div>
    </div>
  )
}

export function PermissionBanner({ preflight }: { preflight: OntologyPreflight }) {
  const { reading, upgrades, notUsed, external } = bucketTiers(preflight.tiers)
  const blocked = readingBlocked(preflight.tiers)

  return (
    <div className="space-y-4">
      {/* Purpose-first, neutral intro — no "N of M ready", no always-amber header. */}
      <p className="max-w-prose text-xs text-secondary">
        Ontology reads your estate as the signed-in admin (OBO) — no service-principal grant is
        required to view. Everything below is either current status or an optional upgrade.
      </p>

      {reading.length > 0 && (
        <PurposeSection
          title="Reading your estate"
          warn={blocked}
          subtitle={
            blocked
              ? "A read is degraded — grant the service principal under Optional upgrades to restore the shared path (you can still view as admin)."
              : "You’re set — reading as the signed-in admin (OBO)."
          }
        >
          {reading.map((t) => (
            <TierRow key={t.id} tier={t} />
          ))}
        </PurposeSection>
      )}

      {upgrades.length > 0 && (
        <PurposeSection
          title="Optional upgrades"
          subtitle="Speed and sharing improvements via a service-principal grant. None are required to view the ontology."
        >
          {upgrades.map((t) => (
            <UpgradeRow key={t.id} tier={t} />
          ))}
        </PurposeSection>
      )}

      {notUsed.length > 0 && (
        <PurposeSection
          title="Not used this release"
          subtitle="Ontology is read-only and writes nothing to Unity Catalog — this capability stays locked."
        >
          {notUsed.map((t) => (
            <TierRow key={t.id} tier={t} />
          ))}
        </PurposeSection>
      )}

      {external.map((t) => (
        <PurposeSection key={t.id} title="External sources">
          {t.sources && t.sources.length > 0 ? <SourcePanel sources={t.sources} /> : <TierRow tier={t} />}
        </PurposeSection>
      ))}
    </div>
  )
}
