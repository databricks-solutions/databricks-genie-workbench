// Frame 17.0a — the tiered permission banner (capability → permission matrix).
// Driven by GET /api/ontology/preflight. Read tiers degrade gracefully; the
// optional write tier is never required to view. Fresh component (does not
// import the mockup scaffold), matching the 17.0a visual contract.
import { useState } from "react"
import { Building2, Check, Copy, Info, Lock, Minus, ShieldAlert, X } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import type { OntologyPreflight, PermissionTier, SourceStatus, TierStatus } from "@/ontology/types"
import {
  copyButtonLabel,
  executeStatusToTier,
  grantCopyText,
  identityLabel,
  showGrantCopy,
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

export function PermissionBanner({ preflight }: { preflight: OntologyPreflight }) {
  const tiers = preflight.tiers
  // Count readiness over the SAME set we size the denominator by, so the header can
  // never read "4 of 3": the optional write + external-enrichment tiers are excluded
  // from both the numerator and the denominator (MV-D107 counter fix).
  const readTiers = tiers.filter((t) => t.id !== "membership_write" && t.id !== "external_enrichment")
  const readyCount = readTiers.filter((t) => t.status === "ok").length

  return (
    <div className="space-y-4">
      <div className="flex items-start gap-2.5 rounded-xl border border-warning/40 bg-warning/5 px-4 py-3.5">
        <ShieldAlert className="mt-0.5 h-6 w-6 shrink-0 text-warning-foreground" />
        <div>
          <p className="text-base font-semibold text-primary">
            Ontology access — {readyCount} of {readTiers.length} read tiers ready
          </p>
          <p className="mt-1 max-w-prose text-xs text-secondary">
            The taxonomy renders as the signed-in admin (OBO) — no service-principal grant is
            required to view. The SP grant lines below are an optional upgrade (a shared cross-user
            cache / consumer-safe serving). Read tiers degrade gracefully; the optional write tier is
            never required — Ontology is read-only in this release and writes nothing to Unity Catalog.
          </p>
        </div>
      </div>

      <div className="overflow-hidden rounded-xl border border-default">
        <div className="grid grid-cols-[auto_1fr_auto] items-center gap-x-3 border-b border-default bg-sunken px-4 py-2 text-xs font-semibold uppercase tracking-wide text-secondary">
          <span>Status</span>
          <span>Capability &amp; permission</span>
          <span>Identity</span>
        </div>
        {tiers.map((t) => {
          const locked = t.id === "membership_write"
          return (
            <div
              key={t.id}
              className="grid grid-cols-[auto_1fr_auto] items-center gap-x-3 border-b border-default bg-surface px-4 py-3 last:border-b-0"
            >
              <StatusPill status={t.status} />
              <div className="min-w-0">
                <p className="flex items-center gap-1.5 text-sm font-medium text-primary">
                  {t.label}
                  {locked && <Lock className="h-3 w-3 text-muted" />}
                </p>
                {t.reason && <p className="text-xs text-muted">{t.reason}</p>}
                {t.grants.length > 0 && (
                  <div className="mt-1 space-y-0.5">
                    {t.grants.map((g) => (
                      <p key={g} className="break-all font-mono text-xs text-secondary">
                        {g}
                      </p>
                    ))}
                  </div>
                )}
                {showGrantCopy(t) && (
                  <div className="mt-1.5">
                    <CopyGrantButton tier={t} />
                  </div>
                )}
                {/* Stage C: per-source panel — external-enrichment tier only, when the
                    preflight reports sources (i.e. external context is on). Off ⇒ no
                    sources ⇒ the tier keeps today's plain reason above. */}
                {t.id === "external_enrichment" && t.sources && t.sources.length > 0 && (
                  <SourcePanel sources={t.sources} />
                )}
              </div>
              <Badge variant={t.identity === "sp" ? "secondary" : "default"}>
                {identityLabel(t)}
              </Badge>
            </div>
          )
        })}
      </div>

      {preflight.company_name && (
        <div className="flex items-start gap-2 rounded-lg border border-info/30 bg-info/5 px-3 py-2.5">
          <Info className="mt-0.5 h-4 w-4 text-info-foreground" />
          <p className="text-xs text-secondary">
            Company name set to{" "}
            <span className="inline-flex items-center gap-1 font-medium text-primary">
              <Building2 className="h-3.5 w-3.5 text-accent" />
              {preflight.company_name}
            </span>{" "}
            (Settings → Ontology) — the estate is read in your business&rsquo;s terms.
          </p>
        </div>
      )}
    </div>
  )
}
