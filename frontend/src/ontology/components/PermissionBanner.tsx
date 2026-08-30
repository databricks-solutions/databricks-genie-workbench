// Frame 17.0a — the tiered permission banner (capability → permission matrix).
// Driven by GET /api/ontology/preflight. Read tiers degrade gracefully; the
// optional write tier is never required to view. Fresh component (does not
// import the mockup scaffold), matching the 17.0a visual contract.
import { Building2, Check, Copy, Info, Lock, Minus, ShieldAlert, X } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import type { OntologyPreflight, PermissionTier, TierStatus } from "@/ontology/types"

const IDENTITY_LABEL: Record<PermissionTier["identity"], string> = {
  obo: "OBO",
  sp: "SP",
  batch: "Batch",
}

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

function copy(text: string) {
  void navigator.clipboard?.writeText(text)
}

export function PermissionBanner({ preflight }: { preflight: OntologyPreflight }) {
  const tiers = preflight.tiers
  const readyCount = tiers.filter((t) => t.status === "ok").length
  const readTiers = tiers.filter((t) => t.id !== "membership_write" && t.id !== "external_enrichment")

  return (
    <div className="space-y-4">
      <div className="flex items-start gap-2.5 rounded-xl border border-warning/40 bg-warning/5 px-4 py-3.5">
        <ShieldAlert className="mt-0.5 h-6 w-6 shrink-0 text-warning-foreground" />
        <div>
          <p className="text-base font-semibold text-primary">
            Ontology access — {readyCount} of {readTiers.length} read tiers ready
          </p>
          <p className="mt-1 max-w-prose text-xs text-secondary">
            Each tier below unlocks more of the ontology. Read tiers degrade gracefully; the optional
            write tier is never required — Ontology is read-only in this release and writes nothing to
            Unity Catalog.
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
          const needsGrant = t.status === "blocked" || t.status === "degraded"
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
                {needsGrant && t.grants.length > 0 && (
                  <div className="mt-1.5">
                    <Button size="sm" variant="secondary" onClick={() => copy(t.grants.join("\n"))}>
                      <Copy className="mr-1 h-3 w-3" />
                      {t.identity === "sp" ? "Copy GRANT SQL" : "Copy entitlement request"}
                    </Button>
                  </div>
                )}
              </div>
              <Badge variant={t.identity === "sp" ? "secondary" : "default"}>
                {IDENTITY_LABEL[t.identity]}
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
