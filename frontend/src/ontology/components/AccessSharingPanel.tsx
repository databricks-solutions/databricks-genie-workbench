// Access & sharing — the demoted permission matrix (MV-D107 Phase 1). The full tier/grant
// banner used to sit atop every tab; it now lives collapsed inside Settings and auto-expands
// ONLY when a required read tier is blocked/degraded. The landing shows a neutral one-line
// status instead (OverviewPanel). The matrix itself (PermissionBanner) is unchanged.
import { useState } from "react"
import { ChevronDown, ChevronRight, ShieldCheck } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { PermissionBanner } from "./PermissionBanner"
import { readTiersBlocked } from "@/ontology/overviewModel"
import type { OntologyPreflight } from "@/ontology/types"

export function AccessSharingPanel({ preflight }: { preflight: OntologyPreflight }) {
  const blocked = readTiersBlocked(preflight)
  // Default follows `blocked`; once the admin toggles it, their choice sticks.
  const [manual, setManual] = useState<boolean | null>(null)
  const open = manual ?? blocked

  return (
    <div className="rounded-xl border border-default bg-surface">
      <button
        type="button"
        onClick={() => setManual(!open)}
        aria-expanded={open}
        className="flex w-full items-center justify-between gap-2 rounded-xl px-4 py-3 text-left hover:bg-elevated"
      >
        <span className="flex items-center gap-2 text-sm font-semibold text-primary">
          <ShieldCheck className="h-4 w-4 text-accent" aria-hidden="true" />
          Access &amp; sharing
          {blocked && <Badge variant="secondary">Action needed</Badge>}
        </span>
        {open ? (
          <ChevronDown className="h-4 w-4 shrink-0 text-muted" aria-hidden="true" />
        ) : (
          <ChevronRight className="h-4 w-4 shrink-0 text-muted" aria-hidden="true" />
        )}
      </button>
      {open && (
        <div className="border-t border-default px-4 py-4">
          <PermissionBanner preflight={preflight} />
        </div>
      )}
    </div>
  )
}
