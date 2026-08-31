// Ontology Settings — company name + catalog allowlist (MV-D42). The allowlist
// scopes every reader; an empty allowlist scans nothing (the page prompts to
// choose catalogs). Backed by GET/PUT /api/ontology/settings — the only write,
// and it writes our own config, never Unity Catalog.
import { useState } from "react"
import { Building2, Check, Database, Filter, Loader2, SlidersHorizontal, UserCog } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { saveSettings } from "@/ontology/api"
import type { IndustryAlignment, OntologySettings, ReadIdentity } from "@/ontology/types"

// "Read as" options (MV-D50). Default is the viewing admin (OBO) — no SP grant
// needed to view. SP / Auto are opt-in upgrades (shared cross-user cache).
const READ_IDENTITY_OPTIONS: { value: ReadIdentity; label: string }[] = [
  { value: "obo", label: "My identity (admin)" },
  { value: "sp", label: "Service principal" },
  { value: "auto", label: "Auto" },
]

export function SettingsForm({
  settings,
  onSaved,
}: {
  settings: OntologySettings
  onSaved: (next: OntologySettings) => void
}) {
  const [company, setCompany] = useState(settings.company_name ?? "")
  const [allowlistText, setAllowlistText] = useState(settings.catalog_allowlist.join(", "))
  const [readIdentity, setReadIdentity] = useState<ReadIdentity>(settings.read_identity ?? "obo")
  // Stage 3 curation policy (MV-D57) — moderate defaults when a stored row predates them.
  const [facetDenylistText, setFacetDenylistText] = useState((settings.domain_facet_denylist ?? []).join(", "))
  const [minTables, setMinTables] = useState(String(settings.domain_min_tables ?? 3))
  const [minSchemas, setMinSchemas] = useState(String(settings.domain_min_schemas ?? 2))
  const [requireConnection, setRequireConnection] = useState(settings.domain_require_connection ?? true)
  // Industry alignment (MV-D58) is STORED + DORMANT (§9 is Phase 4) — preserved as-is.
  const industryAlignment: IndustryAlignment = settings.industry_alignment ?? {
    enabled: false,
    reference_model: null,
  }
  const [saving, setSaving] = useState(false)
  const [savedAt, setSavedAt] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)

  const save = async () => {
    setSaving(true)
    setError(null)
    const catalogs = allowlistText
      .split(/[,\n]/)
      .map((c) => c.trim())
      .filter(Boolean)
    const facetDenylist = facetDenylistText
      .split(/[,\n]/)
      .map((c) => c.trim())
      .filter(Boolean)
    try {
      const next = await saveSettings({
        company_name: company.trim() || null,
        catalog_allowlist: catalogs,
        read_identity: readIdentity,
        domain_facet_denylist: facetDenylist,
        domain_min_tables: Number.parseInt(minTables, 10) || 0,
        domain_min_schemas: Number.parseInt(minSchemas, 10) || 0,
        domain_require_connection: requireConnection,
        industry_alignment: industryAlignment,
      })
      onSaved(next)
      setSavedAt(Date.now())
    } catch (e) {
      setError(e instanceof Error ? e.message : "Could not save settings")
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="space-y-4 rounded-xl border border-default bg-surface p-5">
      <div>
        <p className="flex items-center gap-1.5 text-sm font-semibold text-primary">
          <Building2 className="h-4 w-4 text-accent" />
          Company name
        </p>
        <p className="mt-0.5 text-xs text-muted">
          Optional. Gives the estate read its business context — a run without it still works.
        </p>
        <Input
          className="mt-2"
          value={company}
          placeholder="e.g. Northwind Trading Co."
          onChange={(e) => setCompany(e.target.value)}
        />
      </div>

      <div>
        <p className="flex items-center gap-1.5 text-sm font-semibold text-primary">
          <Database className="h-4 w-4 text-accent" />
          Catalog allowlist
        </p>
        <p className="mt-0.5 max-w-prose text-xs text-muted">
          Comma- or newline-separated catalog names. This scopes every reader — inventory, taxonomy,
          and the tags lens. Leave empty and the ontology scans nothing until you choose catalogs
          (we never scan the whole account by default).
        </p>
        <textarea
          className="mt-2 flex min-h-20 w-full rounded-lg border border-default bg-surface px-4 py-2 text-sm text-primary placeholder:text-muted focus-visible:border-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/20"
          value={allowlistText}
          placeholder="finance, marketing, operations"
          onChange={(e) => setAllowlistText(e.target.value)}
        />
      </div>

      <div>
        <p className="flex items-center gap-1.5 text-sm font-semibold text-primary">
          <UserCog className="h-4 w-4 text-accent" />
          Read as
        </p>
        <p className="mt-0.5 max-w-prose text-xs text-muted">
          Which identity reads the governed-tag graph and usage signals. Defaults to your own admin
          identity (OBO) — no service-principal grant is required to view. Switch to the service
          principal (an optional upgrade) for a shared cross-user cache once its system-table grants
          are in place.
        </p>
        <div className="mt-2 inline-flex rounded-lg border border-default bg-sunken p-0.5">
          {READ_IDENTITY_OPTIONS.map((opt) => (
            <button
              key={opt.value}
              type="button"
              onClick={() => setReadIdentity(opt.value)}
              className={
                "rounded-md px-3 py-1.5 text-xs font-medium transition-colors " +
                (readIdentity === opt.value
                  ? "bg-surface text-primary shadow-sm"
                  : "text-secondary hover:text-primary")
              }
            >
              {opt.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── Stage 3: curation policy (MV-D57) — the legitimacy bar + facet denylist ── */}
      <div className="space-y-4 border-t border-default pt-4">
        <div>
          <p className="flex items-center gap-1.5 text-sm font-semibold text-primary">
            <SlidersHorizontal className="h-4 w-4 text-accent" />
            Domain legitimacy bar
          </p>
          <p className="mt-0.5 max-w-prose text-xs text-muted">
            How big and connected a group must be to stand on its own as a domain. Smaller,
            unconnected groups are still found — they&apos;re suggested as additions to an existing
            domain instead of standalone ones. Moderate defaults suit most estates.
          </p>
          <div className="mt-2 flex flex-wrap items-end gap-4">
            <label className="text-xs text-secondary">
              Minimum tables
              <Input
                type="number"
                min={1}
                className="mt-1 w-24"
                value={minTables}
                onChange={(e) => setMinTables(e.target.value)}
              />
            </label>
            <label className="text-xs text-secondary">
              Minimum schemas
              <Input
                type="number"
                min={1}
                className="mt-1 w-24"
                value={minSchemas}
                onChange={(e) => setMinSchemas(e.target.value)}
              />
            </label>
            <label className="flex items-center gap-2 pb-2 text-xs text-secondary">
              <input
                type="checkbox"
                className="h-4 w-4 rounded border-default"
                checked={requireConnection}
                onChange={(e) => setRequireConnection(e.target.checked)}
              />
              Require a structural connection
            </label>
          </div>
        </div>

        <div>
          <p className="flex items-center gap-1.5 text-sm font-semibold text-primary">
            <Filter className="h-4 w-4 text-accent" />
            Facet denylist
          </p>
          <p className="mt-0.5 max-w-prose text-xs text-muted">
            Tag names that describe an <em>attribute</em> of data (a tier, a sensitivity label, a
            demo flag) rather than a business area — these are kept out of domain suggestions.
            Comma- or newline-separated. Shipped defaults cover the common ones; add your own.
          </p>
          <textarea
            className="mt-2 flex min-h-16 w-full rounded-lg border border-default bg-surface px-4 py-2 text-sm text-primary placeholder:text-muted focus-visible:border-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/20"
            value={facetDenylistText}
            placeholder="data_tier, certification, contains_synthetic"
            onChange={(e) => setFacetDenylistText(e.target.value)}
          />
        </div>
      </div>

      {error && <p className="text-xs text-danger-foreground">{error}</p>}

      <div className="flex items-center gap-3">
        <Button size="sm" onClick={save} disabled={saving}>
          {saving ? <Loader2 className="mr-1 h-3.5 w-3.5 animate-spin" /> : null}
          Save settings
        </Button>
        {savedAt && !saving && (
          <span className="inline-flex items-center gap-1 text-xs text-success-foreground">
            <Check className="h-3.5 w-3.5" />
            Saved
          </span>
        )}
      </div>
    </div>
  )
}
