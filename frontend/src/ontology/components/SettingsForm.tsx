// Ontology Settings — company name + catalog allowlist (MV-D42). The allowlist
// scopes every reader; an empty allowlist scans nothing (the page prompts to
// choose catalogs). Backed by GET/PUT /api/ontology/settings — the only write,
// and it writes our own config, never Unity Catalog.
import { useState } from "react"
import { Building2, Check, Database, Loader2 } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { saveSettings } from "@/ontology/api"
import type { OntologySettings } from "@/ontology/types"

export function SettingsForm({
  settings,
  onSaved,
}: {
  settings: OntologySettings
  onSaved: (next: OntologySettings) => void
}) {
  const [company, setCompany] = useState(settings.company_name ?? "")
  const [allowlistText, setAllowlistText] = useState(settings.catalog_allowlist.join(", "))
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
    try {
      const next = await saveSettings({
        company_name: company.trim() || null,
        catalog_allowlist: catalogs,
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
