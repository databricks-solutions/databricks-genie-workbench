// Ontology Settings — company name + catalog allowlist (MV-D42). The allowlist
// scopes every reader; an empty allowlist scans nothing (the page prompts to
// choose catalogs). Backed by GET/PUT /api/ontology/settings — the only write,
// and it writes our own config, never Unity Catalog.
import { useState } from "react"
import { Building2, Check, Compass, Database, Filter, Globe, Loader2, SlidersHorizontal, UserCog } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { saveSettings } from "@/ontology/api"
import type {
  IndustryAlignment,
  OntologySettings,
  ReadIdentity,
  SourceStatus,
} from "@/ontology/types"

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
  sources = [],
}: {
  settings: OntologySettings
  onSaved: (next: OntologySettings) => void
  // Phase 4 Stage C: the context sources the preflight tier reports (empty when the
  // feature is off). The per-source checkboxes iterate exactly this list.
  sources?: SourceStatus[]
}) {
  const [company, setCompany] = useState(settings.company_name ?? "")
  const [allowlistText, setAllowlistText] = useState(settings.catalog_allowlist.join(", "))
  const [readIdentity, setReadIdentity] = useState<ReadIdentity>(settings.read_identity ?? "obo")
  // Stage 3 curation policy (MV-D57) — moderate defaults when a stored row predates them.
  const [facetDenylistText, setFacetDenylistText] = useState((settings.domain_facet_denylist ?? []).join(", "))
  const [minTables, setMinTables] = useState(String(settings.domain_min_tables ?? 3))
  const [minSchemas, setMinSchemas] = useState(String(settings.domain_min_schemas ?? 2))
  const [requireConnection, setRequireConnection] = useState(settings.domain_require_connection ?? true)
  // Stage 3.2 edge-hygiene + diffuseness net (MV-D61/62) — shipped conservative defaults
  // when a stored row predates them.
  const [schemaDenylistText, setSchemaDenylistText] = useState(
    (settings.domain_schema_denylist ?? ["information_schema"]).join(", "),
  )
  const [joinSuffixesText, setJoinSuffixesText] = useState(
    (settings.domain_join_col_suffixes ?? ["_id", "_key"]).join(", "),
  )
  const [joinMaxSchemas, setJoinMaxSchemas] = useState(String(settings.domain_join_col_max_schemas ?? 2))
  const [joinDenylistText, setJoinDenylistText] = useState(
    (settings.domain_join_col_denylist ?? ["id", "user_id", "workspace_id", "category_id", "tenant_id", "account_id"]).join(", "),
  )
  const [maxDiffuseSchemas, setMaxDiffuseSchemas] = useState(String(settings.domain_max_diffuse_schemas ?? 6))
  const [minHomeConcentration, setMinHomeConcentration] = useState(String(settings.domain_min_home_concentration ?? 0.5))
  // Industry-reference alignment (MV-D58, §9) — the opt-in toggle + reference-model id.
  // DEFAULT OFF (MV-D44): off ⇒ a run is byte-identical estate-only (no typed correspondences,
  // no gap hypotheses). The reference_model is the Vibe industry model id (e.g. "airline").
  const [alignmentEnabled, setAlignmentEnabled] = useState(settings.industry_alignment?.enabled ?? false)
  const [alignmentModel, setAlignmentModel] = useState(settings.industry_alignment?.reference_model ?? "")
  // Phase 4 Stage C (MV-D44 DEFAULT OFF): the one opt-in toggle + per-source overrides.
  const [externalEnabled, setExternalEnabled] = useState(settings.external_context?.enabled ?? false)
  const [externalSources, setExternalSources] = useState<Record<string, boolean>>(
    settings.external_context?.sources ?? {},
  )
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
    const splitList = (text: string) =>
      text
        .split(/[,\n]/)
        .map((c) => c.trim())
        .filter(Boolean)
    const facetDenylist = splitList(facetDenylistText)
    try {
      const next = await saveSettings({
        company_name: company.trim() || null,
        catalog_allowlist: catalogs,
        read_identity: readIdentity,
        domain_facet_denylist: facetDenylist,
        domain_min_tables: Number.parseInt(minTables, 10) || 0,
        domain_min_schemas: Number.parseInt(minSchemas, 10) || 0,
        domain_require_connection: requireConnection,
        // Stage 3.2 edge-hygiene + diffuseness net (MV-D61/62).
        domain_schema_denylist: splitList(schemaDenylistText),
        domain_join_col_suffixes: splitList(joinSuffixesText),
        domain_join_col_max_schemas: Number.parseInt(joinMaxSchemas, 10) || 0,
        domain_join_col_denylist: splitList(joinDenylistText),
        domain_max_diffuse_schemas: Number.parseInt(maxDiffuseSchemas, 10) || 0,
        domain_min_home_concentration: Number.parseFloat(minHomeConcentration) || 0,
        // §9 industry-reference alignment (MV-D58) — DEFAULT OFF; a blank model id disables it.
        industry_alignment: {
          enabled: alignmentEnabled,
          reference_model: alignmentModel.trim() || null,
        } satisfies IndustryAlignment,
        // Phase 4 Stage C: the opt-in toggle + per-source overrides (DEFAULT OFF).
        external_context: { enabled: externalEnabled, sources: externalSources },
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

      {/* ── Stage 3.2: edge hygiene + diffuseness net (MV-D61/62) ── */}
      <div className="space-y-4 border-t border-default pt-4">
        <div>
          <p className="flex items-center gap-1.5 text-sm font-semibold text-primary">
            <Database className="h-4 w-4 text-accent" />
            Non-business schema denylist
          </p>
          <p className="mt-0.5 max-w-prose text-xs text-muted">
            Schemas that are infrastructure / pipeline / demo, not business areas
            (migration, cost attribution, dev/e2e). Their tables are dropped before
            grouping, so a real key that only <em>looked</em> cross-schema collapses back
            to its true home. Exact <code>catalog.schema</code>, a bare schema, or a glob
            (<code>e2e_*</code>, <code>*_dev</code>). Comma- or newline-separated.
          </p>
          <textarea
            className="mt-2 flex min-h-16 w-full rounded-lg border border-default bg-surface px-4 py-2 text-sm text-primary placeholder:text-muted focus-visible:border-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/20"
            value={schemaDenylistText}
            placeholder="information_schema, migration, e2e_*, *_dev"
            onChange={(e) => setSchemaDenylistText(e.target.value)}
          />
        </div>

        <div>
          <p className="flex items-center gap-1.5 text-sm font-semibold text-primary">
            <Filter className="h-4 w-4 text-accent" />
            Join-key proxy
          </p>
          <p className="mt-0.5 max-w-prose text-xs text-muted">
            When no foreign key is declared, tables sharing a join-shaped column are
            treated as related. Suffixes decide which columns qualify (<code>_code</code>
            is an enum, not a key — omit it); a proxy column reaching more than the
            schema cap, or named in the denylist (generic surrogate/audit keys), is
            skipped so it can&apos;t fuse unrelated areas. Declared foreign keys are never
            capped.
          </p>
          <div className="mt-2 flex flex-wrap items-end gap-4">
            <label className="text-xs text-secondary">
              Column suffixes
              <Input
                className="mt-1 w-48"
                value={joinSuffixesText}
                placeholder="_id, _key"
                onChange={(e) => setJoinSuffixesText(e.target.value)}
              />
            </label>
            <label className="text-xs text-secondary">
              Max schemas per column
              <Input
                type="number"
                min={1}
                className="mt-1 w-24"
                value={joinMaxSchemas}
                onChange={(e) => setJoinMaxSchemas(e.target.value)}
              />
            </label>
          </div>
          <label className="mt-3 block text-xs text-secondary">
            Generic column-name denylist
            <textarea
              className="mt-1 flex min-h-16 w-full rounded-lg border border-default bg-surface px-4 py-2 text-sm text-primary placeholder:text-muted focus-visible:border-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-accent/20"
              value={joinDenylistText}
              placeholder="id, user_id, workspace_id"
              onChange={(e) => setJoinDenylistText(e.target.value)}
            />
          </label>
        </div>

        <div>
          <p className="flex items-center gap-1.5 text-sm font-semibold text-primary">
            <SlidersHorizontal className="h-4 w-4 text-accent" />
            Diffuseness net
          </p>
          <p className="mt-0.5 max-w-prose text-xs text-muted">
            A last-resort guard so a sprawling cross-schema blob never surfaces as a
            domain. A domain that spans at least this many schemas <em>and</em> whose
            largest schema holds less than this fraction of its members is kept but not
            surfaced, with a &ldquo;split or attach&rdquo; hint. Curated domains are never
            gated.
          </p>
          <div className="mt-2 flex flex-wrap items-end gap-4">
            <label className="text-xs text-secondary">
              Max schemas before diffuse
              <Input
                type="number"
                min={1}
                className="mt-1 w-24"
                value={maxDiffuseSchemas}
                onChange={(e) => setMaxDiffuseSchemas(e.target.value)}
              />
            </label>
            <label className="text-xs text-secondary">
              Min home concentration
              <Input
                type="number"
                min={0}
                max={1}
                step={0.05}
                className="mt-1 w-24"
                value={minHomeConcentration}
                onChange={(e) => setMinHomeConcentration(e.target.value)}
              />
            </label>
          </div>
        </div>
      </div>

      {/* ── Phase 4 Stage C: one opt-in toggle for industry context (MV-D23/D44) ──
          Plain language only — no pack / provider / tier jargon. DEFAULT OFF ⇒ only the
          toggle shows; the per-source checkboxes appear once it is on and sources exist. */}
      <div className="space-y-3 border-t border-default pt-4">
        <div>
          <label className="flex items-center gap-2 text-sm font-semibold text-primary">
            <Globe className="h-4 w-4 text-accent" />
            <input
              type="checkbox"
              className="h-4 w-4 rounded border-default"
              checked={externalEnabled}
              onChange={(e) => setExternalEnabled(e.target.checked)}
            />
            Use industry context to improve naming
          </label>
          <p className="mt-0.5 max-w-prose text-xs text-muted">
            When on, suggestions can borrow clearer, industry-standard names and synonyms.
            It never changes which assets belong together — only how a group is named — and
            every borrowed name is shown with its source. Off by default.
          </p>
        </div>

        {externalEnabled && sources.length > 0 && (
          <div className="space-y-1.5">
            <p className="text-xs font-medium text-secondary">Sources to draw from</p>
            {sources.map((s) => (
              <label key={s.id} className="flex items-center gap-2 text-xs text-secondary">
                <input
                  type="checkbox"
                  className="h-4 w-4 rounded border-default"
                  checked={externalSources[s.id] ?? true}
                  onChange={(e) =>
                    setExternalSources((prev) => ({ ...prev, [s.id]: e.target.checked }))
                  }
                />
                {s.label}
              </label>
            ))}
          </div>
        )}
      </div>

      {/* ── §9 industry-reference alignment (MV-D58) — the one opt-in toggle + a model id ──
          Off by default (MV-D44): off ⇒ a run is byte-identical (no industry naming, no gaps).
          When on, discovered domains pick up business-language names + typed correspondences,
          and industry domains the estate lacks surface as ranked-below hints. */}
      <div className="space-y-3 border-t border-default pt-4">
        <div>
          <label className="flex items-center gap-2 text-sm font-semibold text-primary">
            <Compass className="h-4 w-4 text-accent" />
            <input
              type="checkbox"
              className="h-4 w-4 rounded border-default"
              checked={alignmentEnabled}
              onChange={(e) => setAlignmentEnabled(e.target.checked)}
            />
            Align to an industry reference model
          </label>
          <p className="mt-0.5 max-w-prose text-xs text-muted">
            When on, domains are matched against a standard industry model to borrow clearer
            names and to flag industry areas your estate has none for — as suggestions ranked
            below everything found in your data. It never changes which assets belong together,
            and a curated name always wins. Off by default.
          </p>
        </div>

        {alignmentEnabled && (
          <label className="block text-xs text-secondary">
            Reference model
            <Input
              className="mt-1 w-64"
              value={alignmentModel}
              placeholder="e.g. airline"
              onChange={(e) => setAlignmentModel(e.target.value)}
            />
            <span className="mt-1 block text-muted">
              The industry model to align against (e.g. <code>airline</code>, <code>retail</code>).
              Leave blank to keep alignment off.
            </span>
          </label>
        )}
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
