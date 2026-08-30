/**
 * Ontology — the read-only estate-ontology page (Phase 1). Standalone,
 * admin-gated, top-level view (MV-D36). Renders the permission banner (17.0a),
 * the governed-tag taxonomy (17.0b), and the tags/dedupe lens (17.0c), all wired
 * to live data under /api/ontology/*. Read-only: the only write is saving
 * Settings (our own config). Fresh components — does not import the mockup scaffold.
 */
import { useCallback, useEffect, useState } from "react"
import { AlertTriangle, Building2, FolderTree, Loader2, Lock, Settings as SettingsIcon, Tags } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import {
  getInventory,
  getPreflight,
  getSettings,
  getTags,
  getTaxonomy,
} from "@/ontology/api"
import type {
  OntologyInventory,
  OntologyPreflight,
  OntologySettings,
  OntologyTaxonomy,
  TagLens,
} from "@/ontology/types"
import { PermissionBanner } from "@/ontology/components/PermissionBanner"
import { TaxonomyView } from "@/ontology/components/TaxonomyView"
import { TagsLensView } from "@/ontology/components/TagsLens"
import { SettingsForm } from "@/ontology/components/SettingsForm"
import { FreshnessControls } from "@/ontology/components/FreshnessControls"

type OntologyTab = "taxonomy" | "tags" | "settings"

function LoadingRow({ label }: { label: string }) {
  return (
    <div className="flex items-center gap-2 py-10 text-sm text-muted">
      <Loader2 className="h-4 w-4 animate-spin" />
      {label}
    </div>
  )
}

function GrantGateNotice() {
  return (
    <div className="flex items-start gap-2.5 rounded-xl border border-warning/40 bg-warning/5 px-4 py-3.5">
      <Lock className="mt-0.5 h-5 w-5 shrink-0 text-warning-foreground" />
      <div>
        <p className="text-sm font-semibold text-primary">Governed-tag access needed</p>
        <p className="mt-1 max-w-prose text-xs text-secondary">
          Grant the service principal <span className="font-mono">SELECT on system.tags.governed_tags</span>{" "}
          to render the taxonomy and tags lens. The copy-ready grant SQL is in the access banner above.
        </p>
      </div>
    </div>
  )
}

function EmptyScopeNotice() {
  return (
    <div className="flex items-start gap-2.5 rounded-xl border border-info/30 bg-info/5 px-4 py-3.5">
      <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-info-foreground" />
      <div>
        <p className="text-sm font-semibold text-primary">Choose catalogs to scope the ontology</p>
        <p className="mt-1 max-w-prose text-xs text-secondary">
          No catalogs are selected yet, so nothing is scanned. Add catalogs in Settings below and the
          inventory, taxonomy, and tags lens will populate.
        </p>
      </div>
    </div>
  )
}

export default function OntologyPage() {
  const [preflight, setPreflight] = useState<OntologyPreflight | null>(null)
  const [inventory, setInventory] = useState<OntologyInventory | null>(null)
  const [settings, setSettings] = useState<OntologySettings | null>(null)
  const [taxonomy, setTaxonomy] = useState<OntologyTaxonomy | null>(null)
  const [tags, setTags] = useState<TagLens | null>(null)

  const [loadingHead, setLoadingHead] = useState(true)
  const [loadingBody, setLoadingBody] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [tab, setTab] = useState<OntologyTab>("taxonomy")

  const canRender = preflight?.can_render_taxonomy ?? false
  const emptyScope = (preflight?.catalog_allowlist.length ?? 0) === 0

  // First render: cheap preflight + OBO inventory fast-path (+ settings).
  const loadHead = useCallback(async () => {
    setLoadingHead(true)
    setError(null)
    try {
      const [pf, inv, st] = await Promise.all([getPreflight(), getInventory(), getSettings()])
      setPreflight(pf)
      setInventory(inv)
      setSettings(st)
      if (pf.catalog_allowlist.length === 0) setTab("settings")
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load Ontology")
    } finally {
      setLoadingHead(false)
    }
  }, [])

  useEffect(() => {
    void loadHead()
  }, [loadHead])

  // Heavier SP reads once the tag_graph tier is unlocked and catalogs are chosen.
  useEffect(() => {
    if (!canRender || emptyScope) return
    let cancelled = false
    setLoadingBody(true)
    Promise.all([getTaxonomy(), getTags()])
      .then(([tx, tg]) => {
        if (cancelled) return
        setTaxonomy(tx)
        setTags(tg)
      })
      .catch((e) => {
        if (!cancelled) setError(e instanceof Error ? e.message : "Failed to load taxonomy")
      })
      .finally(() => {
        if (!cancelled) setLoadingBody(false)
      })
    return () => {
      cancelled = true
    }
  }, [canRender, emptyScope])

  const TABS: { id: OntologyTab; label: string; icon: React.ReactNode }[] = [
    { id: "taxonomy", label: "Taxonomy", icon: <FolderTree className="h-4 w-4" /> },
    { id: "tags", label: "Tags", icon: <Tags className="h-4 w-4" /> },
    { id: "settings", label: "Settings", icon: <SettingsIcon className="h-4 w-4" /> },
  ]

  return (
    <div className="rounded-xl border border-default bg-surface">
      {/* Standalone page chrome (not a SpaceDetail tab strip — MV-D36) */}
      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-default px-5 py-4">
        <div className="flex items-center gap-2.5">
          <FolderTree className="h-5 w-5 text-accent" />
          <div>
            <h2 className="text-base font-semibold text-primary">Ontology</h2>
            <p className="text-xs text-muted">
              Domains, Sub-Domains &amp; governed tags across the estate — read-only
            </p>
          </div>
        </div>
        <div className="flex items-center gap-2">
          {preflight?.company_name && (
            <span className="inline-flex items-center gap-1.5 rounded-full border border-default bg-elevated px-2.5 py-1 text-xs text-secondary">
              <Building2 className="h-3.5 w-3.5 text-accent" />
              {preflight.company_name}
            </span>
          )}
          <Badge variant="secondary">Admin</Badge>
        </div>
      </div>

      <div className="space-y-4 p-5">
        {error && (
          <div className="rounded-xl border border-danger/30 bg-danger/5 px-4 py-3 text-sm text-danger-foreground">
            {error}
          </div>
        )}

        {loadingHead ? (
          <LoadingRow label="Resolving access & reading the estate…" />
        ) : preflight ? (
          <>
            <PermissionBanner preflight={preflight} />

            {/* Sub-tab strip */}
            <div className="flex items-center gap-1 border-b border-default">
              {TABS.map((t) => (
                <button
                  key={t.id}
                  onClick={() => setTab(t.id)}
                  className={`flex items-center gap-1.5 px-3 py-2 text-sm font-medium transition-colors border-b-2 -mb-px ${
                    tab === t.id
                      ? "border-accent text-accent"
                      : "border-transparent text-muted hover:text-secondary"
                  }`}
                >
                  {t.icon}
                  {t.label}
                </button>
              ))}
            </div>

            {tab === "settings" && settings && (
              <SettingsForm
                settings={settings}
                onSaved={(next) => {
                  setSettings(next)
                  // Re-resolve scope + re-read once catalogs change.
                  void loadHead()
                }}
              />
            )}

            {/* Freshness chip + Refresh button (Phase 2). The page is admin-gated,
                so the refresh action is available; the chip is always informative. */}
            {(tab === "taxonomy" || tab === "tags") && canRender && !emptyScope && (
              <div className="flex justify-end">
                <FreshnessControls isAdmin={true} />
              </div>
            )}

            {tab === "taxonomy" && (
              emptyScope ? (
                <EmptyScopeNotice />
              ) : !canRender ? (
                <GrantGateNotice />
              ) : loadingBody || !taxonomy ? (
                <LoadingRow label="Reading governed tags & building the taxonomy…" />
              ) : (
                <TaxonomyView taxonomy={taxonomy} inventory={inventory} />
              )
            )}

            {tab === "tags" && (
              emptyScope ? (
                <EmptyScopeNotice />
              ) : !canRender ? (
                <GrantGateNotice />
              ) : loadingBody || !tags ? (
                <LoadingRow label="Reading governed tags & finding collisions…" />
              ) : (
                <TagsLensView lens={tags} />
              )
            )}
          </>
        ) : null}
      </div>
    </div>
  )
}
