/**
 * Ontology — the read-only estate-ontology page (Phase 1). Standalone,
 * admin-gated, top-level view (MV-D36). Renders the permission banner (17.0a),
 * the governed-tag taxonomy (17.0b), and the tags/dedupe lens (17.0c), all wired
 * to live data under /api/ontology/*. Read-only: the only write is saving
 * Settings (our own config). Fresh components — does not import the mockup scaffold.
 */
import { useCallback, useEffect, useRef, useState } from "react"
import { Building2, Database, FolderTree, LayoutDashboard, Lightbulb, Loader2, Lock, Network, Settings as SettingsIcon } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import {
  getDrafts,
  getGraph,
  getInventory,
  getPreflight,
  getRefreshStatus,
  getSettings,
  getTags,
  getTaxonomy,
  triggerRefresh,
} from "@/ontology/api"
import type {
  OntologyDrafts,
  OntologyGraph,
  OntologyInventory,
  OntologyPreflight,
  OntologySettings,
  OntologyTaxonomy,
  TagLens,
} from "@/ontology/types"
import { AccessSharingPanel } from "@/ontology/components/AccessSharingPanel"
import { OverviewPanel } from "@/ontology/components/OverviewPanel"
import { OnboardingState } from "@/ontology/components/OnboardingState"
import type { NextAction } from "@/ontology/overviewModel"
import { ONTOLOGY_TABS, type OntologyTab } from "@/ontology/tabs"
import { pollSettleAction } from "@/ontology/refreshPolling"
import { EstateView } from "@/ontology/components/EstateView"
import { SettingsForm } from "@/ontology/components/SettingsForm"
import { FreshnessControls } from "@/ontology/components/FreshnessControls"
import { DraftsView } from "@/ontology/components/DraftsView"
import { EstateGraph } from "@/ontology/components/EstateGraph"

const TAB_ICON: Record<OntologyTab, React.ReactNode> = {
  overview: <LayoutDashboard className="h-4 w-4" aria-hidden="true" />,
  review: <Lightbulb className="h-4 w-4" aria-hidden="true" />,
  map: <Network className="h-4 w-4" aria-hidden="true" />,
  estate: <FolderTree className="h-4 w-4" aria-hidden="true" />,
  settings: <SettingsIcon className="h-4 w-4" aria-hidden="true" />,
}

function LoadingRow({ label }: { label: string }) {
  return (
    <div className="flex items-center gap-2 py-10 text-sm text-muted">
      <Loader2 className="h-4 w-4 animate-spin" aria-hidden="true" />
      {label}
    </div>
  )
}

// Governed-tag reads are blocked — send the admin to the grant matrix (which auto-expands when
// a read tier is blocked), with a ghost preview of the surface it will unlock.
function GrantGateNotice({ onOpenAccess, onBack }: { onOpenAccess: () => void; onBack: () => void }) {
  return (
    <OnboardingState
      icon={<Lock className="h-6 w-6" />}
      heading="Unlock the governed-tag reads"
      body={
        <>
          Grant the service principal{" "}
          <span className="font-mono">SELECT on system.tags.governed_tags</span> to render the
          taxonomy and tags. The copy-ready grant SQL is in Access &amp; sharing.
        </>
      }
      primary={{ label: "Open Access & sharing", onClick: onOpenAccess }}
      escape={{ label: "Back to overview", onClick: onBack }}
    />
  )
}

// No catalogs scoped yet — the whole surface is empty until the admin picks catalogs to scan.
function EmptyScopeNotice({
  onChooseCatalogs,
  onBack,
}: {
  onChooseCatalogs: () => void
  onBack: () => void
}) {
  return (
    <OnboardingState
      icon={<Database className="h-6 w-6" />}
      heading="Choose catalogs to see your ontology"
      body="No catalogs are scoped yet, so nothing is scanned. Pick the catalogs to read and the inventory, taxonomy, and suggestions all populate. Read-only — nothing is written."
      primary={{ label: "Choose catalogs", onClick: onChooseCatalogs }}
      escape={{ label: "Back to overview", onClick: onBack }}
    />
  )
}

export default function OntologyPage() {
  const [preflight, setPreflight] = useState<OntologyPreflight | null>(null)
  const [inventory, setInventory] = useState<OntologyInventory | null>(null)
  const [settings, setSettings] = useState<OntologySettings | null>(null)
  const [taxonomy, setTaxonomy] = useState<OntologyTaxonomy | null>(null)
  const [tags, setTags] = useState<TagLens | null>(null)
  const [drafts, setDrafts] = useState<OntologyDrafts | null>(null)
  const [graph, setGraph] = useState<OntologyGraph | null>(null)

  const [loadingHead, setLoadingHead] = useState(true)
  const [loadingBody, setLoadingBody] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [tab, setTab] = useState<OntologyTab>("overview")
  const [scanning, setScanning] = useState(false)
  // Bump to re-run the heavy taxonomy/tags/drafts/graph fetch (e.g. after a refresh completes).
  const [reloadKey, setReloadKey] = useState(0)
  const reload = useCallback(() => setReloadKey((k) => k + 1), [])
  // Poll handle for a scan launched from the Overview CTA; cleared on settle/unmount.
  const scanTimer = useRef<ReturnType<typeof setInterval> | null>(null)
  useEffect(
    () => () => {
      if (scanTimer.current) clearInterval(scanTimer.current)
    },
    [],
  )

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
    Promise.all([getTaxonomy(), getTags(), getDrafts(), getGraph()])
      .then(([tx, tg, dr, gr]) => {
        if (cancelled) return
        setTaxonomy(tx)
        setTags(tg)
        setDrafts(dr)
        setGraph(gr)
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
  }, [canRender, emptyScope, reloadKey])

  // Scan launched from the Overview CTA: reuse the same triggerRefresh + poll the
  // FreshnessControls button uses, then reload the body reads and land on Drafts.
  const runScan = useCallback(async () => {
    if (scanning) return
    setScanning(true)
    setError(null)
    try {
      await triggerRefresh()
    } catch (e) {
      setError(e instanceof Error ? e.message : "Couldn't start a scan — please try again.")
      setScanning(false)
      return
    }
    scanTimer.current = setInterval(async () => {
      try {
        const s = await getRefreshStatus()
        const { settled } = pollSettleAction(s.state, false)
        if (settled) {
          if (scanTimer.current) {
            clearInterval(scanTimer.current)
            scanTimer.current = null
          }
          setScanning(false)
          reload()
          setTab("review")
        }
      } catch {
        // Transient poll error — keep polling; the interval retries.
      }
    }, 2500)
  }, [scanning, reload])

  // Map the Overview's one adaptive CTA to a tab switch or the scan routine.
  const handlePrimary = useCallback(
    (action: NextAction) => {
      switch (action.target) {
        case "settings":
          setTab("settings")
          break
        case "scan":
          void runScan()
          break
        case "review":
          setTab("review")
          break
        case "map":
          setTab("map")
          break
      }
    },
    [runScan],
  )

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
            {/* Sub-tab strip — Overview · Review · Map · Estate · Settings (MV-D107 IA) */}
            <div className="flex items-center gap-1 border-b border-default">
              {ONTOLOGY_TABS.map((t) => (
                <button
                  key={t.id}
                  onClick={() => setTab(t.id)}
                  className={`flex items-center gap-1.5 px-3 py-2 text-sm font-medium transition-colors border-b-2 -mb-px ${
                    tab === t.id
                      ? "border-accent text-accent"
                      : "border-transparent text-muted hover:text-secondary"
                  }`}
                >
                  {TAB_ICON[t.id]}
                  {t.label}
                </button>
              ))}
            </div>

            {tab === "overview" && (
              <OverviewPanel
                preflight={preflight}
                inventory={inventory}
                taxonomy={taxonomy}
                drafts={drafts}
                onPrimary={handlePrimary}
                busy={scanning}
              />
            )}

            {tab === "settings" && settings && (
              <>
                <SettingsForm
                  settings={settings}
                  // Stage C: the context sources the preflight reports (empty when off) drive
                  // the per-source checkboxes.
                  sources={
                    preflight?.tiers.find((t) => t.id === "external_enrichment")?.sources ?? []
                  }
                  onSaved={(next) => {
                    setSettings(next)
                    // Re-resolve scope + re-read once catalogs change.
                    void loadHead()
                  }}
                />
                {/* Demoted permission matrix — collapsed unless a read tier is blocked. */}
                <AccessSharingPanel preflight={preflight} />
              </>
            )}

            {/* Freshness chip + Refresh button. The page is admin-gated, so the refresh
                action is available; the chip is always informative. */}
            {(tab === "review" || tab === "map" || tab === "estate") && canRender && !emptyScope && (
              <div className="flex justify-end">
                <FreshnessControls
                  isAdmin={true}
                  onOpenSettings={() => setTab("settings")}
                  onRefreshComplete={reload}
                />
              </div>
            )}

            {tab === "review" && (
              emptyScope ? (
                <EmptyScopeNotice
                  onChooseCatalogs={() => setTab("settings")}
                  onBack={() => setTab("overview")}
                />
              ) : !canRender ? (
                <GrantGateNotice
                  onOpenAccess={() => setTab("settings")}
                  onBack={() => setTab("overview")}
                />
              ) : loadingBody || !drafts ? (
                <LoadingRow label="Ranking domain & page suggestions…" />
              ) : (
                <DraftsView
                  drafts={drafts}
                  onScan={runScan}
                  scanning={scanning}
                  onBrowseEstate={() => setTab("estate")}
                />
              )
            )}

            {tab === "map" && (
              emptyScope ? (
                <EmptyScopeNotice
                  onChooseCatalogs={() => setTab("settings")}
                  onBack={() => setTab("overview")}
                />
              ) : !canRender ? (
                <GrantGateNotice
                  onOpenAccess={() => setTab("settings")}
                  onBack={() => setTab("overview")}
                />
              ) : loadingBody || !graph ? (
                <LoadingRow label="Building the estate graph…" />
              ) : (
                <EstateGraph graph={graph} />
              )
            )}

            {tab === "estate" && (
              emptyScope ? (
                <EmptyScopeNotice
                  onChooseCatalogs={() => setTab("settings")}
                  onBack={() => setTab("overview")}
                />
              ) : !canRender ? (
                <GrantGateNotice
                  onOpenAccess={() => setTab("settings")}
                  onBack={() => setTab("overview")}
                />
              ) : loadingBody || !taxonomy ? (
                <LoadingRow label="Reading governed tags & building the taxonomy…" />
              ) : (
                <EstateView
                  taxonomy={taxonomy}
                  tags={tags}
                  inventory={inventory}
                  drafts={drafts?.domains ?? []}
                  onReview={() => setTab("review")}
                />
              )
            )}
          </>
        ) : null}
      </div>
    </div>
  )
}
