/**
 * Metric view output/results section for the run-detail screen (Prompt 13).
 *
 * This is the CONTAINER (MV-D23): because it lives on a run screen it may fetch
 * by run_id — proposals, the rendered DDL artifact, and the created-object ledger
 * are all run-keyed — and it reads the Agent's current metric_views[] by space_id
 * for the suggest-only config diff. Everything below is presentational and takes
 * its data as props, keying nothing on run_id, so Prompt 13.5 can feed the same
 * panels from a space-scoped source unchanged.
 *
 * Two run states, mutually exclusive per MV-D1: a run that created and attached
 * shows the create-and-attach panels; a suggest-only run shows the proposals.
 */
import { useCallback, useEffect, useState } from "react"
import { AlertTriangle, Table2 } from "lucide-react"
import { RunActivitySection } from "@/components/auto-optimize/RunActivitySection"
import { MvSuggestOnlyPanel } from "@/components/auto-optimize/MvSuggestOnlyPanel"
import { MvCreateAttachPanel } from "@/components/auto-optimize/MvCreateAttachPanel"
import {
  catalogExplorerUrl,
  metricViewIdentifiers,
  workspaceOriginFromLinks,
} from "@/components/auto-optimize/mvFormat"
import {
  fetchSpace,
  getMvCreatedObjects,
  getMvDdl,
  getRunMvProposals,
} from "@/lib/api"
import type {
  GSOResourceLink,
  MvCreatedObject,
  MvDdlArtifact,
  MvProposal,
} from "@/types"

interface MvRunOutputSectionProps {
  runId: string
  spaceId: string
  links: GSOResourceLink[]
  /** Opens the run config pre-filled in create_and_attach mode (MV-D1 flow). */
  onRerunWithMv: (proposal: MvProposal) => void
}

export function MvRunOutputSection({
  runId,
  spaceId,
  links,
  onRerunWithMv,
}: MvRunOutputSectionProps) {
  const [proposals, setProposals] = useState<MvProposal[]>([])
  // MV-D23: one rendered DDL per view bundle, keyed by suggestion_id. The run
  // writes one artifact per bundle, so a single shared artifact could only feed
  // one card — every other proposal/created object rendered blank. Fetch each
  // card's DDL by its own suggestion_id (getMvDdl(runId, id)) and index them here.
  const [ddlBySuggestion, setDdlBySuggestion] = useState<Record<string, MvDdlArtifact>>({})
  const [created, setCreated] = useState<MvCreatedObject[]>([])
  const [downgradeReason, setDowngradeReason] = useState<string | null>(null)
  const [currentIdentifiers, setCurrentIdentifiers] = useState<string[]>([])
  const [loaded, setLoaded] = useState(false)

  const refreshCreated = useCallback(async () => {
    try {
      const res = await getMvCreatedObjects(runId)
      setCreated(res.created)
      setDowngradeReason(res.downgrade_reason)
    } catch {
      // Read-only surface: a warehouse hiccup should not break the run screen.
    }
  }, [runId])

  useEffect(() => {
    let cancelled = false
    async function load() {
      const [proposalsRes, createdRes, spaceRes] = await Promise.allSettled([
        getRunMvProposals(runId),
        getMvCreatedObjects(runId),
        fetchSpace(spaceId),
      ])
      if (cancelled) return
      const proposalsList =
        proposalsRes.status === "fulfilled" ? proposalsRes.value.proposals : []
      const createdList =
        createdRes.status === "fulfilled" ? createdRes.value.created : []
      if (proposalsRes.status === "fulfilled") setProposals(proposalsList)
      if (createdRes.status === "fulfilled") {
        setCreated(createdList)
        setDowngradeReason(createdRes.value.downgrade_reason)
      }
      if (spaceRes.status === "fulfilled") {
        setCurrentIdentifiers(metricViewIdentifiers(spaceRes.value.space_data))
      }

      // Fetch each card's DDL by its own suggestion_id so a multi-bundle run
      // shows DDL on every card, not just whichever artifact was written last.
      // mv-ddl 404s when no body was rendered — that card simply gets no DDL.
      const ids = Array.from(
        new Set(
          [...proposalsList, ...createdList]
            .map((x) => x.suggestion_id)
            .filter((id): id is string => Boolean(id)),
        ),
      )
      const ddlResults = await Promise.allSettled(ids.map((id) => getMvDdl(runId, id)))
      if (cancelled) return
      const nextDdl: Record<string, MvDdlArtifact> = {}
      ddlResults.forEach((res, i) => {
        if (res.status === "fulfilled" && res.value) nextDdl[ids[i]] = res.value
      })
      setDdlBySuggestion(nextDdl)
      setLoaded(true)
    }
    void load()
    return () => {
      cancelled = true
    }
  }, [runId, spaceId])

  const hasCreated = created.length > 0
  const hasProposals = proposals.length > 0

  // Nothing to show for a run with no MV activity.
  if (loaded && !hasCreated && !hasProposals && !downgradeReason) return null

  const origin = workspaceOriginFromLinks(links)

  return (
    <RunActivitySection
      title="Metric Views"
      description="Metric views the advisor proposed for this run, and any it created and attached under your identity."
      icon={Table2}
    >
      {downgradeReason && (
        <div className="flex items-start gap-1.5 rounded-lg border border-amber-500/30 bg-amber-500/10 px-3 py-2 text-xs text-amber-700 dark:text-amber-300">
          <AlertTriangle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <span>
            This run was downgraded to suggest-only before any metric view was created:{" "}
            {downgradeReason}
          </span>
        </div>
      )}

      {hasCreated ? (
        <div className="space-y-4">
          {created.map((obj) => {
            const objDdl = obj.suggestion_id
              ? ddlBySuggestion[obj.suggestion_id] ?? null
              : null
            return (
              <MvCreateAttachPanel
                key={obj.suggestion_id}
                obj={obj}
                ddl={objDdl}
                catalogUrl={catalogExplorerUrl(origin, obj.full_name)}
                onDropped={refreshCreated}
              />
            )
          })}
        </div>
      ) : (
        hasProposals && (
          <MvSuggestOnlyPanel
            runId={runId}
            proposals={proposals}
            ddlBySuggestion={ddlBySuggestion}
            currentIdentifiers={currentIdentifiers}
            onRerun={onRerunWithMv}
          />
        )
      )}
    </RunActivitySection>
  )
}
