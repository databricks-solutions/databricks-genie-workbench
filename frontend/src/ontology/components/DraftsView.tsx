/**
 * The Drafts tab body: the ranked Domain + Page drafts (17.0d / 17.0e), ordered
 * HIGH → LOW by the backend. Owns the decision-action calls (POST /decision) and the
 * optimistic removal of a card once its decision is recorded. Prop-driven cards do the
 * rendering; this view holds the list state and the API wiring.
 */
import { useEffect, useRef, useState } from "react"
import { CheckCircle2, FolderTree, FileText, Wand2 } from "lucide-react"
import { getDrafts, pollBulkDraft, postDecision, startBulkDraft } from "@/ontology/api"
import type { DecisionAction, DomainDraft, OntologyDrafts, PageDraft } from "@/ontology/types"
import { Button } from "@/components/ui/button"
import { ApplyPreview } from "@/ontology/components/ApplyPreview"
import { DomainDraftCard, type BulkDraftState } from "@/ontology/components/DomainDraftCard"
import { PageDraftCard } from "@/ontology/components/PageDraftCard"

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))
const BULK_POLL_MS = 1500
const DEFAULT_BULK: BulkDraftState = { running: false, done: 0, total: 0, error: null, summary: null }

export function DraftsView({ drafts }: { drafts: OntologyDrafts }) {
  const [domains, setDomains] = useState<DomainDraft[]>(drafts.domains)
  const [pages, setPages] = useState<PageDraft[]>(drafts.pages)
  const [busyId, setBusyId] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  // Bulk "Draft this sub-domain with AI" progress, keyed by sub-domain id
  // (== DomainDraft.proposal_id == the Pages' domain_id). Step 4, MV-D66.
  const [bulk, setBulk] = useState<Record<string, BulkDraftState>>({})
  // Phase 5 (17i): the estate-level "Apply approved changes" panel — the ONLY
  // governed-tag write surface. Preview is a dry-run; nothing writes until confirmed.
  const [showApply, setShowApply] = useState(false)
  const mounted = useRef(true)
  useEffect(() => {
    mounted.current = true
    return () => {
      mounted.current = false
    }
  }, [])

  // Re-sync when a fresh payload arrives (e.g. after a Refresh).
  useEffect(() => {
    setDomains(drafts.domains)
    setPages(drafts.pages)
  }, [drafts])

  const patchBulk = (id: string, patch: Partial<BulkDraftState>) =>
    setBulk((prev) => ({
      ...prev,
      [id]: { ...DEFAULT_BULK, ...prev[id], ...patch },
    }))

  // Start → poll to completion → re-fetch drafts so the freshly-drafted bodies
  // (body_source="llm_bulk") land in the Page list. The status payload carries no
  // body, so the refetch is how "results land" on the cards (MV-D43: never hangs).
  const bulkDraft = async (subdomainId: string) => {
    if (bulk[subdomainId]?.running) return
    if (
      !window.confirm(
        "Draft every page in this sub-domain with AI? This makes one AI call per page.",
      )
    )
      return
    patchBulk(subdomainId, { running: true, done: 0, total: 0, error: null, summary: null })
    try {
      const { task_id, total } = await startBulkDraft(subdomainId)
      patchBulk(subdomainId, { total })
      for (;;) {
        await sleep(BULK_POLL_MS)
        if (!mounted.current) return
        const st = await pollBulkDraft(subdomainId, task_id)
        patchBulk(subdomainId, { done: st.done, total: st.total })
        if (!st.running) {
          const ok = st.results.filter((r) => r.ok).length
          const skipped = st.results.length - ok
          patchBulk(subdomainId, {
            running: false,
            summary: `Drafted ${ok} page${ok === 1 ? "" : "s"}${skipped ? `, ${skipped} skipped` : ""}.`,
          })
          break
        }
      }
      const fresh = await getDrafts()
      if (!mounted.current) return
      setDomains(fresh.domains)
      setPages(fresh.pages)
    } catch (e) {
      if (!mounted.current) return
      patchBulk(subdomainId, {
        running: false,
        error: e instanceof Error ? e.message : "Couldn't draft this sub-domain — please try again.",
      })
    }
  }

  const decide = async (
    kind: DomainDraft["kind"] | "page",
    proposalId: string,
    action: DecisionAction,
    remove: () => void,
  ) => {
    setBusyId(proposalId)
    setError(null)
    try {
      await postDecision({ kind, proposal_id: proposalId, action })
      remove() // optimistic: a decided proposal never resurfaces (MV-D26)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Couldn't record that decision — please try again.")
    } finally {
      setBusyId(null)
    }
  }

  const total = domains.length + pages.length
  if (total === 0) {
    return (
      <div className="flex items-start gap-2.5 rounded-xl border border-info/30 bg-info/5 px-4 py-3.5">
        <CheckCircle2 className="mt-0.5 h-5 w-5 shrink-0 text-info-foreground" />
        <div>
          <p className="text-sm font-semibold text-primary">
            {drafts.source === "cold" ? "No drafts yet" : "You're all caught up"}
          </p>
          <p className="mt-1 max-w-prose text-xs text-secondary">
            {drafts.source === "cold"
              ? "Run a refresh to look across the estate for domain and page suggestions."
              : "Every suggestion has been reviewed. New ones will appear here after the next refresh."}
          </p>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-5">
      {error && (
        <div className="rounded-xl border border-danger/30 bg-danger/5 px-4 py-3 text-sm text-danger-foreground">
          {error}
        </div>
      )}

      {/* Estate-level apply: preview + confirm the changes from everything approved so far. */}
      {showApply ? (
        <ApplyPreview onClose={() => setShowApply(false)} />
      ) : (
        <div className="flex justify-end">
          <Button variant="secondary" size="sm" onClick={() => setShowApply(true)}>
            <Wand2 className="h-4 w-4" /> Apply approved changes
          </Button>
        </div>
      )}

      {domains.length > 0 && (
        <section className="space-y-3">
          <div className="flex items-center gap-2">
            <FolderTree className="h-4 w-4 text-accent" />
            <h3 className="text-sm font-semibold text-primary">Domain suggestions</h3>
            <span className="text-xs text-muted">— strongest first</span>
          </div>
          {domains.map((d) => (
            <DomainDraftCard
              key={d.proposal_id}
              draft={d}
              busy={busyId === d.proposal_id}
              onDecide={(action) =>
                decide(d.kind, d.proposal_id, action, () =>
                  setDomains((prev) => prev.filter((x) => x.proposal_id !== d.proposal_id)),
                )
              }
              onBulkDraft={d.kind === "subdomain" ? () => bulkDraft(d.proposal_id) : undefined}
              bulk={bulk[d.proposal_id]}
            />
          ))}
        </section>
      )}

      {pages.length > 0 && (
        <section className="space-y-3">
          <div className="flex items-center gap-2">
            <FileText className="h-4 w-4 text-accent" />
            <h3 className="text-sm font-semibold text-primary">Page suggestions</h3>
            <span className="text-xs text-muted">— strongest first</span>
          </div>
          {pages.map((p) => (
            <PageDraftCard
              key={p.proposal_id}
              draft={p}
              busy={busyId === p.proposal_id}
              onDecide={(action) =>
                decide("page", p.proposal_id, action, () =>
                  setPages((prev) => prev.filter((x) => x.proposal_id !== p.proposal_id)),
                )
              }
            />
          ))}
        </section>
      )}
    </div>
  )
}
