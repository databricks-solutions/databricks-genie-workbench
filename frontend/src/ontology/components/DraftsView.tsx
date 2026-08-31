/**
 * The Drafts tab body: the ranked Domain + Page drafts (17.0d / 17.0e), ordered
 * HIGH → LOW by the backend. Owns the decision-action calls (POST /decision) and the
 * optimistic removal of a card once its decision is recorded. Prop-driven cards do the
 * rendering; this view holds the list state and the API wiring.
 */
import { useEffect, useState } from "react"
import { CheckCircle2, FolderTree, FileText } from "lucide-react"
import { postDecision } from "@/ontology/api"
import type { DecisionAction, DomainDraft, OntologyDrafts, PageDraft } from "@/ontology/types"
import { DomainDraftCard } from "@/ontology/components/DomainDraftCard"
import { PageDraftCard } from "@/ontology/components/PageDraftCard"

export function DraftsView({ drafts }: { drafts: OntologyDrafts }) {
  const [domains, setDomains] = useState<DomainDraft[]>(drafts.domains)
  const [pages, setPages] = useState<PageDraft[]>(drafts.pages)
  const [busyId, setBusyId] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)

  // Re-sync when a fresh payload arrives (e.g. after a Refresh).
  useEffect(() => {
    setDomains(drafts.domains)
    setPages(drafts.pages)
  }, [drafts])

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
