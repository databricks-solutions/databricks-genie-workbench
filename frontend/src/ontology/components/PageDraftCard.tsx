/**
 * 17.0e — Page draft card. Prop-driven and zero-burden (MV-D23). Leads with the
 * reason, shows prominent Synonyms, Related / Sources chips, a certify recommendation,
 * a do-it-yourself checklist + a Copy-for-Discover button, and Approve / Dismiss.
 * Apply-for-me is DISABLED (17i). No DDL, table names, or backend jargon in the copy.
 * Step 3 (MV-D66): adds a "Draft with AI" button that calls draftPageBody on the page.
 */
import { useCallback, useState } from "react"
import { BadgeCheck, Check, Copy, FileText, Loader2, Sparkles, X } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import { draftPageBody } from "@/ontology/api"
import type { DecisionAction, PageDraft } from "@/ontology/types"
import { EvidenceChips, TierBadge } from "@/ontology/components/DomainDraftCard"

function copyText(draft: PageDraft): string {
  const lines = [draft.title, "", draft.reason, "", draft.body]
  if (draft.synonyms.length) lines.push("", `Also called: ${draft.synonyms.join(", ")}`)
  if (draft.source_fqns.length) lines.push("", `Sources: ${draft.source_fqns.join(", ")}`)
  return lines.join("\n")
}

/**
 * A labelled list of Source/Related assets, each with its one-line "why this asset"
 * (MV-D55) beneath. Reuses the card's mono chip styling; the reason is muted so the
 * asset FQN stays primary.
 */
function AssetRows({
  label,
  fqns,
  why,
}: {
  label: string
  fqns: string[]
  why: Record<string, string>
}) {
  if (!fqns.length) return null
  return (
    <div>
      <p className="text-xs font-semibold uppercase tracking-wide text-muted">{label}</p>
      <ul className="mt-1 space-y-1.5">
        {fqns.map((f) => (
          <li key={f}>
            <span className="inline-block rounded-md bg-elevated px-2 py-0.5 font-mono text-xs text-secondary">
              {f}
            </span>
            {why[f] && <p className="mt-0.5 text-xs text-muted">{why[f]}</p>}
          </li>
        ))}
      </ul>
    </div>
  )
}

export function PageDraftCard({
  draft,
  onDecide,
  busy = false,
}: {
  draft: PageDraft
  onDecide: (action: DecisionAction) => void
  busy?: boolean
}) {
  const [copied, setCopied] = useState(false)
  const [draftedBody, setDraftedBody] = useState<string | null>(null)
  const [drafting, setDrafting] = useState(false)
  const [draftError, setDraftError] = useState<string | null>(null)

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(copyText(draft))
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      // Clipboard unavailable — no-op.
    }
  }

  const handleDraft = useCallback(async () => {
    setDrafting(true)
    setDraftError(null)
    try {
      const resp = await draftPageBody(draft.proposal_id)
      if (resp.ok) {
        setDraftedBody(resp.body)
      } else {
        setDraftError(resp.reason || "Drafting failed — try again")
      }
    } catch (e) {
      setDraftError(e instanceof Error ? e.message : "Failed to draft body")
    } finally {
      setDrafting(false)
    }
  }, [draft.proposal_id])

  return (
    <div className="rounded-xl border border-default bg-surface p-4 space-y-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="flex items-start gap-2.5">
          <FileText className="mt-0.5 h-5 w-5 shrink-0 text-accent" />
          <div>
            <div className="flex items-center gap-2">
              <Badge variant="secondary">{draft.archetype}</Badge>
              <TierBadge tier={draft.tier} />
              {draft.certify && (
                <span className="inline-flex items-center gap-1 text-xs font-medium text-success-foreground">
                  <BadgeCheck className="h-3.5 w-3.5" /> Recommended to certify
                </span>
              )}
            </div>
            <h3 className="mt-1 text-sm font-semibold text-primary">{draft.title}</h3>
          </div>
        </div>
      </div>

      {/* Reason leads the card. */}
      <p className="text-sm text-secondary">{draft.reason}</p>

      {/* Drafted body (Step 3 MV-D66): show on success, deterministic stub otherwise */}
      {draftedBody ? (
        <div className="rounded-lg border border-default bg-elevated/50 px-3 py-2">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted">Drafted description</p>
          <p className="mt-1 text-sm text-secondary">{draftedBody}</p>
        </div>
      ) : (
        <div className="rounded-lg border border-default/50 bg-elevated/25 px-3 py-2">
          <p className="text-xs font-semibold uppercase tracking-wide text-muted">Description</p>
          <p className="mt-1 text-sm text-secondary">{draft.body}</p>
        </div>
      )}

      {draftError && (
        <div className="rounded-lg border border-danger/30 bg-danger/5 px-3 py-2 text-xs text-danger-foreground">
          {draftError}
        </div>
      )}

      <EvidenceChips chips={draft.evidence} />

      {/* Prominent Synonyms — this is what makes the Page findable. */}
      {draft.synonyms.length > 0 && (
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-muted">Also called</p>
          <div className="mt-1 flex flex-wrap gap-1.5">
            {draft.synonyms.map((s) => (
              <span key={s} className="rounded-full bg-accent/10 px-2.5 py-0.5 text-xs text-accent">
                {s}
              </span>
            ))}
          </div>
        </div>
      )}

      <AssetRows label="Related" fqns={draft.related_fqns} why={draft.asset_why ?? {}} />
      <AssetRows label="Sources" fqns={draft.source_fqns} why={draft.asset_why ?? {}} />

      <details className="rounded-lg border border-default bg-elevated/50 px-3 py-2">
        <summary className="cursor-pointer text-xs font-semibold text-secondary">
          How to add this yourself
        </summary>
        <ol className="mt-2 list-decimal space-y-1 pl-5 text-xs text-secondary">
          <li>In Discover, add a page for "{draft.title}".</li>
          <li>Paste the description and synonyms (use Copy for Discover).</li>
          {draft.certify && <li>Certify the page so Genie treats it as trusted.</li>}
        </ol>
      </details>

      <div className="flex flex-wrap items-center gap-2 pt-1">
        <Button size="sm" variant="success" disabled={busy} onClick={() => onDecide("approve")}>
          <Check className="h-4 w-4" /> Approve
        </Button>
        <Button size="sm" variant="ghost" disabled={busy} onClick={() => onDecide("dismiss")}>
          <X className="h-4 w-4" /> Dismiss
        </Button>
        <Button size="sm" variant="outline" onClick={handleCopy}>
          <Copy className="h-4 w-4" /> {copied ? "Copied" : "Copy for Discover"}
        </Button>
        <Button size="sm" variant="secondary" disabled={drafting} onClick={handleDraft} title={draftedBody ? "Body drafted" : undefined}>
          {drafting ? (
            <Loader2 className="mr-1 h-4 w-4 animate-spin" />
          ) : (
            <Sparkles className="mr-1 h-4 w-4" />
          )}
          {drafting ? "Drafting…" : draftedBody ? "Drafted" : "Draft with AI"}
        </Button>
      </div>
    </div>
  )
}
