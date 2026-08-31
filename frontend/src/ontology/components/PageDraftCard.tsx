/**
 * 17.0e — Page draft card. Prop-driven and zero-burden (MV-D23). Leads with the
 * reason, shows prominent Synonyms, Related / Sources chips, a certify recommendation,
 * a do-it-yourself checklist + a Copy-for-Discover button, and Approve / Dismiss.
 * Apply-for-me is DISABLED (17i). No DDL, table names, or backend jargon in the copy.
 */
import { useState } from "react"
import { BadgeCheck, Check, Copy, FileText, Sparkles, X } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import type { DecisionAction, PageDraft } from "@/ontology/types"
import { EvidenceChips, TierBadge } from "@/ontology/components/DomainDraftCard"

function copyText(draft: PageDraft): string {
  const lines = [draft.title, "", draft.reason, "", draft.body]
  if (draft.synonyms.length) lines.push("", `Also called: ${draft.synonyms.join(", ")}`)
  if (draft.source_fqns.length) lines.push("", `Sources: ${draft.source_fqns.join(", ")}`)
  return lines.join("\n")
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

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(copyText(draft))
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      // Clipboard unavailable — no-op.
    }
  }

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

      {draft.related_fqns.length > 0 && (
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-muted">Related</p>
          <div className="mt-1 flex flex-wrap gap-1.5">
            {draft.related_fqns.map((r) => (
              <span key={r} className="rounded-md bg-elevated px-2 py-0.5 font-mono text-xs text-secondary">
                {r}
              </span>
            ))}
          </div>
        </div>
      )}

      {draft.source_fqns.length > 0 && (
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-muted">Sources</p>
          <div className="mt-1 flex flex-wrap gap-1.5">
            {draft.source_fqns.map((s) => (
              <span key={s} className="rounded-md bg-elevated px-2 py-0.5 font-mono text-xs text-secondary">
                {s}
              </span>
            ))}
          </div>
        </div>
      )}

      <details className="rounded-lg border border-default bg-elevated/50 px-3 py-2">
        <summary className="cursor-pointer text-xs font-semibold text-secondary">
          How to add this yourself
        </summary>
        <ol className="mt-2 list-decimal space-y-1 pl-5 text-xs text-secondary">
          <li>In Discover, add a page for “{draft.title}”.</li>
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
        <Button size="sm" variant="secondary" disabled title="Coming soon">
          <Sparkles className="h-4 w-4" /> Apply for me
        </Button>
      </div>
    </div>
  )
}
