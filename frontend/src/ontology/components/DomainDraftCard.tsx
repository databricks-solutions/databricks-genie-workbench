/**
 * 17.0d — Domain draft card. Prop-driven and zero-burden (MV-D23): the backend
 * assembles the recommendation, the "why", and the evidence chips; this component
 * renders them and assembles nothing. It shows the plain new-vs-reuse-vs-reassign
 * recommendation, the evidence, proposed sub-domains, member assets, a do-it-yourself
 * checklist + a Copy-for-Discover button, and the Approve / Dismiss (or, for a
 * reassign, Keep current / Accept reassignment) actions. Apply-for-me is DISABLED
 * (17i). No DDL, grants, table names, or backend jargon appears in the copy.
 */
import { useState } from "react"
import { Check, Copy, FolderTree, Sparkles, X } from "lucide-react"
import { Badge } from "@/components/ui/badge"
import { Button } from "@/components/ui/button"
import type { DecisionAction, DomainDraft, EvidenceChip } from "@/ontology/types"

const TIER_LABEL = { high: "High priority", medium: "Worth a look", low: "Lower priority" } as const

const DECISION_LEAD: Record<DomainDraft["tag_decision"], string> = {
  create: "New domain",
  reuse: "Group under an existing domain",
  reassign: "Resolve a tag overlap",
}

export function TierBadge({ tier }: { tier: DomainDraft["tier"] }) {
  return <Badge variant={tier}>{TIER_LABEL[tier]}</Badge>
}

export function EvidenceChips({ chips }: { chips: EvidenceChip[] }) {
  if (chips.length === 0) return null
  return (
    <div className="flex flex-wrap gap-1.5">
      {chips.map((c, i) => (
        <span
          key={`${c.kind}-${i}`}
          className={`inline-flex items-center rounded-full px-2.5 py-0.5 text-xs ${
            c.kind === "conflict"
              ? "bg-warning/10 text-warning-foreground"
              : "bg-elevated text-secondary border border-default"
          }`}
        >
          {c.label}
        </span>
      ))}
    </div>
  )
}

function copyText(draft: DomainDraft): string {
  const lines = [
    `${draft.name}`,
    draft.description,
    "",
    draft.why,
  ]
  if (draft.subdomains.length) lines.push("", `Sub-domains: ${draft.subdomains.join(", ")}`)
  if (draft.members.length) lines.push("", `Assets: ${draft.members.map((m) => m.fqn).join(", ")}`)
  return lines.filter((l) => l !== undefined).join("\n")
}

export function DomainDraftCard({
  draft,
  onDecide,
  busy = false,
}: {
  draft: DomainDraft
  onDecide: (action: DecisionAction) => void
  busy?: boolean
}) {
  const [copied, setCopied] = useState(false)
  const isReassign = draft.tag_decision === "reassign"

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(copyText(draft))
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {
      // Clipboard unavailable — a no-op is fine; nothing to surface.
    }
  }

  return (
    <div className="rounded-xl border border-default bg-surface p-4 space-y-3">
      {/* Recommendation-first header */}
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="flex items-start gap-2.5">
          <FolderTree className="mt-0.5 h-5 w-5 shrink-0 text-accent" />
          <div>
            <div className="flex items-center gap-2">
              <h3 className="text-sm font-semibold text-primary">{draft.name}</h3>
              <TierBadge tier={draft.tier} />
            </div>
            <p className="mt-0.5 text-xs font-medium text-secondary">{DECISION_LEAD[draft.tag_decision]}</p>
          </div>
        </div>
      </div>

      {draft.description && <p className="text-sm text-secondary">{draft.description}</p>}

      {/* Reassign: name the tag it overlaps with. */}
      {isReassign && draft.conflict_tag && (
        <div className="rounded-lg border border-warning/40 bg-warning/5 px-3 py-2 text-xs text-warning-foreground">
          Overlaps with the existing “{draft.conflict_tag}” tag — confirm which grouping is right.
        </div>
      )}

      {/* Why we're suggesting this */}
      <div>
        <p className="text-xs font-semibold uppercase tracking-wide text-muted">Why we&apos;re suggesting this</p>
        <p className="mt-1 text-sm text-secondary">{draft.why}</p>
      </div>

      <EvidenceChips chips={draft.evidence} />

      {draft.subdomains.length > 0 && (
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-muted">Proposed sub-domains</p>
          <div className="mt-1 flex flex-wrap gap-1.5">
            {draft.subdomains.map((s) => (
              <span key={s} className="rounded-full bg-accent/10 px-2.5 py-0.5 text-xs text-accent">
                {s}
              </span>
            ))}
          </div>
        </div>
      )}

      {draft.members.length > 0 && (
        <div>
          <p className="text-xs font-semibold uppercase tracking-wide text-muted">Member assets</p>
          <div className="mt-1 flex flex-wrap gap-1.5">
            {draft.members.slice(0, 12).map((m) => (
              <span key={m.fqn} className="rounded-md bg-elevated px-2 py-0.5 font-mono text-xs text-secondary">
                {m.fqn}
              </span>
            ))}
            {draft.members.length > 12 && (
              <span className="text-xs text-muted">+{draft.members.length - 12} more</span>
            )}
          </div>
        </div>
      )}

      {/* Do-it-yourself checklist (plain product steps). */}
      <details className="rounded-lg border border-default bg-elevated/50 px-3 py-2">
        <summary className="cursor-pointer text-xs font-semibold text-secondary">
          How to set this up yourself
        </summary>
        <ol className="mt-2 list-decimal space-y-1 pl-5 text-xs text-secondary">
          <li>In Catalog Explorer, group these assets under “{draft.name}”.</li>
          {draft.subdomains.length > 0 && <li>Add the sub-domains: {draft.subdomains.join(", ")}.</li>}
          <li>Publish so Genie and Discover pick up the new grouping.</li>
        </ol>
      </details>

      {/* Actions */}
      <div className="flex flex-wrap items-center gap-2 pt-1">
        {isReassign ? (
          <>
            <Button size="sm" variant="success" disabled={busy} onClick={() => onDecide("reassign_accept")}>
              <Check className="h-4 w-4" /> Accept reassignment
            </Button>
            <Button size="sm" variant="secondary" disabled={busy} onClick={() => onDecide("reassign_reject")}>
              Keep current
            </Button>
          </>
        ) : (
          <>
            <Button size="sm" variant="success" disabled={busy} onClick={() => onDecide("approve")}>
              <Check className="h-4 w-4" /> Approve
            </Button>
            <Button size="sm" variant="ghost" disabled={busy} onClick={() => onDecide("dismiss")}>
              <X className="h-4 w-4" /> Dismiss
            </Button>
          </>
        )}
        <Button size="sm" variant="outline" onClick={handleCopy}>
          <Copy className="h-4 w-4" /> {copied ? "Copied" : "Copy for Discover"}
        </Button>
        {/* Apply-for-me is disabled until 17i. */}
        <Button size="sm" variant="secondary" disabled title="Coming soon">
          <Sparkles className="h-4 w-4" /> Apply for me
        </Button>
      </div>
    </div>
  )
}
