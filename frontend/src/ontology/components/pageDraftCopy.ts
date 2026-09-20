/**
 * Copy-for-Discover text for a Page draft. Extracted from PageDraftCard so it can be
 * unit-tested directly (a component file may export only components — react-refresh).
 * Stage 4.1j (MV-D102/D103): carries the Related assets (with their per-asset "why") and
 * the best-effort external Links blocks so a curator's paste into Discover is complete.
 */
import type { PageDraft } from "@/ontology/types"

export function copyText(draft: PageDraft): string {
  const lines = [draft.title, "", draft.reason, "", draft.body]
  if (draft.synonyms.length) lines.push("", `Also called: ${draft.synonyms.join(", ")}`)
  // Related assets carry their per-asset "why" into the Discover copy.
  if (draft.related_fqns.length) {
    lines.push("", "Related assets:")
    for (const f of draft.related_fqns) {
      const why = draft.asset_why?.[f]
      lines.push(why ? `- ${f} — ${why}` : `- ${f}`)
    }
  }
  if (draft.source_fqns.length) lines.push("", `Sources: ${draft.source_fqns.join(", ")}`)
  // Best-effort external Links, each with its not-certified label.
  if (draft.links?.length) {
    lines.push("", "Links:")
    for (const l of draft.links) {
      const label = l.title || l.url
      lines.push(`- ${label} (${l.url})${l.note ? ` — ${l.note}` : ""}`)
    }
  }
  return lines.join("\n")
}
