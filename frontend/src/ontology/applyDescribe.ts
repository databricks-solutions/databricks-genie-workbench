// Phase 5 (17i) apply — plain-language phrasing for a pending governed-tag change.
// Kept out of the component file so it can be unit-tested directly and so the
// component module stays component-only (react-refresh). The MV-D23 zero-burden
// contract lives here: describe the EFFECT of a change, never the SQL that realizes
// it (the server owns `ApplyItem.statement`; it is never rendered).
import type { ApplyItem } from "@/ontology/types"

/** The short, human name of an asset (drop the catalog/schema qualifier). */
export function shortName(fqn: string): string {
  const parts = fqn.split(".")
  return parts[parts.length - 1] || fqn
}

/** One pending change, described by its human effect — never its SQL (MV-D23). */
export function describeChange(item: ApplyItem): string {
  const grouping = item.tag_value || item.tag_key
  if (item.shape === "create_tag") return `Create the “${grouping}” grouping`
  if (item.shape === "unset_tag") return `Remove ${shortName(item.target_fqn)} from “${item.tag_key}”`
  // set_tag: an add (no current value) or a move (had a different value)
  if (item.current_value && item.current_value !== item.tag_value) {
    return `Move ${shortName(item.target_fqn)} from “${item.current_value}” to “${grouping}”`
  }
  return `Organize ${shortName(item.target_fqn)} under “${grouping}”`
}
