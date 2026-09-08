/**
 * Space-config diff for the suggest-only panel (Prompt 13, POV §7.5).
 *
 * Shows what attaching the proposed metric view would add to the Agent's
 * data_sources.metric_views[]. The proposed side is synthesized CLIENT-SIDE —
 * the current identifiers with proposed_object appended — the same precedent the
 * Prompt 12 Model tab set for its proposal overlay. No endpoint renders a patch.
 */
import ReactDiffViewer, { DiffMethod } from "react-diff-viewer-continued"
import { diffViewerStyles } from "@/lib/diffViewerStyles"
import { useTheme } from "@/hooks/useTheme"

interface MvSpaceConfigDiffProps {
  /** Current data_sources.metric_views[] identifiers for the Agent. */
  currentIdentifiers: string[]
  /** The proposed object's full name (data_sources.metric_views[].identifier). */
  proposedObject: string
}

export function MvSpaceConfigDiff({
  currentIdentifiers,
  proposedObject,
}: MvSpaceConfigDiffProps) {
  const { isDark } = useTheme()

  // Already present (e.g. a re-proposed object) — nothing to add, no diff.
  if (currentIdentifiers.includes(proposedObject)) return null

  const current = [...currentIdentifiers].sort()
  const proposed = [...current, proposedObject].sort()
  const oldValue = current.length ? current.join("\n") : "(no metric views)"
  const newValue = proposed.join("\n")

  return (
    <div className="rounded-lg overflow-hidden border border-default">
      <div className="flex border-b border-default">
        <div className="flex-1 px-4 py-2 bg-elevated text-sm font-medium text-secondary">
          Current metric views
        </div>
        <div className="flex-1 px-4 py-2 bg-elevated text-sm font-medium text-secondary border-l border-default">
          With this metric view attached
        </div>
      </div>
      <ReactDiffViewer
        oldValue={oldValue}
        newValue={newValue}
        splitView={true}
        useDarkTheme={isDark}
        compareMethod={DiffMethod.LINES}
        styles={diffViewerStyles}
        hideLineNumbers={false}
        showDiffOnly={false}
      />
    </div>
  )
}
