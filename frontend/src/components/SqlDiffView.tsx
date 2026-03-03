/**
 * Side-by-side SQL diff view comparing generated SQL vs expected SQL.
 */

import ReactDiffViewer, { DiffMethod } from "react-diff-viewer-continued"
import { diffViewerStyles } from "@/lib/diffViewerStyles"
import { useTheme } from "@/hooks/useTheme"

interface SqlDiffViewProps {
  generatedSql: string
  expectedSql: string
}

export function SqlDiffView({ generatedSql, expectedSql }: SqlDiffViewProps) {
  const { isDark } = useTheme()

  return (
    <div className="rounded-lg overflow-hidden border border-default">
      <div className="flex border-b border-default">
        <div className="flex-1 px-4 py-2 bg-elevated text-sm font-medium text-secondary">
          Generated SQL (Genie)
        </div>
        <div className="flex-1 px-4 py-2 bg-elevated text-sm font-medium text-secondary border-l border-default">
          Expected SQL (Benchmark)
        </div>
      </div>
      <ReactDiffViewer
        oldValue={generatedSql}
        newValue={expectedSql}
        splitView={true}
        useDarkTheme={isDark}
        compareMethod={DiffMethod.WORDS}
        styles={diffViewerStyles}
        hideLineNumbers={false}
        showDiffOnly={false}
      />
    </div>
  )
}
