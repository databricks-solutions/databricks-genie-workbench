/**
 * Data table component for displaying SQL query results.
 */

import { cn } from "@/lib/utils"
import type { SqlExecutionColumn } from "@/types"

interface DataTableProps {
  columns: SqlExecutionColumn[]
  data: (string | number | boolean | null)[][]
  maxHeight?: string
  truncated?: boolean
  className?: string
}

export function DataTable({
  columns,
  data,
  maxHeight = "300px",
  truncated = false,
  className,
}: DataTableProps) {
  if (columns.length === 0 || data.length === 0) {
    return (
      <div className="text-center py-4 text-muted text-sm">
        No results
      </div>
    )
  }

  return (
    <div className={cn("space-y-2", className)}>
      <div
        className="overflow-auto border border-default rounded-lg"
        style={{ maxHeight }}
      >
        <table className="w-full text-sm">
          <thead className="bg-elevated sticky top-0">
            <tr>
              {columns.map((col, idx) => (
                <th
                  key={idx}
                  className="px-3 py-2 text-left font-medium text-secondary border-b border-default whitespace-nowrap"
                >
                  <div className="flex flex-col">
                    <span>{col.name}</span>
                    <span className="text-xs text-muted font-normal">
                      {col.type_name}
                    </span>
                  </div>
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-default">
            {data.map((row, rowIdx) => (
              <tr
                key={rowIdx}
                className="hover:bg-sunken transition-colors"
              >
                {row.map((cell, cellIdx) => (
                  <td
                    key={cellIdx}
                    className="px-3 py-2 text-primary whitespace-nowrap font-mono text-xs"
                  >
                    {cell === null ? (
                      <span className="text-muted italic">null</span>
                    ) : (
                      String(cell)
                    )}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="flex items-center justify-between text-xs text-muted">
        <span>
          {data.length} row{data.length !== 1 ? "s" : ""}
        </span>
        {truncated && (
          <span className="text-warning">
            Results truncated
          </span>
        )}
      </div>
    </div>
  )
}
