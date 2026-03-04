/**
 * Checklist progress component displaying pass/fail items.
 */

import { Check, X } from "lucide-react"
import { Progress } from "@/components/ui/progress"
import { cn } from "@/lib/utils"
import type { ChecklistItem } from "@/types"

interface ChecklistProgressProps {
  checklist: ChecklistItem[]
}

export function ChecklistProgress({ checklist }: ChecklistProgressProps) {
  const passed = checklist.filter((item) => item.passed).length
  const total = checklist.length
  const percentage = total > 0 ? (passed / total) * 100 : 0

  const getProgressVariant = () => {
    if (percentage >= 80) return "success"
    if (percentage >= 60) return "warning"
    return "danger"
  }

  return (
    <div className="space-y-4">
      {/* Progress header */}
      <div>
        <div className="flex items-center justify-between mb-2">
          <span className="font-semibold text-primary">
            Checklist Progress
          </span>
          <span className="text-sm text-muted font-mono">
            {passed}/{total} passed
          </span>
        </div>
        <Progress value={percentage} variant={getProgressVariant()} glow />
      </div>

      {/* Checklist items */}
      {checklist.length > 0 && (
        <div className="space-y-2">
          {checklist.map((item) => (
            <ChecklistItemCard key={item.id} item={item} />
          ))}
        </div>
      )}
    </div>
  )
}

interface ChecklistItemCardProps {
  item: ChecklistItem
}

function ChecklistItemCard({ item }: ChecklistItemCardProps) {
  return (
    <div
      className={cn(
        "flex items-start gap-3 p-3 rounded-lg border-l-4 transition-colors",
        item.passed
          ? "bg-success/5 dark:bg-success/10 border-l-success"
          : "bg-danger/5 dark:bg-danger/10 border-l-danger"
      )}
    >
      <div
        className={cn(
          "flex-shrink-0 w-5 h-5 rounded-full flex items-center justify-center",
          item.passed ? "bg-success text-white" : "bg-danger text-white"
        )}
      >
        {item.passed ? (
          <Check className="w-3 h-3" />
        ) : (
          <X className="w-3 h-3" />
        )}
      </div>
      <div className="flex-1 min-w-0">
        <p
          className={cn(
            "text-sm font-medium",
            item.passed ? "text-success" : "text-danger"
          )}
        >
          {item.description}
        </p>
        {item.details && (
          <p className="text-xs text-muted mt-1">{item.details}</p>
        )}
      </div>
    </div>
  )
}
