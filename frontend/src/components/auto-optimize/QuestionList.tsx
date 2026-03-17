import { useState } from "react"
import { CheckCircle, XCircle, Search } from "lucide-react"
import type { GSOQuestionResult } from "@/types"

interface QuestionListProps {
  questions: GSOQuestionResult[]
  selectedId: string | null
  onSelect: (id: string) => void
}

type Filter = "all" | "passing" | "failing"

function isPassing(q: GSOQuestionResult): boolean {
  return q.failure_type == null || q.failure_type === ""
}

export function QuestionList({ questions, selectedId, onSelect }: QuestionListProps) {
  const [search, setSearch] = useState("")
  const [filter, setFilter] = useState<Filter>("all")

  const filtered = questions.filter((q) => {
    if (search && !q.question_id.toLowerCase().includes(search.toLowerCase())) return false
    if (filter === "passing" && !isPassing(q)) return false
    if (filter === "failing" && isPassing(q)) return false
    return true
  })

  const filters: { id: Filter; label: string }[] = [
    { id: "all", label: "All" },
    { id: "passing", label: "Passing" },
    { id: "failing", label: "Failing" },
  ]

  return (
    <div className="flex flex-col h-full">
      {/* Search */}
      <div className="relative mb-3">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-muted" />
        <input
          type="text"
          placeholder="Search questions..."
          value={search}
          onChange={(e) => setSearch(e.target.value)}
          className="w-full pl-9 pr-3 py-2 text-sm rounded-lg border border-default bg-surface text-primary placeholder:text-muted focus:outline-none focus:ring-2 focus:ring-accent/50"
        />
      </div>

      {/* Filter buttons */}
      <div className="flex gap-1 mb-3">
        {filters.map((f) => (
          <button
            key={f.id}
            onClick={() => setFilter(f.id)}
            className={`px-2.5 py-1 text-xs rounded-md font-medium transition-colors ${
              filter === f.id
                ? "bg-accent/10 text-accent"
                : "text-muted hover:text-primary hover:bg-elevated"
            }`}
          >
            {f.label}
          </button>
        ))}
      </div>

      {/* Question list */}
      <div className="flex-1 overflow-y-auto space-y-0.5">
        {filtered.length === 0 ? (
          <p className="text-xs text-muted py-4 text-center">No questions match</p>
        ) : (
          filtered.map((q) => {
            const pass = isPassing(q)
            const isSelected = q.question_id === selectedId
            return (
              <button
                key={q.question_id + q.judge}
                onClick={() => onSelect(q.question_id)}
                className={`w-full flex items-start gap-2 px-3 py-2 rounded-lg text-left text-sm transition-colors ${
                  isSelected
                    ? "bg-accent/10 border border-accent/20"
                    : "hover:bg-elevated border border-transparent"
                }`}
              >
                {pass ? (
                  <CheckCircle className="w-4 h-4 text-emerald-400 shrink-0 mt-0.5" />
                ) : (
                  <XCircle className="w-4 h-4 text-red-400 shrink-0 mt-0.5" />
                )}
                <span className="text-primary truncate">{q.question_id}</span>
              </button>
            )
          })
        )}
      </div>
    </div>
  )
}
