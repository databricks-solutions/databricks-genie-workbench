import { useState } from "react"
import { CheckCircle, XCircle, MinusCircle, Search } from "lucide-react"
import type { GSOQuestionDetail } from "@/types"

interface QuestionListProps {
  questions: GSOQuestionDetail[]
  selectedId: string | null
  onSelect: (id: string) => void
}

type Filter = "all" | "passing" | "failing"

export function QuestionList({ questions, selectedId, onSelect }: QuestionListProps) {
  const [search, setSearch] = useState("")
  const [filter, setFilter] = useState<Filter>("all")

  const filtered = questions.filter((q) => {
    if (search) {
      const s = search.toLowerCase()
      if (!q.question.toLowerCase().includes(s) && !q.question_id.toLowerCase().includes(s)) return false
    }
    if (filter === "passing" && q.passed !== true) return false
    if (filter === "failing" && q.passed !== false) return false
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
            const isSelected = q.question_id === selectedId
            return (
              <button
                key={q.question_id}
                onClick={() => onSelect(q.question_id)}
                className={`w-full flex items-start gap-2 px-3 py-2.5 rounded-lg text-left transition-colors ${
                  isSelected
                    ? "bg-accent/10 border border-accent/20"
                    : "hover:bg-elevated border border-transparent"
                }`}
              >
                {q.passed === true ? (
                  <CheckCircle className="w-4 h-4 text-emerald-400 shrink-0 mt-0.5" />
                ) : q.passed === false ? (
                  <XCircle className="w-4 h-4 text-red-400 shrink-0 mt-0.5" />
                ) : (
                  <MinusCircle className="w-4 h-4 text-muted shrink-0 mt-0.5" />
                )}
                <div className="min-w-0">
                  <p className="text-sm text-primary truncate leading-snug">
                    {q.question || q.question_id}
                  </p>
                  {q.question && (
                    <p className="text-xs text-muted truncate mt-0.5">{q.question_id}</p>
                  )}
                </div>
              </button>
            )
          })
        )}
      </div>
    </div>
  )
}
