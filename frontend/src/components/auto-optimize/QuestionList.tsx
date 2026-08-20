import { useState } from "react"
import { CheckCircle, XCircle, MinusCircle, AlertCircle, Search } from "lucide-react"
import type { GSOQuestionDetail } from "@/types"
import { questionState } from "@/lib/assessment"

interface QuestionListProps {
  questions: GSOQuestionDetail[]
  selectedId: string | null
  onSelect: (id: string) => void
}

type Filter = "all" | "passing" | "failing" | "needs_review"

export function QuestionList({ questions, selectedId, onSelect }: QuestionListProps) {
  const [search, setSearch] = useState("")
  const [filter, setFilter] = useState<Filter>("all")

  const filtered = questions.filter((q) => {
    if (search) {
      const s = search.toLowerCase()
      if (!q.question.toLowerCase().includes(s) && !q.question_id.toLowerCase().includes(s)) return false
    }
    const state = questionState(q)
    if (filter === "passing" && state !== "passing") return false
    if (filter === "failing" && state !== "failing") return false
    if (filter === "needs_review" && state !== "needs_review") return false
    return true
  })

  const filters: { id: Filter; label: string }[] = [
    { id: "all", label: "All" },
    { id: "passing", label: "Passing" },
    { id: "failing", label: "Failing" },
    { id: "needs_review", label: "Needs Review" },
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
            const state = questionState(q)
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
                {state === "passing" ? (
                  <CheckCircle className="w-4 h-4 text-emerald-400 shrink-0 mt-0.5" />
                ) : state === "failing" ? (
                  <XCircle className="w-4 h-4 text-red-400 shrink-0 mt-0.5" />
                ) : state === "needs_review" ? (
                  <AlertCircle className="w-4 h-4 text-amber-400 shrink-0 mt-0.5" />
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
