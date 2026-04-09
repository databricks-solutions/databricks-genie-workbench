import { User, Bot, ArrowRight } from "lucide-react"
import { cn } from "@/lib/utils"

interface PermRow {
  operation: string
  identity: "user" | "service-principal" | "both"
  note?: string
}

const rows: PermRow[] = [
  { operation: "Browse Unity Catalog", identity: "user" },
  { operation: "Query Genie Spaces", identity: "user", note: "Falls back to SP if scope missing" },
  { operation: "Create Genie Space", identity: "user" },
  { operation: "IQ Scan (Score)", identity: "user" },
  { operation: "Fix Agent (Apply Patches)", identity: "user" },
  { operation: "Trigger Auto-Optimize", identity: "both", note: "User triggers, SP executes job" },
  { operation: "Run Optimization Job", identity: "service-principal" },
  { operation: "Read/Write Lakebase", identity: "service-principal" },
]

export function PermissionDiagram({ className }: { className?: string }) {
  return (
    <div className={cn("space-y-6", className)}>
      {/* Visual: two identity lanes */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {/* OBO Lane */}
        <div className="rounded-xl border-2 border-accent/30 bg-accent/5 p-5">
          <div className="flex items-center gap-3 mb-4">
            <div className="flex h-10 w-10 items-center justify-center rounded-full bg-accent/20">
              <User className="h-5 w-5 text-accent" />
            </div>
            <div>
              <h4 className="font-display font-bold text-primary text-sm">On-Behalf-Of (OBO)</h4>
              <p className="text-xs text-muted">User's own identity via token</p>
            </div>
          </div>
          <div className="space-y-2">
            {["Browse catalogs & schemas", "List & query Genie Spaces", "Create new Genie Spaces", "Score spaces (IQ Scan)", "Apply fixes (Fix Agent)", "Trigger optimization"].map(
              (item) => (
                <div key={item} className="flex items-center gap-2 text-sm text-secondary">
                  <div className="h-1.5 w-1.5 rounded-full bg-accent shrink-0" />
                  {item}
                </div>
              ),
            )}
          </div>
        </div>

        {/* SP Lane */}
        <div className="rounded-xl border-2 border-cyan/30 bg-cyan/5 p-5">
          <div className="flex items-center gap-3 mb-4">
            <div className="flex h-10 w-10 items-center justify-center rounded-full bg-cyan/20">
              <Bot className="h-5 w-5 text-cyan" />
            </div>
            <div>
              <h4 className="font-display font-bold text-primary text-sm">Service Principal (SP)</h4>
              <p className="text-xs text-muted">App's own machine identity</p>
            </div>
          </div>
          <div className="space-y-2">
            {["Run optimization Lakeflow Job", "Read/write Lakebase storage", "Fallback for Genie API scope gap", "Access benchmark results", "Write optimization state", "Deploy approved changes"].map(
              (item) => (
                <div key={item} className="flex items-center gap-2 text-sm text-secondary">
                  <div className="h-1.5 w-1.5 rounded-full bg-cyan shrink-0" />
                  {item}
                </div>
              ),
            )}
          </div>
        </div>
      </div>

      {/* Handoff visual */}
      <div className="flex items-center justify-center gap-3 py-3 px-4 rounded-lg bg-elevated border border-default">
        <div className="flex items-center gap-1.5 text-xs font-medium text-accent">
          <User className="h-4 w-4" /> User triggers
        </div>
        <ArrowRight className="h-4 w-4 text-muted" />
        <div className="text-xs text-muted">permission verified</div>
        <ArrowRight className="h-4 w-4 text-muted" />
        <div className="flex items-center gap-1.5 text-xs font-medium text-cyan">
          <Bot className="h-4 w-4" /> SP executes
        </div>
      </div>

      {/* Detailed table */}
      <div className="overflow-x-auto rounded-lg border border-default">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-elevated">
              <th className="text-left font-semibold text-primary px-4 py-2.5">Operation</th>
              <th className="text-center font-semibold text-primary px-4 py-2.5">Identity</th>
              <th className="text-left font-semibold text-primary px-4 py-2.5">Notes</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.operation} className="border-t border-default">
                <td className="px-4 py-2.5 text-secondary">{row.operation}</td>
                <td className="px-4 py-2.5 text-center">
                  {row.identity === "user" && (
                    <span className="inline-flex items-center gap-1 rounded-full bg-accent/10 px-2.5 py-0.5 text-xs font-medium text-accent">
                      <User className="h-3 w-3" /> OBO
                    </span>
                  )}
                  {row.identity === "service-principal" && (
                    <span className="inline-flex items-center gap-1 rounded-full bg-cyan/10 px-2.5 py-0.5 text-xs font-medium text-cyan">
                      <Bot className="h-3 w-3" /> SP
                    </span>
                  )}
                  {row.identity === "both" && (
                    <span className="inline-flex items-center gap-1 rounded-full bg-warning/10 px-2.5 py-0.5 text-xs font-medium text-warning">
                      OBO → SP
                    </span>
                  )}
                </td>
                <td className="px-4 py-2.5 text-muted text-xs">{row.note ?? "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
