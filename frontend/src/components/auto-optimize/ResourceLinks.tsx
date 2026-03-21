import {
  ExternalLink,
  FlaskConical,
  BriefcaseBusiness,
  Database,
  Sparkles,
} from "lucide-react"
import type { GSOResourceLink } from "@/types"

interface ResourceLinksProps {
  links: GSOResourceLink[]
}

const categoryConfig: Record<string, {
  icon: React.ReactNode
  title: string
  color: string
  bgColor: string
  borderColor: string
}> = {
  genie: {
    icon: <Sparkles className="h-4 w-4" />,
    title: "Genie",
    color: "text-purple-700 dark:text-purple-400",
    bgColor: "bg-purple-50 dark:bg-purple-950/30",
    borderColor: "border-purple-200 dark:border-purple-800",
  },
  job: {
    icon: <BriefcaseBusiness className="h-4 w-4" />,
    title: "Jobs",
    color: "text-blue-700 dark:text-blue-400",
    bgColor: "bg-blue-50 dark:bg-blue-950/30",
    borderColor: "border-blue-200 dark:border-blue-800",
  },
  mlflow: {
    icon: <FlaskConical className="h-4 w-4" />,
    title: "MLflow",
    color: "text-emerald-700 dark:text-emerald-400",
    bgColor: "bg-emerald-50 dark:bg-emerald-950/30",
    borderColor: "border-emerald-200 dark:border-emerald-800",
  },
  data: {
    icon: <Database className="h-4 w-4" />,
    title: "Data",
    color: "text-amber-700 dark:text-amber-400",
    bgColor: "bg-amber-50 dark:bg-amber-950/30",
    borderColor: "border-amber-200 dark:border-amber-800",
  },
}

export function ResourceLinks({ links }: ResourceLinksProps) {
  if (!links.length) return null

  const grouped = links.reduce<Record<string, GSOResourceLink[]>>((acc, link) => {
    const cat = link.category || "other"
    if (!acc[cat]) acc[cat] = []
    acc[cat].push(link)
    return acc
  }, {})

  const categoryOrder = ["genie", "job", "mlflow", "data"]
  const sortedCategories = Object.keys(grouped).sort(
    (a, b) =>
      (categoryOrder.indexOf(a) === -1 ? 99 : categoryOrder.indexOf(a)) -
      (categoryOrder.indexOf(b) === -1 ? 99 : categoryOrder.indexOf(b))
  )

  return (
    <div className="space-y-3">
      <h3 className="flex items-center gap-1.5 text-xs font-semibold uppercase tracking-wide text-muted">
        <ExternalLink className="h-3.5 w-3.5" />
        Databricks Resources
      </h3>

      <div className="grid grid-cols-1 gap-2 sm:grid-cols-2">
        {sortedCategories.map((cat) => {
          const cfg = categoryConfig[cat] || {
            icon: <ExternalLink className="h-4 w-4" />,
            title: cat,
            color: "text-gray-700 dark:text-gray-400",
            bgColor: "bg-gray-50 dark:bg-gray-900",
            borderColor: "border-gray-200 dark:border-gray-700",
          }
          const items = grouped[cat]

          return (
            <div
              key={cat}
              className={`rounded-lg border ${cfg.borderColor} ${cfg.bgColor} p-3`}
            >
              <div className={`mb-2 flex items-center gap-1.5 text-xs font-semibold ${cfg.color}`}>
                {cfg.icon}
                {cfg.title}
              </div>
              <div className="space-y-1">
                {items.map((link, i) => (
                  <a
                    key={i}
                    href={link.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className={`group flex items-center gap-1.5 rounded px-1.5 py-1 text-xs font-medium transition-colors hover:bg-white/70 dark:hover:bg-white/10 ${cfg.color}`}
                  >
                    <span className="truncate">{link.label}</span>
                    <ExternalLink className="h-3 w-3 shrink-0 opacity-0 transition-opacity group-hover:opacity-100" />
                  </a>
                ))}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
