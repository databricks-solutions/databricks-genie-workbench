/**
 * WelcomeHero - Atmospheric intro section for the SpaceList page.
 * Shows workspace branding, aggregated stats, and primary CTA.
 */
import { Plus, ArrowRight, Sparkles } from "lucide-react"
import type { SpaceListItem } from "@/types"

interface WelcomeHeroProps {
  spaces: SpaceListItem[]
  loading?: boolean
  onCreateSpace?: () => void
}

function StatPill({ label, value, variant = "default" }: { label: string; value: string | number; variant?: "default" | "danger" }) {
  return (
    <div className={`inline-flex items-center gap-2 px-3 py-1.5 rounded-full border text-sm backdrop-blur-sm ${
      variant === "danger"
        ? "border-danger/30 bg-danger/10"
        : "border-default bg-surface/60"
    }`}>
      <span className={`font-mono font-bold tabular-nums ${variant === "danger" ? "text-danger" : "text-primary"}`}>
        {value}
      </span>
      <span className="text-muted text-xs">{label}</span>
    </div>
  )
}

export function WelcomeHero({ spaces, loading, onCreateSpace }: WelcomeHeroProps) {
  const total = spaces.length
  const scanned = spaces.filter(s => s.score != null).length
  const avgScore = scanned > 0
    ? (spaces.reduce((sum, s) => sum + (s.score ?? 0), 0) / scanned).toFixed(1)
    : "—"
  const critical = spaces.filter(s => s.maturity === "Not Ready").length
  const trusted = spaces.filter(s => s.maturity === "Trusted").length

  return (
    <div className="relative overflow-hidden rounded-2xl border border-default">
      {/* Background layers */}
      <div className="hero-mesh absolute inset-0" />
      <div className="hero-grid absolute inset-0" />

      <div className="relative px-8 py-10 md:py-12">
        {/* Eyebrow */}
        <div
          className="flex items-center gap-2.5 mb-5"
          style={{ animation: "fadeSlideUp 0.5s ease-out 0ms both" }}
        >
          <Sparkles className="w-4 h-4 text-accent" />
          <span className="text-[11px] font-mono text-accent uppercase tracking-[0.2em] font-medium">
            Workspace
          </span>
        </div>

        {/* Title */}
        <h1
          className="text-4xl md:text-5xl font-display font-extrabold text-gradient leading-[1.1] mb-3"
          style={{ animation: "fadeSlideUp 0.5s ease-out 60ms both" }}
        >
          Genie Workbench
        </h1>

        {/* Description */}
        <p
          className="text-secondary text-lg max-w-lg mb-8 leading-relaxed"
          style={{ animation: "fadeSlideUp 0.5s ease-out 120ms both" }}
        >
          Score, optimize, and govern your Databricks Genie Spaces — all in one place.
        </p>

        {/* Stats */}
        <div
          className="flex flex-wrap items-center gap-2.5 mb-8"
          style={{ animation: "fadeSlideUp 0.5s ease-out 180ms both" }}
        >
          {loading ? (
            <>
              {[1, 2, 3].map(i => (
                <div key={i} className="h-8 w-24 rounded-full bg-elevated/60 animate-pulse" />
              ))}
            </>
          ) : total > 0 ? (
            <>
              <StatPill label="spaces" value={total} />
              <StatPill label="scanned" value={scanned > 0 ? `${Math.round(scanned / total * 100)}%` : "0%"} />
              <StatPill label="avg score" value={avgScore} />
              {trusted > 0 && <StatPill label="trusted" value={trusted} />}
              {critical > 0 && <StatPill label="critical" value={critical} variant="danger" />}
            </>
          ) : null}
        </div>

        {/* CTA */}
        {onCreateSpace && (
          <div style={{ animation: "fadeSlideUp 0.5s ease-out 240ms both" }}>
            <button
              onClick={onCreateSpace}
              className="inline-flex items-center gap-2 px-5 py-2.5 rounded-xl gradient-accent text-white text-sm font-semibold shadow-lg shadow-accent/25 hover:shadow-xl hover:shadow-accent/30 hover:-translate-y-0.5 transition-all duration-200"
            >
              <Plus className="w-4 h-4" />
              Create Space
              <ArrowRight className="w-3.5 h-3.5 ml-0.5" />
            </button>
          </div>
        )}
      </div>
    </div>
  )
}
