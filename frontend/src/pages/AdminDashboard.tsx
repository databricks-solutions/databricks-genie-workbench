/**
 * AdminDashboard - Org-wide statistics, leaderboard, and critical alerts.
 */
import { useState, useEffect } from "react"
import { TrendingUp, TrendingDown, AlertTriangle, Award, BarChart2, RefreshCw } from "lucide-react"
import { getAdminDashboard, getLeaderboard, getAlerts } from "@/lib/api"
import { getScoreColor, MATURITY_COLORS } from "@/lib/utils"
import type { AdminDashboardStats, LeaderboardEntry, AlertItem } from "@/types"

interface AdminDashboardProps {
  onSelectSpace?: (spaceId: string, displayName: string) => void
}

function StatCard({ label, value, sub, icon }: { label: string; value: string | number; sub?: string; icon: React.ReactNode }) {
  return (
    <div className="bg-surface border border-default rounded-xl p-4">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs text-muted uppercase tracking-wide">{label}</span>
        <span className="text-muted opacity-60">{icon}</span>
      </div>
      <div className="text-2xl font-bold text-primary">{value}</div>
      {sub && <div className="text-xs text-muted mt-1">{sub}</div>}
    </div>
  )
}

function ScoreBadge({ score }: { score: number }) {
  return <span className={`font-bold text-lg ${getScoreColor(score)}`}>{score}</span>
}

export function AdminDashboard({ onSelectSpace }: AdminDashboardProps) {
  const [stats, setStats] = useState<AdminDashboardStats | null>(null)
  const [leaderboard, setLeaderboard] = useState<{ top: LeaderboardEntry[]; bottom: LeaderboardEntry[] } | null>(null)
  const [alerts, setAlerts] = useState<AlertItem[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const loadData = async () => {
    setLoading(true)
    setError(null)
    try {
      const [statsData, lbData, alertsData] = await Promise.all([
        getAdminDashboard(),
        getLeaderboard(),
        getAlerts(),
      ])
      setStats(statsData)
      setLeaderboard(lbData)
      setAlerts(alertsData)
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load admin data")
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { loadData() }, [])

  if (loading) {
    return (
      <div className="space-y-6">
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          {Array.from({ length: 4 }).map((_, i) => (
            <div key={i} className="bg-surface border border-default rounded-xl p-4 animate-pulse h-24" />
          ))}
        </div>
      </div>
    )
  }

  if (error) {
    return (
      <div className="flex items-center gap-3 p-4 bg-red-500/10 border border-red-500/20 rounded-xl text-red-400">
        <AlertTriangle className="w-5 h-5" />
        <span>{error}</span>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-2xl font-display font-bold text-primary">Admin Dashboard</h2>
          <p className="text-muted mt-1">Org-wide Genie Space health</p>
        </div>
        <button onClick={loadData} className="flex items-center gap-2 px-3 py-2 rounded-lg border border-default bg-surface hover:bg-surface-secondary text-sm text-secondary transition-colors">
          <RefreshCw className="w-4 h-4" />
          Refresh
        </button>
      </div>

      {/* Stats */}
      {stats && (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <StatCard label="Total Spaces" value={stats.total_spaces} icon={<BarChart2 className="w-4 h-4" />} />
          <StatCard label="Scanned" value={stats.scanned_spaces} sub={`${stats.total_spaces > 0 ? Math.round(stats.scanned_spaces / stats.total_spaces * 100) : 0}% coverage`} icon={<BarChart2 className="w-4 h-4" />} />
          <StatCard label="Avg Score" value={stats.avg_score} icon={<TrendingUp className="w-4 h-4" />} />
          <StatCard label="Critical" value={stats.critical_count} sub="score ≤ 20" icon={<AlertTriangle className="w-4 h-4" />} />
        </div>
      )}

      {/* Maturity distribution */}
      {stats && Object.keys(stats.maturity_distribution).length > 0 && (
        <div className="bg-surface border border-default rounded-xl p-5">
          <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4">Maturity Distribution</h3>
          <div className="space-y-2">
            {Object.entries(stats.maturity_distribution).sort((a, b) => b[1] - a[1]).map(([label, count]) => {
              const pct = stats.scanned_spaces > 0 ? (count / stats.scanned_spaces) * 100 : 0
              const color = MATURITY_COLORS[label]?.bar || "bg-red-500"
              return (
                <div key={label}>
                  <div className="flex justify-between text-sm mb-1">
                    <span className="text-secondary">{label}</span>
                    <span className="text-muted">{count} ({Math.round(pct)}%)</span>
                  </div>
                  <div className="h-2 bg-surface-secondary rounded-full overflow-hidden">
                    <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Leaderboard */}
        {leaderboard && (
          <div className="bg-surface border border-default rounded-xl p-5">
            <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4 flex items-center gap-2">
              <Award className="w-4 h-4 text-amber-400" />
              Top Spaces
            </h3>
            <div className="space-y-2">
              {leaderboard.top.map((entry, i) => (
                <div
                  key={entry.space_id}
                  onClick={() => onSelectSpace?.(entry.space_id, entry.display_name)}
                  className="flex items-center gap-3 p-2 rounded-lg hover:bg-surface-secondary cursor-pointer transition-colors"
                >
                  <span className="w-5 text-xs text-muted font-medium">{i + 1}</span>
                  <span className="flex-1 text-sm text-primary truncate">{entry.display_name}</span>
                  <ScoreBadge score={entry.score} />
                </div>
              ))}
            </div>
            {leaderboard.bottom.length > 0 && (
              <>
                <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mt-5 mb-4 flex items-center gap-2">
                  <TrendingDown className="w-4 h-4 text-red-400" />
                  Needs Attention
                </h3>
                <div className="space-y-2">
                  {leaderboard.bottom.map((entry, i) => (
                    <div
                      key={entry.space_id}
                      onClick={() => onSelectSpace?.(entry.space_id, entry.display_name)}
                      className="flex items-center gap-3 p-2 rounded-lg hover:bg-surface-secondary cursor-pointer transition-colors"
                    >
                      <span className="w-5 text-xs text-muted font-medium">{i + 1}</span>
                      <span className="flex-1 text-sm text-primary truncate">{entry.display_name}</span>
                      <ScoreBadge score={entry.score} />
                    </div>
                  ))}
                </div>
              </>
            )}
          </div>
        )}

        {/* Alerts */}
        {alerts.length > 0 && (
          <div className="bg-surface border border-default rounded-xl p-5">
            <h3 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4 flex items-center gap-2">
              <AlertTriangle className="w-4 h-4 text-red-400" />
              Critical Alerts
            </h3>
            <div className="space-y-3">
              {alerts.map(alert => (
                <div
                  key={alert.space_id}
                  onClick={() => onSelectSpace?.(alert.space_id, alert.display_name)}
                  className="p-3 rounded-lg border border-red-500/20 bg-red-500/5 hover:bg-red-500/10 cursor-pointer transition-colors"
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-sm font-medium text-primary truncate">{alert.display_name}</span>
                    <span className="text-sm font-bold text-red-400 ml-2">{alert.score}</span>
                  </div>
                  {alert.top_finding && (
                    <p className="text-xs text-muted">{alert.top_finding}</p>
                  )}
                </div>
              ))}
            </div>
          </div>
        )}
      </div>
    </div>
  )
}
