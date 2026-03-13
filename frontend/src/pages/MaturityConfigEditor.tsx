/**
 * MaturityConfigEditor - Admin UI for configuring maturity scoring.
 *
 * Lets admins adjust point weights, enable/disable criteria, modify stage
 * thresholds, and reset to defaults. Only sends changed fields as overrides.
 */
import { useState, useEffect, useMemo } from "react"
import { Save, RotateCcw, AlertTriangle, Check, ChevronDown, ChevronRight } from "lucide-react"
import { getMaturityConfig, updateMaturityConfig, resetMaturityConfig } from "@/lib/api"
import type { MaturityConfig, MaturityConfigCriterion, MaturityConfigStage } from "@/types"

const STAGE_COLORS: Record<string, string> = {
  Nascent: "border-red-500/30 bg-red-500/5",
  Basic: "border-orange-500/30 bg-orange-500/5",
  Developing: "border-yellow-500/30 bg-yellow-500/5",
  Proficient: "border-blue-500/30 bg-blue-500/5",
  Optimized: "border-emerald-500/30 bg-emerald-500/5",
}

const STAGE_DOT_COLORS: Record<string, string> = {
  Nascent: "bg-red-500",
  Basic: "bg-orange-500",
  Developing: "bg-yellow-500",
  Proficient: "bg-blue-500",
  Optimized: "bg-emerald-500",
}

export function MaturityConfigEditor() {
  const [activeConfig, setActiveConfig] = useState<MaturityConfig | null>(null)
  const [defaultConfig, setDefaultConfig] = useState<MaturityConfig | null>(null)
  const [editedStages, setEditedStages] = useState<MaturityConfigStage[]>([])
  const [editedCriteria, setEditedCriteria] = useState<MaturityConfigCriterion[]>([])
  const [loading, setLoading] = useState(true)
  const [saving, setSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)
  const [expandedStages, setExpandedStages] = useState<Set<string>>(new Set())

  const loadConfig = async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await getMaturityConfig()
      setActiveConfig(data.active)
      setDefaultConfig(data.default)
      setEditedStages(structuredClone(data.active.stages))
      setEditedCriteria(structuredClone(data.active.criteria))
      // Expand all stages initially
      setExpandedStages(new Set(data.active.stages.map(s => s.name)))
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to load config")
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { loadConfig() }, [])

  // Clear success message after 3s
  useEffect(() => {
    if (success) {
      const t = setTimeout(() => setSuccess(null), 3000)
      return () => clearTimeout(t)
    }
  }, [success])

  // Group criteria by stage
  const criteriaByStage = useMemo(() => {
    const grouped: Record<string, MaturityConfigCriterion[]> = {}
    for (const c of editedCriteria) {
      if (!grouped[c.stage]) grouped[c.stage] = []
      grouped[c.stage].push(c)
    }
    return grouped
  }, [editedCriteria])

  // Detect if anything has changed from active config
  const hasChanges = useMemo(() => {
    if (!activeConfig) return false
    return (
      JSON.stringify(editedStages) !== JSON.stringify(activeConfig.stages) ||
      JSON.stringify(editedCriteria) !== JSON.stringify(activeConfig.criteria)
    )
  }, [editedStages, editedCriteria, activeConfig])

  // Build minimal overrides (only fields that differ from default)
  const buildOverrides = (): Record<string, unknown> => {
    if (!defaultConfig) return {}
    const overrides: Record<string, unknown> = {}

    // Check if stages differ from default
    if (JSON.stringify(editedStages) !== JSON.stringify(defaultConfig.stages)) {
      overrides.stages = editedStages
    }

    // Build criteria overrides (only changed fields per criterion)
    const criteriaOverrides: Record<string, unknown>[] = []
    for (const edited of editedCriteria) {
      const def = defaultConfig.criteria.find(c => c.id === edited.id)
      if (!def) continue
      const diff: Record<string, unknown> = { id: edited.id }
      let changed = false
      if (edited.points !== def.points) { diff.points = edited.points; changed = true }
      if (edited.enabled !== def.enabled) { diff.enabled = edited.enabled; changed = true }
      if (changed) criteriaOverrides.push(diff)
    }
    if (criteriaOverrides.length > 0) {
      overrides.criteria = criteriaOverrides
    }

    return overrides
  }

  const handleSave = async () => {
    setSaving(true)
    setError(null)
    setSuccess(null)
    try {
      const overrides = buildOverrides()
      const result = await updateMaturityConfig(overrides)
      setActiveConfig(result.active)
      setEditedStages(structuredClone(result.active.stages))
      setEditedCriteria(structuredClone(result.active.criteria))
      setSuccess("Configuration saved")
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to save")
    } finally {
      setSaving(false)
    }
  }

  const handleReset = async () => {
    if (!confirm("Reset all maturity scoring to defaults? This removes all admin overrides.")) return
    setSaving(true)
    setError(null)
    setSuccess(null)
    try {
      const result = await resetMaturityConfig()
      setActiveConfig(result.active)
      setEditedStages(structuredClone(result.active.stages))
      setEditedCriteria(structuredClone(result.active.criteria))
      setSuccess("Reset to defaults")
    } catch (e) {
      setError(e instanceof Error ? e.message : "Failed to reset")
    } finally {
      setSaving(false)
    }
  }

  const updateCriterion = (id: string, field: string, value: unknown) => {
    setEditedCriteria(prev =>
      prev.map(c => c.id === id ? { ...c, [field]: value } : c)
    )
  }

  const updateStageRange = (index: number, bound: 0 | 1, value: number) => {
    setEditedStages(prev => {
      const next = structuredClone(prev)
      next[index].range[bound] = value
      return next
    })
  }

  const toggleStageExpanded = (name: string) => {
    setExpandedStages(prev => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  // Check if a criterion differs from default
  const isModified = (criterion: MaturityConfigCriterion): boolean => {
    if (!defaultConfig) return false
    const def = defaultConfig.criteria.find(c => c.id === criterion.id)
    if (!def) return false
    return criterion.points !== def.points || criterion.enabled !== def.enabled
  }

  // Calculate total possible points
  const totalPoints = useMemo(() =>
    editedCriteria.filter(c => c.enabled).reduce((sum, c) => sum + c.points, 0),
    [editedCriteria]
  )

  if (loading) {
    return (
      <div className="space-y-4">
        {Array.from({ length: 3 }).map((_, i) => (
          <div key={i} className="bg-surface border border-default rounded-xl p-4 animate-pulse h-20" />
        ))}
      </div>
    )
  }

  if (error && !activeConfig) {
    return (
      <div className="flex items-center gap-3 p-4 bg-red-500/10 border border-red-500/20 rounded-xl text-red-400">
        <AlertTriangle className="w-5 h-5" />
        <span>{error}</span>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h3 className="text-lg font-semibold text-primary">Maturity Scoring Configuration</h3>
          <p className="text-sm text-muted mt-1">
            Adjust stage thresholds, point weights, and enable/disable criteria.
            Total points: <span className={`font-medium ${totalPoints === 100 ? "text-emerald-400" : "text-amber-400"}`}>{totalPoints}</span>/100
          </p>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={handleReset}
            disabled={saving}
            className="flex items-center gap-1.5 px-3 py-2 rounded-lg border border-default bg-surface hover:bg-surface-secondary text-sm text-muted hover:text-secondary transition-colors disabled:opacity-50"
          >
            <RotateCcw className="w-3.5 h-3.5" />
            Reset Defaults
          </button>
          <button
            onClick={handleSave}
            disabled={saving || !hasChanges}
            className="flex items-center gap-1.5 px-3 py-2 rounded-lg bg-accent text-white text-sm font-medium hover:bg-accent/90 transition-colors disabled:opacity-50"
          >
            {saving ? (
              <RotateCcw className="w-3.5 h-3.5 animate-spin" />
            ) : (
              <Save className="w-3.5 h-3.5" />
            )}
            {saving ? "Saving..." : "Save Changes"}
          </button>
        </div>
      </div>

      {/* Status messages */}
      {error && (
        <div className="flex items-center gap-2 p-3 bg-red-500/10 border border-red-500/20 rounded-lg text-red-400 text-sm">
          <AlertTriangle className="w-4 h-4 flex-shrink-0" />
          {error}
        </div>
      )}
      {success && (
        <div className="flex items-center gap-2 p-3 bg-emerald-500/10 border border-emerald-500/20 rounded-lg text-emerald-400 text-sm">
          <Check className="w-4 h-4 flex-shrink-0" />
          {success}
        </div>
      )}

      {/* Stage thresholds */}
      <div className="bg-surface border border-default rounded-xl p-5">
        <h4 className="text-sm font-semibold text-secondary uppercase tracking-wide mb-4">Stage Thresholds</h4>
        <div className="grid grid-cols-5 gap-3">
          {editedStages.map((stage, i) => (
            <div key={stage.name} className={`rounded-lg border p-3 ${STAGE_COLORS[stage.name] || "border-default"}`}>
              <div className="flex items-center gap-2 mb-2">
                <div className={`w-2 h-2 rounded-full ${STAGE_DOT_COLORS[stage.name] || "bg-gray-500"}`} />
                <span className="text-xs font-semibold text-secondary">{stage.name}</span>
              </div>
              <div className="flex items-center gap-1">
                <input
                  type="number"
                  min={0}
                  max={100}
                  value={stage.range[0]}
                  onChange={e => updateStageRange(i, 0, parseInt(e.target.value) || 0)}
                  className="w-14 px-1.5 py-1 rounded border border-default bg-background text-primary text-xs text-center focus:outline-none focus:ring-1 focus:ring-accent/50"
                />
                <span className="text-xs text-muted">–</span>
                <input
                  type="number"
                  min={0}
                  max={100}
                  value={stage.range[1]}
                  onChange={e => updateStageRange(i, 1, parseInt(e.target.value) || 0)}
                  className="w-14 px-1.5 py-1 rounded border border-default bg-background text-primary text-xs text-center focus:outline-none focus:ring-1 focus:ring-accent/50"
                />
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* Criteria by stage */}
      {editedStages.map(stage => {
        const criteria = criteriaByStage[stage.name] || []
        const isExpanded = expandedStages.has(stage.name)
        const stagePoints = criteria.filter(c => c.enabled).reduce((s, c) => s + c.points, 0)

        return (
          <div key={stage.name} className={`border rounded-xl overflow-hidden ${STAGE_COLORS[stage.name] || "border-default"}`}>
            {/* Stage header */}
            <button
              onClick={() => toggleStageExpanded(stage.name)}
              className="w-full flex items-center justify-between p-4 hover:bg-surface-secondary/30 transition-colors"
            >
              <div className="flex items-center gap-3">
                {isExpanded ? <ChevronDown className="w-4 h-4 text-muted" /> : <ChevronRight className="w-4 h-4 text-muted" />}
                <div className={`w-2.5 h-2.5 rounded-full ${STAGE_DOT_COLORS[stage.name] || "bg-gray-500"}`} />
                <span className="font-semibold text-primary">{stage.name}</span>
                <span className="text-xs text-muted">({stage.range[0]}–{stage.range[1]})</span>
              </div>
              <span className="text-sm text-muted">{stagePoints} pts</span>
            </button>

            {/* Criteria table */}
            {isExpanded && criteria.length > 0 && (
              <div className="border-t border-default">
                <table className="w-full">
                  <thead>
                    <tr className="text-xs text-muted uppercase tracking-wide">
                      <th className="text-left pl-12 pr-3 py-2 font-medium">Criterion</th>
                      <th className="text-left px-3 py-2 font-medium w-20">Type</th>
                      <th className="text-left px-3 py-2 font-medium w-24">Points</th>
                      <th className="text-center px-3 py-2 font-medium w-20">Enabled</th>
                      <th className="text-right pr-4 py-2 font-medium w-20"></th>
                    </tr>
                  </thead>
                  <tbody>
                    {criteria.map(c => {
                      const modified = isModified(c)
                      return (
                        <tr
                          key={c.id}
                          className={`border-t border-default/50 ${modified ? "bg-accent/5" : ""}`}
                        >
                          <td className="pl-12 pr-3 py-3">
                            <div className="text-sm text-primary">{c.description}</div>
                            <div className="text-xs text-muted font-mono mt-0.5">{c.id}</div>
                          </td>
                          <td className="px-3 py-3">
                            <span className={`text-xs px-1.5 py-0.5 rounded ${c.type === "boolean" ? "bg-purple-500/20 text-purple-400" : "bg-cyan-500/20 text-cyan-400"}`}>
                              {c.type}
                            </span>
                          </td>
                          <td className="px-3 py-3">
                            <input
                              type="number"
                              min={0}
                              max={50}
                              value={c.points}
                              onChange={e => updateCriterion(c.id, "points", parseInt(e.target.value) || 0)}
                              className="w-16 px-2 py-1 rounded border border-default bg-background text-primary text-sm text-center focus:outline-none focus:ring-1 focus:ring-accent/50"
                            />
                          </td>
                          <td className="px-3 py-3 text-center">
                            <button
                              onClick={() => updateCriterion(c.id, "enabled", !c.enabled)}
                              className={`w-8 h-5 rounded-full transition-colors relative ${c.enabled ? "bg-emerald-500" : "bg-surface-secondary"}`}
                            >
                              <span className={`absolute top-0.5 w-4 h-4 rounded-full bg-white shadow transition-transform ${c.enabled ? "left-3.5" : "left-0.5"}`} />
                            </button>
                          </td>
                          <td className="pr-4 py-3 text-right">
                            {modified && (
                              <span className="text-[10px] text-accent font-medium uppercase">Modified</span>
                            )}
                          </td>
                        </tr>
                      )
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )
      })}
    </div>
  )
}
