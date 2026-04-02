import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/** Returns a Tailwind text color class based on maturity tier. */
export function getScoreColor(maturity: string | null | undefined): string {
  if (maturity == null) return "text-muted"
  if (maturity === "Trusted") return "text-emerald-400"
  if (maturity === "Ready to Optimize") return "text-blue-400"
  return "text-red-400"  // Not Ready
}

/** Returns a hex color string based on maturity tier (for SVG strokes, etc.). */
export function getScoreHex(maturity: string | null | undefined): string {
  if (maturity == null) return "#6b7280"
  if (maturity === "Trusted") return "#10b981"
  if (maturity === "Ready to Optimize") return "#3b82f6"
  return "#ef4444"  // Not Ready
}

/** Format optimization accuracy for display. */
export function getOptimizationLabel(accuracy: number | null | undefined): string {
  if (accuracy != null) return `${Math.round(accuracy * 100)}% benchmark accuracy`
  return "Not yet optimized"
}

/** Returns Tailwind badge classes for optimization accuracy tiers. */
export function getAccuracyBadgeClass(accuracy: number | null | undefined): string {
  if (accuracy == null) return "border-default bg-elevated text-muted"
  if (accuracy >= 0.85) return "border-emerald-500/30 bg-emerald-500/20 text-emerald-400"
  if (accuracy >= 0.61) return "border-amber-500/30 bg-amber-500/20 text-amber-400"
  return "border-red-500/30 bg-red-500/20 text-red-400"
}

/** Maturity tier color definitions — single source of truth for all views. */
export const MATURITY_COLORS: Record<string, { hex: string; bg: string; border: string; badge: string; bar: string }> = {
  // 3-tier system
  "Not Ready":          { hex: "#ef4444", bg: "bg-red-500/10",     border: "border-red-500/30",     badge: "bg-red-500/20 text-red-400 border-red-500/30",         bar: "bg-red-500" },
  "Ready to Optimize":  { hex: "#3b82f6", bg: "bg-blue-500/10",    border: "border-blue-500/30",    badge: "bg-blue-500/20 text-blue-400 border-blue-500/30",       bar: "bg-blue-500" },
  Trusted:              { hex: "#10b981", bg: "bg-emerald-500/10", border: "border-emerald-500/30", badge: "bg-emerald-500/20 text-emerald-400 border-emerald-500/30", bar: "bg-emerald-500" },
  // Backward compat: old tier names map to closest new tier colors
  Connected:  { hex: "#ef4444", bg: "bg-red-500/10",     border: "border-red-500/30",     badge: "bg-red-500/20 text-red-400 border-red-500/30",         bar: "bg-red-500" },
  Configured: { hex: "#ef4444", bg: "bg-red-500/10",     border: "border-red-500/30",     badge: "bg-red-500/20 text-red-400 border-red-500/30",         bar: "bg-red-500" },
  Calibrated: { hex: "#3b82f6", bg: "bg-blue-500/10",    border: "border-blue-500/30",    badge: "bg-blue-500/20 text-blue-400 border-blue-500/30",       bar: "bg-blue-500" },
  Optimized:  { hex: "#10b981", bg: "bg-emerald-500/10", border: "border-emerald-500/30", badge: "bg-emerald-500/20 text-emerald-400 border-emerald-500/30", bar: "bg-emerald-500" },
}
