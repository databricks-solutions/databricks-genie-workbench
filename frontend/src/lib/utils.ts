import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/** Returns a Tailwind text color class for a 0-100 IQ score. */
export function getScoreColor(score: number | null | undefined): string {
  if (score == null) return "text-muted"
  if (score >= 81) return "text-emerald-400"   // Optimized
  if (score >= 61) return "text-blue-400"      // Trusted
  if (score >= 41) return "text-yellow-400"    // Calibrated
  if (score >= 21) return "text-orange-400"    // Configured
  return "text-red-400"                         // Connected
}

/** Returns a hex color string for a 0-100 IQ score (for SVG strokes, etc.). */
export function getScoreHex(score: number | null | undefined): string {
  if (score == null) return "#6b7280"
  if (score >= 81) return "#10b981"
  if (score >= 61) return "#3b82f6"
  if (score >= 41) return "#eab308"
  if (score >= 21) return "#f97316"
  return "#ef4444"
}

/** Maturity tier color definitions — single source of truth for all views. */
export const MATURITY_COLORS: Record<string, { hex: string; bg: string; border: string; badge: string; bar: string }> = {
  Optimized:  { hex: "#10b981", bg: "bg-emerald-500/10", border: "border-emerald-500/30", badge: "bg-emerald-500/20 text-emerald-400 border-emerald-500/30", bar: "bg-emerald-500" },
  Trusted:    { hex: "#3b82f6", bg: "bg-blue-500/10",    border: "border-blue-500/30",    badge: "bg-blue-500/20 text-blue-400 border-blue-500/30",       bar: "bg-blue-500" },
  Calibrated: { hex: "#eab308", bg: "bg-yellow-500/10",  border: "border-yellow-500/30",  badge: "bg-yellow-500/20 text-yellow-400 border-yellow-500/30", bar: "bg-yellow-500" },
  Configured: { hex: "#f97316", bg: "bg-orange-500/10",  border: "border-orange-500/30",  badge: "bg-orange-500/20 text-orange-400 border-orange-500/30", bar: "bg-orange-500" },
  Connected:  { hex: "#ef4444", bg: "bg-red-500/10",     border: "border-red-500/30",     badge: "bg-red-500/20 text-red-400 border-red-500/30",         bar: "bg-red-500" },
}
