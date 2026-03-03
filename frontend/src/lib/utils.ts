import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/** Returns a Tailwind text color class for a 0-100 IQ score. */
export function getScoreColor(score: number | null | undefined): string {
  if (score == null) return "text-muted"
  if (score >= 85) return "text-emerald-400"
  if (score >= 70) return "text-blue-400"
  if (score >= 50) return "text-yellow-400"
  if (score >= 30) return "text-orange-400"
  return "text-red-400"
}

/** Returns a hex color string for a 0-100 IQ score (for SVG strokes, etc.). */
export function getScoreHex(score: number | null | undefined): string {
  if (score == null) return "#6b7280"
  if (score >= 85) return "#10b981"
  if (score >= 70) return "#3b82f6"
  if (score >= 50) return "#eab308"
  if (score >= 30) return "#f97316"
  return "#ef4444"
}

