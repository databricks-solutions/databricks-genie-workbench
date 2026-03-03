/**
 * Radial score gauge component for displaying compliance scores.
 * Features animated SVG ring and counting number animation.
 */

import { useEffect, useState, useRef } from "react"
import { cn } from "@/lib/utils"

interface ScoreGaugeProps {
  /** Number of items passed */
  passed: number
  /** Total number of items */
  total: number
  /** Size of the gauge in pixels */
  size?: number
  /** Stroke width of the progress ring */
  strokeWidth?: number
  /** Animation duration in ms */
  animationDuration?: number
  className?: string
}

function getScoreColor(percentage: number): {
  gradient: string
  glow: string
  text: string
} {
  if (percentage >= 80) {
    return {
      gradient: "url(#scoreGradientSuccess)",
      glow: "rgba(16, 185, 129, 0.4)",
      text: "text-success",
    }
  }
  if (percentage >= 60) {
    return {
      gradient: "url(#scoreGradientWarning)",
      glow: "rgba(245, 158, 11, 0.4)",
      text: "text-warning",
    }
  }
  return {
    gradient: "url(#scoreGradientDanger)",
    glow: "rgba(239, 68, 68, 0.4)",
    text: "text-danger",
  }
}

export function ScoreGauge({
  passed,
  total,
  size = 200,
  strokeWidth = 12,
  animationDuration = 1000,
  className,
}: ScoreGaugeProps) {
  const [animatedPassed, setAnimatedPassed] = useState(0)
  const [animatedPercentage, setAnimatedPercentage] = useState(0)
  const [isAnimating, setIsAnimating] = useState(true)
  const animationRef = useRef<number | undefined>(undefined)

  const percentage = total > 0 ? (passed / total) * 100 : 0
  const radius = (size - strokeWidth) / 2
  const circumference = 2 * Math.PI * radius
  const center = size / 2

  // Animate on mount
  useEffect(() => {
    const startTime = performance.now()

    const animate = (currentTime: number) => {
      const elapsed = currentTime - startTime
      const progress = Math.min(elapsed / animationDuration, 1)

      // Easing function (ease-out cubic)
      const eased = 1 - Math.pow(1 - progress, 3)

      setAnimatedPassed(Math.round(eased * passed))
      setAnimatedPercentage(eased * percentage)

      if (progress < 1) {
        animationRef.current = requestAnimationFrame(animate)
      } else {
        setIsAnimating(false)
      }
    }

    animationRef.current = requestAnimationFrame(animate)

    return () => {
      if (animationRef.current) {
        cancelAnimationFrame(animationRef.current)
      }
    }
  }, [passed, percentage, animationDuration])

  const strokeDashoffset = circumference - (animatedPercentage / 100) * circumference
  const { gradient, glow, text } = getScoreColor(percentage)

  return (
    <div
      className={cn("relative inline-flex items-center justify-center", className)}
      style={{ width: size, height: size }}
    >
      <svg
        width={size}
        height={size}
        viewBox={`0 0 ${size} ${size}`}
        className="transform -rotate-90"
      >
        {/* Gradient definitions */}
        <defs>
          <linearGradient id="scoreGradientSuccess" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#10B981" />
            <stop offset="100%" stopColor="#34D399" />
          </linearGradient>
          <linearGradient id="scoreGradientWarning" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#F59E0B" />
            <stop offset="100%" stopColor="#FBBF24" />
          </linearGradient>
          <linearGradient id="scoreGradientDanger" x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor="#EF4444" />
            <stop offset="100%" stopColor="#F87171" />
          </linearGradient>
          {/* Glow filter */}
          <filter id="scoreGlow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="4" result="coloredBlur" />
            <feMerge>
              <feMergeNode in="coloredBlur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
        </defs>

        {/* Background track */}
        <circle
          cx={center}
          cy={center}
          r={radius}
          fill="none"
          stroke="var(--bg-elevated)"
          strokeWidth={strokeWidth}
          className="opacity-50"
        />

        {/* Progress ring */}
        <circle
          cx={center}
          cy={center}
          r={radius}
          fill="none"
          stroke={gradient}
          strokeWidth={strokeWidth}
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={strokeDashoffset}
          style={{
            transition: isAnimating ? "none" : "stroke-dashoffset 0.5s ease-out",
            filter: "url(#scoreGlow)",
          }}
        />
      </svg>

      {/* Center content */}
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <div
          className={cn(
            "font-display font-extrabold tabular-nums tracking-tight",
            text
          )}
          style={{ fontSize: size * 0.22 }}
        >
          {animatedPassed}
          <span className="text-muted" style={{ fontSize: size * 0.12 }}>
            /{total}
          </span>
        </div>
        <div
          className="text-muted uppercase tracking-widest font-medium"
          style={{ fontSize: size * 0.055 }}
        >
          Checks Passed
        </div>
      </div>

      {/* Decorative glow ring (dark mode) */}
      <div
        className="absolute inset-0 rounded-full opacity-0 dark:opacity-100 transition-opacity pointer-events-none"
        style={{
          boxShadow: `0 0 40px ${glow}, inset 0 0 20px ${glow}`,
        }}
      />
    </div>
  )
}
