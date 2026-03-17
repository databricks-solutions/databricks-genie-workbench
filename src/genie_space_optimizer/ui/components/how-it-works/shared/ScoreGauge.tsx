"use client";

import { motion, useReducedMotion } from "motion/react";
import { cn } from "@/lib/utils";

export interface ScoreGaugeProps {
  value: number;
  label: string;
  threshold?: number;
  color?: string;
}

export function ScoreGauge({
  value,
  label,
  threshold,
  color = "bg-db-green",
}: ScoreGaugeProps) {
  const prefersReducedMotion = useReducedMotion();
  const clampedValue = Math.max(0, Math.min(100, value));
  const clampedThreshold = threshold != null ? Math.max(0, Math.min(100, threshold)) : undefined;

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-sm font-medium">{label}</span>
        <span className="text-sm font-semibold tabular-nums">{clampedValue}%</span>
      </div>
      <div className="relative h-3 overflow-hidden rounded-full bg-db-gray-bg">
        <motion.div
          className={cn("h-full rounded-full", color)}
          initial={{ width: "0%" }}
          animate={{ width: `${clampedValue}%` }}
          transition={{
            duration: prefersReducedMotion ? 0 : 0.6,
            ease: "easeOut",
          }}
        />
        {clampedThreshold != null && (
          <div
            className="absolute top-0 z-10 h-full w-0.5 bg-db-dark"
            style={{ left: `${clampedThreshold}%` }}
          />
        )}
      </div>
    </div>
  );
}
