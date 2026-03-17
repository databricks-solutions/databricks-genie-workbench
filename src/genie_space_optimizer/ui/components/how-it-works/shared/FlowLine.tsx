"use client";

import { motion, useReducedMotion } from "motion/react";
import { cn } from "@/lib/utils";

export interface FlowLineProps {
  className?: string;
}

export function FlowLine({ className }: FlowLineProps) {
  const prefersReducedMotion = useReducedMotion();

  return (
    <svg
      className={cn("h-5 w-full", className)}
      viewBox="0 0 100 20"
      preserveAspectRatio="none"
      aria-hidden
    >
      <defs>
        <linearGradient id="flowLineGradient" x1="0%" y1="0%" x2="100%" y2="0%">
          <stop offset="0%" stopColor="var(--color-db-blue)" />
          <stop offset="100%" stopColor="var(--color-db-green)" />
        </linearGradient>
      </defs>
      <motion.line
        x1="0"
        y1="10"
        x2="100"
        y2="10"
        stroke="url(#flowLineGradient)"
        strokeWidth="2"
        strokeLinecap="round"
        strokeDasharray="8 4"
        initial={{ strokeDashoffset: 0 }}
        animate={
          prefersReducedMotion
            ? false
            : {
                strokeDashoffset: [0, -30],
              }
        }
        transition={{
          duration: 2,
          repeat: Infinity,
          ease: "linear",
        }}
      />
    </svg>
  );
}
