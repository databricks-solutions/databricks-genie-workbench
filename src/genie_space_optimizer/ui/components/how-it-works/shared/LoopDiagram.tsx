"use client";

import * as React from "react";
import { motion, useReducedMotion } from "motion/react";
import { ArrowRight, type LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

export interface LoopDiagramStep {
  label: string;
  icon?: LucideIcon;
}

export interface LoopDiagramProps {
  steps: LoopDiagramStep[];
  className?: string;
}

export function LoopDiagram({ steps, className }: LoopDiagramProps) {
  const prefersReducedMotion = useReducedMotion();
  const n = steps.length;

  if (n === 0) return null;

  const leftKeyframes =
    n === 1
      ? ["0%"]
      : [
          ...Array.from({ length: n }, (_, i) => `${(i / (n - 1)) * 100}%`),
          "0%",
        ];

  return (
    <div className={cn("relative w-full", className)}>
      <div className="relative flex items-center">
        {steps.map((step, index) => (
          <React.Fragment key={index}>
            <motion.div
              initial={{ opacity: 0, scale: 0.9 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: index * 0.1, duration: 0.2 }}
              className="relative flex shrink-0 flex-col items-center"
            >
              <div className="flex items-center justify-center rounded-lg border border-db-gray-border bg-white px-3 py-2">
                {step.icon ? (
                  <step.icon className="h-4 w-4 text-db-blue" />
                ) : (
                  <span className="text-xs font-medium">{step.label}</span>
                )}
              </div>
              {step.icon && (
                <span className="mt-1 text-center text-xs font-medium">
                  {step.label}
                </span>
              )}
            </motion.div>
            {index < n - 1 && (
              <div className="flex min-w-8 shrink-0 items-center justify-center">
                <div className="h-px w-4 bg-db-gray-border" aria-hidden />
                <ArrowRight
                  className="h-4 w-4 shrink-0 text-db-gray-border"
                  aria-hidden
                />
              </div>
            )}
          </React.Fragment>
        ))}
        <motion.div
          className="absolute left-0 top-1/2 flex h-4 -translate-y-1/2 items-center justify-center"
          animate={
            prefersReducedMotion
              ? false
              : {
                  left: leftKeyframes,
                }
          }
          transition={{
            duration: n * 0.8,
            repeat: Infinity,
            ease: "easeInOut",
          }}
        >
          <span className="h-2 w-2 rounded-full bg-db-blue" />
        </motion.div>
      </div>
      <div className="mt-3 flex items-center justify-center gap-1">
        <ArrowRight
          className="h-4 w-4 rotate-[-90deg] text-db-gray-border"
          aria-hidden
        />
        <motion.span
          className="text-xs text-muted"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.3 }}
        >
          Loop back to start
        </motion.span>
      </div>
    </div>
  );
}
