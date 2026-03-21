"use client";

import { motion, useReducedMotion } from "motion/react";
import { ArrowRight } from "lucide-react";
import { cn } from "@/lib/utils";

export interface GateFunnelProps {
  activeGate?: 0 | 1 | 2;
  className?: string;
}

const gates = [
  { name: "Slice", description: "Quick subset validation" },
  { name: "P0", description: "Core metrics check" },
  { name: "Full", description: "Complete evaluation" },
] as const;

export function GateFunnel({ activeGate, className }: GateFunnelProps) {
  const prefersReducedMotion = useReducedMotion();

  return (
    <div className={cn("w-full max-w-[400px]", className)}>
      <div className="flex items-stretch">
        {gates.map((gate, index) => (
          <div key={gate.name} className="flex flex-1 items-stretch">
            <motion.div
              initial={prefersReducedMotion ? false : { opacity: 0, x: -8 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{
                delay: prefersReducedMotion ? 0 : index * 0.2,
                duration: 0.3,
              }}
              className={cn(
                "flex flex-1 flex-col items-center justify-center rounded-lg border px-3 py-3 transition-colors",
                activeGate === index
                  ? "border-db-green bg-db-green/10 shadow-[0_0_12px_rgba(0,169,114,0.3)]"
                  : "border-db-gray-border bg-db-gray-bg"
              )}
            >
              <span
                className={cn(
                  "text-sm font-semibold",
                  activeGate === index ? "text-db-green" : "text-muted"
                )}
              >
                {gate.name} Gate
              </span>
              <p className="mt-0.5 text-center text-xs text-muted">
                {gate.description}
              </p>
            </motion.div>
            {index < gates.length - 1 && (
              <div className="flex shrink-0 items-center px-0.5">
                <ArrowRight className="h-4 w-4 text-db-gray-border" aria-hidden />
              </div>
            )}
          </div>
        ))}
      </div>
      <p className="mt-2 text-center text-xs text-muted">
        Fast fail: if any gate fails → rollback
      </p>
    </div>
  );
}
