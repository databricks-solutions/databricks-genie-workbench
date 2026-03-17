"use client";

import { motion, useReducedMotion } from "motion/react";
import { ArrowRight } from "lucide-react";
import { cn } from "@/lib/utils";

export interface BeforeAfterSplitProps {
  before: { title: string; items: string[] };
  after: { title: string; items: string[] };
  className?: string;
}

export function BeforeAfterSplit({
  before,
  after,
  className,
}: BeforeAfterSplitProps) {
  const prefersReducedMotion = useReducedMotion();

  return (
    <div
      className={cn(
        "grid grid-cols-2 gap-0 overflow-hidden rounded-lg border border-db-gray-border",
        className
      )}
    >
      <div className="flex flex-col border-r border-db-gray-border bg-db-gray-bg p-4">
        <h4 className="text-sm font-medium text-muted">{before.title}</h4>
        <ul className="mt-2 space-y-2">
          {before.items.map((item, i) => (
            <li
              key={i}
              className="whitespace-pre-line rounded border border-db-gray-border bg-white/50 px-2 py-1 text-xs text-muted"
            >
              {item}
            </li>
          ))}
        </ul>
      </div>
      <div className="relative flex flex-col p-4">
        <div className="absolute left-0 top-1/2 -translate-y-1/2 -translate-x-1/2 rounded-full border border-db-gray-border bg-white p-1">
          <ArrowRight className="h-3 w-3 text-db-green" aria-hidden />
        </div>
        <motion.div
          initial={
            prefersReducedMotion ? false : { opacity: 0, x: 12 }
          }
          animate={{ opacity: 1, x: 0 }}
          transition={{
            duration: 0.4,
            delay: prefersReducedMotion ? 0 : 0.15,
          }}
          className="flex flex-1 flex-col"
        >
          <h4 className="text-sm font-bold text-db-green">{after.title}</h4>
          <ul className="mt-2 space-y-2">
            {after.items.map((item, i) => (
              <li
                key={i}
                className="whitespace-pre-line rounded border border-db-green/30 bg-db-green/5 px-2 py-1 text-xs text-primary"
              >
                {item}
              </li>
            ))}
          </ul>
        </motion.div>
      </div>
    </div>
  );
}
