"use client";

import * as React from "react";
import { motion, useReducedMotion } from "motion/react";
import { Circle, CheckCircle2 } from "lucide-react";
import { cn } from "@/lib/utils";

export interface AnimatedChecklistItem {
  id: string;
  label: string;
  description?: string;
}

export interface AnimatedChecklistProps {
  items: AnimatedChecklistItem[];
  staggerDelay?: number;
  autoPlay?: boolean;
  /** When "card", each item renders in a card-like row with left accent */
  variant?: "default" | "card";
  /** Tailwind class for left border when variant="card" (e.g. border-l-blue-500) */
  accentBorderClass?: string;
}

export function AnimatedChecklist({
  items,
  staggerDelay = 200,
  autoPlay = true,
  variant = "default",
  accentBorderClass = "border-l-blue-500",
}: AnimatedChecklistProps) {
  const [checkedIndices, setCheckedIndices] = React.useState<Set<number>>(
    () => new Set()
  );
  const prefersReducedMotion = useReducedMotion();

  React.useEffect(() => {
    if (!autoPlay || prefersReducedMotion) {
      if (prefersReducedMotion) {
        setCheckedIndices(new Set(items.map((_, i) => i)));
      }
      return;
    }

    let current = 0;
    const timers: ReturnType<typeof setTimeout>[] = [];

    for (let i = 0; i < items.length; i++) {
      const timer = setTimeout(() => {
        setCheckedIndices((prev) => new Set([...prev, i]));
        current++;
      }, i * staggerDelay);
      timers.push(timer);
    }

    return () => timers.forEach((t) => clearTimeout(t));
  }, [items.length, staggerDelay, autoPlay, prefersReducedMotion]);

  const allChecked = prefersReducedMotion || checkedIndices.size === items.length;

  const itemContent = (item: AnimatedChecklistItem, index: number) => {
    const isChecked = checkedIndices.has(index) || allChecked;
    return (
      <>
        <span className="mt-0.5 shrink-0">
          {isChecked ? (
            <CheckCircle2 className="h-5 w-5 text-blue-600" />
          ) : (
            <Circle className="h-5 w-5 text-slate-300" strokeWidth={2} />
          )}
        </span>
        <div className="min-w-0 flex-1">
          <span
            className={cn(
              "font-medium text-slate-700",
              isChecked && "text-slate-900"
            )}
          >
            {item.label}
          </span>
          {item.description && (
            <p className="mt-0.5 text-sm text-muted">
              {item.description}
            </p>
          )}
        </div>
      </>
    );
  };

  return (
    <ul className="space-y-2.5">
      {items.map((item, index) => (
          <motion.li
            key={item.id}
            initial={prefersReducedMotion ? false : { opacity: 0, x: -8 }}
            animate={
              prefersReducedMotion
                ? false
                : { opacity: 1, x: 0 }
            }
            transition={{
              delay: prefersReducedMotion ? 0 : (index * staggerDelay * 0.5) / 1000,
              duration: prefersReducedMotion ? 0 : 0.2,
            }}
            className={cn(
              "flex items-start gap-3",
              variant === "card" &&
                "rounded-lg border border-slate-100 bg-slate-50/50 px-4 py-2.5 border-l-4",
              variant === "card" && accentBorderClass
            )}
          >
            {itemContent(item, index)}
          </motion.li>
        ))}
    </ul>
  );
}
