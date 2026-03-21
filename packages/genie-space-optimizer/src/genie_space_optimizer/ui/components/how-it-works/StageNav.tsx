"use client";

import { useState, useEffect } from "react";
import { motion, useReducedMotion } from "motion/react";
import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";
import { PIPELINE_GROUP_COLORS } from "./data";
import type { WalkthroughStage } from "./types";

interface StageNavProps {
  stages: WalkthroughStage[];
  currentIndex: number;
  onNext: () => void;
  onPrev: () => void;
}

export function StageNav({
  stages,
  currentIndex,
  onNext,
  onPrev,
}: StageNavProps) {
  const isFirst = currentIndex === 0;
  const isLast = currentIndex === stages.length - 1;
  const nextStage = !isLast ? stages[currentIndex + 1] : null;
  const prevStage = !isFirst ? stages[currentIndex - 1] : null;
  const currentStage = stages[currentIndex];
  const colors =
    PIPELINE_GROUP_COLORS[currentStage?.pipelineGroup ?? "neutral"] ??
    PIPELINE_GROUP_COLORS.neutral;
  const prefersReducedMotion = useReducedMotion();

  const [showPulse, setShowPulse] = useState(false);
  useEffect(() => {
    if (currentIndex !== 0) {
      setShowPulse(false);
      return;
    }
    const timer = setTimeout(() => setShowPulse(true), 3000);
    return () => clearTimeout(timer);
  }, [currentIndex]);

  return (
    <div className="flex items-center justify-between border-t border-slate-100 bg-white/80 px-4 py-3 backdrop-blur-sm lg:px-8">
      {/* Back */}
      <div className="w-1/3">
        {prevStage && (
          <Button
            variant="ghost"
            size="sm"
            onClick={onPrev}
            className="gap-1.5 text-slate-500 hover:text-slate-800"
          >
            <ChevronLeft className="h-4 w-4" />
            <span className="hidden sm:inline">{prevStage.title}</span>
            <span className="sm:hidden">Back</span>
          </Button>
        )}
      </div>

      {/* Counter with progress dots */}
      <div className="flex flex-col items-center gap-1">
        <span className="text-[11px] font-medium text-slate-400">
          {currentIndex + 1} / {stages.length}
        </span>
        <div className="flex gap-0.5">
          {stages.map((s, i) => (
            <span
              key={s.id}
              className={cn(
                "block h-0.5 w-2 rounded-full transition-all",
                i === currentIndex
                  ? colors.dot
                  : i < currentIndex
                    ? "bg-slate-300"
                    : "bg-slate-200",
              )}
            />
          ))}
        </div>
      </div>

      {/* Next */}
      <div className="flex w-1/3 justify-end">
        {nextStage && (
          <motion.div
            animate={
              showPulse && !prefersReducedMotion
                ? { scale: [1, 1.04, 1] }
                : {}
            }
            transition={
              showPulse
                ? { duration: 1.5, repeat: Infinity, ease: "easeInOut" }
                : {}
            }
          >
            <Button
              size="sm"
              onClick={onNext}
              className="gap-1.5 bg-slate-900 text-white shadow-sm hover:bg-slate-800"
            >
              <span className="hidden sm:inline">
                Next: {nextStage.title}
              </span>
              <span className="sm:hidden">Next</span>
              <ChevronRight className="h-4 w-4" />
            </Button>
          </motion.div>
        )}
      </div>
    </div>
  );
}
