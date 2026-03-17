"use client";

import { motion } from "motion/react";
import { cn } from "@/lib/utils";
import { PIPELINE_GROUP_COLORS } from "./data";
import type { WalkthroughStage } from "./types";
import {
  Tooltip,
  TooltipTrigger,
  TooltipContent,
  TooltipProvider,
} from "@/components/ui/tooltip";

interface ProgressRailProps {
  stages: WalkthroughStage[];
  currentIndex: number;
  onNavigate: (index: number) => void;
}

export function ProgressRail({
  stages,
  currentIndex,
  onNavigate,
}: ProgressRailProps) {
  let lastGroup = "";

  return (
    <TooltipProvider delayDuration={150}>
      <nav aria-label="Walkthrough progress" className="flex flex-col">
        {stages.map((stage, i) => {
          const isCompleted = i < currentIndex;
          const isCurrent = i === currentIndex;
          const colors =
            PIPELINE_GROUP_COLORS[stage.pipelineGroup] ??
            PIPELINE_GROUP_COLORS.neutral;

          const showGroupLabel =
            stage.pipelineGroup !== "neutral" &&
            stage.pipelineGroup !== lastGroup;
          lastGroup = stage.pipelineGroup;

          return (
            <div key={stage.id}>
              {/* Group separator label */}
              {showGroupLabel && i > 0 && (
                <div className="pb-1 pl-8 pt-3">
                  <span
                    className={cn(
                      "text-[10px] font-bold uppercase tracking-widest",
                      colors.accent,
                    )}
                  >
                    {stage.pipelineGroup === "leverLoop"
                      ? "Lever Loop"
                      : stage.pipelineGroup.charAt(0).toUpperCase() +
                        stage.pipelineGroup.slice(1)}
                  </span>
                </div>
              )}

              <div className="flex items-start gap-3">
                {/* Dot + connector */}
                <div className="flex flex-col items-center">
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <button
                        type="button"
                        onClick={() => onNavigate(i)}
                        className={cn(
                          "relative z-10 flex h-5 w-5 shrink-0 items-center justify-center rounded-full transition-all",
                          isCurrent &&
                            `${colors.dot} text-white shadow-sm ring-2 ring-offset-2 ${colors.ring}`,
                          isCompleted &&
                            `${colors.dot} text-white opacity-80`,
                          !isCurrent &&
                            !isCompleted &&
                            `${colors.fadedDot} ring-1 ring-inset ring-slate-200`,
                        )}
                        aria-current={isCurrent ? "step" : undefined}
                      >
                        {isCurrent && (
                          <motion.span
                            className="absolute inset-0 rounded-full bg-current opacity-20"
                            animate={{ scale: [1, 1.6, 1] }}
                            transition={{ duration: 2, repeat: Infinity }}
                          />
                        )}
                        {isCompleted && (
                          <svg
                            className="h-2.5 w-2.5"
                            viewBox="0 0 12 12"
                            fill="none"
                          >
                            <path
                              d="M2 6l3 3 5-5"
                              stroke="currentColor"
                              strokeWidth="2.5"
                              strokeLinecap="round"
                              strokeLinejoin="round"
                            />
                          </svg>
                        )}
                      </button>
                    </TooltipTrigger>
                    <TooltipContent
                      side="right"
                      className="text-xs font-medium"
                    >
                      {stage.title}
                    </TooltipContent>
                  </Tooltip>
                  {/* Connector line */}
                  {i < stages.length - 1 && (
                    <div
                      className={cn(
                        "w-0.5",
                        i < currentIndex ? colors.dot : "bg-slate-200",
                        stages[i + 1]?.pipelineGroup !== stage.pipelineGroup
                          ? "min-h-[1.5rem]"
                          : "min-h-[1rem]",
                      )}
                      style={{ opacity: i < currentIndex ? 0.4 : 1 }}
                    />
                  )}
                </div>

                {/* Label */}
                <button
                  type="button"
                  onClick={() => onNavigate(i)}
                  className={cn(
                    "pt-0.5 text-left text-[12px] leading-tight transition-colors",
                    isCurrent
                      ? `font-bold ${colors.accent}`
                      : isCompleted
                        ? "font-medium text-slate-500"
                        : "text-slate-400 hover:text-slate-600",
                  )}
                >
                  {stage.title}
                </button>
              </div>
            </div>
          );
        })}
      </nav>
    </TooltipProvider>
  );
}
