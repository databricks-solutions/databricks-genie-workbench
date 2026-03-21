"use client";

import { cn } from "@/lib/utils";
import { PIPELINE_GROUP_COLORS } from "./data";

const PIPELINE_GROUPS = [
  { id: "preflight", label: "Preflight" },
  { id: "baseline", label: "Baseline" },
  { id: "enrichment", label: "Enrichment" },
  { id: "leverLoop", label: "Lever Loop" },
  { id: "finalize", label: "Finalize" },
] as const;

interface PipelineMiniMapProps {
  currentGroup: string;
}

export function PipelineMiniMap({ currentGroup }: PipelineMiniMapProps) {
  return (
    <div className="flex items-center justify-center gap-0 border-b border-slate-100 bg-white/80 px-4 py-2.5 backdrop-blur-sm">
      {PIPELINE_GROUPS.map((group, i) => {
        const colors = PIPELINE_GROUP_COLORS[group.id];
        const isActive = currentGroup === group.id;
        const isPast =
          PIPELINE_GROUPS.findIndex((g) => g.id === currentGroup) >
          PIPELINE_GROUPS.findIndex((g) => g.id === group.id);

        return (
          <div key={group.id} className="flex items-center">
            <div
              className={cn(
                "flex items-center gap-1.5 rounded-full px-2.5 py-1 transition-all",
                isActive && `${colors.bg} ring-1 ${colors.ring}`,
              )}
            >
              <span
                className={cn(
                  "inline-block h-2 w-2 rounded-full transition-all",
                  isActive
                    ? `${colors.dot} ring-2 ring-offset-1 ring-current`
                    : isPast
                      ? colors.dot
                      : colors.fadedDot,
                )}
              />
              <span
                className={cn(
                  "hidden text-[11px] font-medium sm:inline",
                  isActive
                    ? colors.accent
                    : isPast
                      ? "text-slate-500"
                      : "text-slate-400",
                )}
              >
                {group.label}
              </span>
            </div>
            {i < PIPELINE_GROUPS.length - 1 && (
              <div
                className={cn(
                  "mx-1 h-px w-6 sm:w-8",
                  isPast ? "bg-slate-300" : "bg-slate-200",
                )}
              />
            )}
          </div>
        );
      })}
    </div>
  );
}
