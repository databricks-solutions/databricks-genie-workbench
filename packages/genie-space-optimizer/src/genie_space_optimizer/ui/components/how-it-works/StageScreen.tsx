"use client";

import type { ReactNode } from "react";
import { cn } from "@/lib/utils";
import { AccordionItem } from "@/components/ui/accordion";
import { PIPELINE_GROUP_COLORS } from "./data";

interface LearnMoreItem {
  id: string;
  title: string;
  content: ReactNode;
}

export interface StageScreenProps {
  title: string;
  subtitle: string;
  pipelineGroup: string;
  visual: ReactNode;
  explanation: ReactNode;
  learnMore?: LearnMoreItem[];
  children?: ReactNode;
}

export function StageScreen({
  title,
  subtitle,
  pipelineGroup,
  visual,
  explanation,
  learnMore,
  children,
}: StageScreenProps) {
  const colors =
    PIPELINE_GROUP_COLORS[pipelineGroup] ?? PIPELINE_GROUP_COLORS.neutral;

  return (
    <div className="space-y-8">
      {/* Header with colored accent */}
      <div className={cn("rounded-xl border-l-4 py-5 pl-5 pr-4", colors.border, colors.bg)}>
        <h2 className={cn("text-2xl font-bold tracking-tight", colors.accent)}>
          {title}
        </h2>
        <p className="mt-1.5 text-sm text-muted">{subtitle}</p>
      </div>

      {/* Primary visual (hero) */}
      <div className="rounded-xl border border-default bg-surface p-6 shadow-sm">
        {visual}
      </div>

      {/* Explanation */}
      <div className="max-w-prose text-[15px] leading-relaxed text-secondary">
        {explanation}
      </div>

      {children}

      {/* Learn more accordions */}
      {learnMore && learnMore.length > 0 && (
        <div className="space-y-3 pt-2">
          <div className="flex items-center gap-2">
            <div className="h-px flex-1 bg-[var(--border-color)]" />
            <p className="shrink-0 text-[11px] font-semibold uppercase tracking-widest text-muted">
              Deep Dive
            </p>
            <div className="h-px flex-1 bg-[var(--border-color)]" />
          </div>
          <div className="space-y-2">
            {learnMore.map((item) => (
              <AccordionItem
                key={item.id}
                title={item.title}
              >
                <div className="text-sm leading-relaxed text-secondary">
                  {item.content}
                </div>
              </AccordionItem>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
