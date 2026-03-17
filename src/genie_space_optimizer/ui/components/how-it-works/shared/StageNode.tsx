"use client";

import { motion } from "motion/react";
import { type LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

export interface StageNodeProps {
  icon: LucideIcon;
  title: string;
  summary: string;
  isActive?: boolean;
  onClick?: () => void;
}

export function StageNode({
  icon: Icon,
  title,
  summary,
  isActive = false,
  onClick,
}: StageNodeProps) {
  return (
    <motion.button
      type="button"
      onClick={onClick}
      whileHover={{ scale: 1.03 }}
      className={cn(
        "flex w-[150px] cursor-pointer flex-col items-center rounded-lg border p-4 transition-colors",
        isActive
          ? "border-db-blue bg-db-blue/10 shadow-sm"
          : "border-db-gray-border bg-white hover:border-db-blue/30 hover:bg-db-blue/5"
      )}
      style={{ minHeight: 0 }}
    >
      <div
        className={cn(
          "mb-2 flex h-10 w-10 shrink-0 items-center justify-center rounded-full",
          isActive ? "bg-db-blue text-white" : "bg-db-gray-bg text-muted"
        )}
      >
        <Icon className="h-5 w-5" />
      </div>
      <span
        className={cn(
          "text-center text-sm font-semibold",
          isActive ? "text-db-blue" : "text-primary"
        )}
      >
        {title}
      </span>
      <p className="mt-1 line-clamp-2 text-center text-xs text-muted">
        {summary}
      </p>
    </motion.button>
  );
}
