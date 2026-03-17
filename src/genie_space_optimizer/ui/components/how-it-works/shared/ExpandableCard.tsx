"use client";

import * as React from "react";
import { motion, AnimatePresence, useReducedMotion } from "motion/react";
import { ChevronDown, type LucideIcon } from "lucide-react";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";

export interface ExpandableCardProps {
  title: string;
  subtitle?: string;
  badge?: { text: string; variant?: "default" | "secondary" | "destructive" | "outline"; className?: string };
  icon?: LucideIcon;
  accentColor?: string;
  children: React.ReactNode;
  defaultOpen?: boolean;
}

export function ExpandableCard({
  title,
  subtitle,
  badge,
  icon: Icon,
  accentColor = "border-l-db-blue",
  children,
  defaultOpen = false,
}: ExpandableCardProps) {
  const [open, setOpen] = React.useState(defaultOpen);
  const prefersReducedMotion = useReducedMotion();

  return (
    <Card className={cn("overflow-hidden border-l-4", accentColor)}>
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        className="w-full text-left"
        aria-expanded={open}
      >
        <CardHeader className="flex flex-row items-center gap-3 py-4">
          {Icon && (
            <div className="flex shrink-0 items-center justify-center rounded-lg bg-db-gray-bg p-2">
              <Icon className="h-5 w-5 text-muted" />
            </div>
          )}
          <div className="min-w-0 flex-1">
            <div className="flex flex-wrap items-center gap-2">
              <span className="font-semibold">{title}</span>
              {badge && (
                <Badge variant={badge.variant ?? "secondary"} className={badge.className}>
                  {badge.text}
                </Badge>
              )}
            </div>
            {subtitle && (
              <p className="mt-0.5 text-sm text-muted">{subtitle}</p>
            )}
          </div>
          <motion.span
            animate={{ rotate: open ? 180 : 0 }}
            transition={{ duration: prefersReducedMotion ? 0 : 0.2 }}
            className="shrink-0"
          >
            <ChevronDown className="h-5 w-5 text-muted" />
          </motion.span>
        </CardHeader>
      </button>
      <AnimatePresence initial={false}>
        {open &&
          (prefersReducedMotion ? (
            <div className="overflow-hidden">
              <CardContent className="border-t border-db-gray-border pt-4">
                {children}
              </CardContent>
            </div>
          ) : (
            <motion.div
              initial={{ height: 0 }}
              animate={{ height: "auto" }}
              exit={{ height: 0 }}
              transition={{ duration: 0.25, ease: "easeInOut" }}
              className="overflow-hidden"
            >
              <CardContent className="border-t border-db-gray-border pt-4">
                {children}
              </CardContent>
            </motion.div>
          ))}
      </AnimatePresence>
    </Card>
  );
}
