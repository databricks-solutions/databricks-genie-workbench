"use client";

import { useState, useEffect, useCallback, type ReactNode } from "react";
import { AnimatePresence, motion, useReducedMotion } from "motion/react";
import { ProgressRail } from "./ProgressRail";
import { PipelineMiniMap } from "./PipelineMiniMap";
import { StageNav } from "./StageNav";
import { WALKTHROUGH_STAGES } from "./data";

export interface WalkthroughShellProps {
  children: (stageId: string, stageIndex: number) => ReactNode;
}

export function WalkthroughShell({ children }: WalkthroughShellProps) {
  const [currentStage, setCurrentStage] = useState(0);
  const [direction, setDirection] = useState(0);
  const prefersReducedMotion = useReducedMotion();

  useEffect(() => {
    const hash = window.location.hash.slice(1);
    if (hash) {
      const idx = WALKTHROUGH_STAGES.findIndex((s) => s.id === hash);
      if (idx >= 0) setCurrentStage(idx);
    }
  }, []);

  useEffect(() => {
    const stage = WALKTHROUGH_STAGES[currentStage];
    if (stage) {
      window.history.replaceState(null, "", `#${stage.id}`);
    }
  }, [currentStage]);

  const goTo = useCallback(
    (index: number) => {
      if (index < 0 || index >= WALKTHROUGH_STAGES.length) return;
      setDirection(index > currentStage ? 1 : -1);
      setCurrentStage(index);
    },
    [currentStage],
  );

  const goNext = useCallback(
    () => goTo(currentStage + 1),
    [goTo, currentStage],
  );
  const goPrev = useCallback(
    () => goTo(currentStage - 1),
    [goTo, currentStage],
  );

  useEffect(() => {
    function handleKey(e: KeyboardEvent) {
      if (
        e.target instanceof HTMLInputElement ||
        e.target instanceof HTMLTextAreaElement
      )
        return;
      switch (e.key) {
        case "ArrowRight":
          e.preventDefault();
          goNext();
          break;
        case "ArrowLeft":
          e.preventDefault();
          goPrev();
          break;
        case "Escape":
          e.preventDefault();
          goTo(0);
          break;
        default:
          if (/^[0-9]$/.test(e.key)) {
            e.preventDefault();
            const num = parseInt(e.key, 10);
            const target = num === 0 ? 10 : num;
            if (target < WALKTHROUGH_STAGES.length) goTo(target);
          }
      }
    }
    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [goTo, goNext, goPrev]);

  const stage = WALKTHROUGH_STAGES[currentStage];

  const slideVariants = prefersReducedMotion
    ? {
        enter: { opacity: 0 },
        center: { opacity: 1 },
        exit: { opacity: 0 },
      }
    : {
        enter: (d: number) => ({ x: d > 0 ? 200 : -200, opacity: 0 }),
        center: { x: 0, opacity: 1 },
        exit: (d: number) => ({ x: d > 0 ? -200 : 200, opacity: 0 }),
      };

  return (
    <div className="flex min-h-[calc(100vh-4rem)]">
      {/* Left progress rail */}
      <aside className="hidden shrink-0 border-r border-slate-100 bg-slate-50/50 lg:block lg:w-52">
        <div className="sticky top-16 max-h-[calc(100vh-5rem)] overflow-y-auto px-4 py-6">
          <ProgressRail
            stages={WALKTHROUGH_STAGES}
            currentIndex={currentStage}
            onNavigate={goTo}
          />
        </div>
      </aside>

      {/* Main content */}
      <div className="flex min-w-0 flex-1 flex-col bg-white">
        {/* Mini-map */}
        <div className="sticky top-16 z-20">
          <PipelineMiniMap
            currentGroup={stage?.pipelineGroup ?? "neutral"}
          />
        </div>

        {/* Mobile progress */}
        <div className="flex items-center justify-between border-b border-slate-100 px-4 py-2 text-xs text-slate-400 lg:hidden">
          <button
            onClick={goPrev}
            disabled={currentStage === 0}
            className="disabled:opacity-30"
          >
            &larr; Prev
          </button>
          <span className="font-medium">
            {currentStage + 1} of {WALKTHROUGH_STAGES.length}
          </span>
          <button
            onClick={goNext}
            disabled={currentStage === WALKTHROUGH_STAGES.length - 1}
            className="disabled:opacity-30"
          >
            Next &rarr;
          </button>
        </div>

        {/* Stage content */}
        <div className="relative flex-1 overflow-x-hidden overflow-y-auto">
          <AnimatePresence mode="wait" custom={direction}>
            <motion.div
              key={stage?.id}
              custom={direction}
              variants={slideVariants}
              initial="enter"
              animate="center"
              exit="exit"
              transition={{
                duration: prefersReducedMotion ? 0 : 0.25,
                ease: "easeOut",
              }}
              className="mx-auto w-full max-w-4xl px-4 py-8 lg:px-8"
            >
              {stage && children(stage.id, currentStage)}
            </motion.div>
          </AnimatePresence>
        </div>

        {/* Bottom navigation */}
        <div className="sticky bottom-0 z-20">
          <StageNav
            stages={WALKTHROUGH_STAGES}
            currentIndex={currentStage}
            onNext={goNext}
            onPrev={goPrev}
          />
        </div>
      </div>
    </div>
  );
}
