import type React from "react";
import { createFileRoute } from "@tanstack/react-router";
import { WalkthroughShell } from "@/components/how-it-works/WalkthroughShell";
import { OverviewStage } from "@/components/how-it-works/stages/OverviewStage";
import { PreflightStage } from "@/components/how-it-works/stages/PreflightStage";
import { BenchmarksStage } from "@/components/how-it-works/stages/BenchmarksStage";
import { BaselineStage } from "@/components/how-it-works/stages/BaselineStage";
import { JudgesStage } from "@/components/how-it-works/stages/JudgesStage";
import { EnrichmentStage } from "@/components/how-it-works/stages/EnrichmentStage";
import { LeverLoopStage } from "@/components/how-it-works/stages/LeverLoopStage";
import { FailureAnalysisStage } from "@/components/how-it-works/stages/FailureAnalysisStage";
import { LeversStage } from "@/components/how-it-works/stages/LeversStage";
import { ThreeGatesStage } from "@/components/how-it-works/stages/ThreeGatesStage";
import { ConvergenceStage } from "@/components/how-it-works/stages/ConvergenceStage";
import { FinalizeDeployStage } from "@/components/how-it-works/stages/FinalizeDeployStage";

export const Route = createFileRoute("/how-it-works")({
  component: HowItWorksPage,
});

const STAGE_COMPONENTS: Record<string, () => React.ReactNode> = {
  overview: OverviewStage,
  preflight: PreflightStage,
  benchmarks: BenchmarksStage,
  baseline: BaselineStage,
  judges: JudgesStage,
  enrichment: EnrichmentStage,
  "lever-loop": LeverLoopStage,
  "failure-analysis": FailureAnalysisStage,
  levers: LeversStage,
  "three-gates": ThreeGatesStage,
  convergence: ConvergenceStage,
  finalize: FinalizeDeployStage,
};

function HowItWorksPage() {
  return (
    <WalkthroughShell>
      {(stageId) => {
        const Component = STAGE_COMPONENTS[stageId];
        return Component ? <Component /> : null;
      }}
    </WalkthroughShell>
  );
}
