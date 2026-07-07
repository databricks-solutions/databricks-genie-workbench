import type React from "react";
import { createFileRoute } from "@tanstack/react-router";
import { WalkthroughShell } from "@/components/how-it-works/WalkthroughShell";
import { OverviewStage } from "@/components/how-it-works/stages/OverviewStage";
import { IntakeSnapshotStage } from "@/components/how-it-works/stages/IntakeSnapshotStage";
import { BenchmarksStage } from "@/components/how-it-works/stages/BenchmarksStage";
import { OptimizeBaselineStage } from "@/components/how-it-works/stages/OptimizeBaselineStage";
import { JudgesStage } from "@/components/how-it-works/stages/JudgesStage";
import { LeverLoopStage } from "@/components/how-it-works/stages/LeverLoopStage";
import { FailureAnalysisStage } from "@/components/how-it-works/stages/FailureAnalysisStage";
import { LeversStage } from "@/components/how-it-works/stages/LeversStage";
import { AcceptanceRollbackStage } from "@/components/how-it-works/stages/AcceptanceRollbackStage";
import { ConvergenceStage } from "@/components/how-it-works/stages/ConvergenceStage";
import { PublishAuditStage } from "@/components/how-it-works/stages/PublishAuditStage";

export const Route = createFileRoute("/how-it-works")({
  component: HowItWorksPage,
});

const STAGE_COMPONENTS: Record<string, () => React.ReactNode> = {
  overview: OverviewStage,
  intake: IntakeSnapshotStage,
  benchmarks: BenchmarksStage,
  "optimize-baseline": OptimizeBaselineStage,
  judges: JudgesStage,
  "lever-loop": LeverLoopStage,
  "failure-analysis": FailureAnalysisStage,
  levers: LeversStage,
  "acceptance-rollback": AcceptanceRollbackStage,
  convergence: ConvergenceStage,
  "publish-audit": PublishAuditStage,
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
