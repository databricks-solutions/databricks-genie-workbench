# Genie Space Optimizer — Process Design Documentation

## Overview

The Genie Space Optimizer (GSO) is a benchmark-driven, evidence-grounded improvement loop for Databricks Genie Spaces. It treats a Genie Space as a **system under test**: it measures it against a benchmark, diagnoses why specific questions fail, applies bounded interventions, re-evaluates against the same benchmark, and only keeps changes that demonstrably improve the score.

> **Core Principle**
> The optimizer does not guess. It runs an experiment. Every accepted change is justified by post-patch evidence; every rejected change is rolled back.

This documentation set explains the optimizer through two complementary lenses:

1. **The Process Lens** — the six-task DAG that runs in production: Preflight → Baseline → Enrichment → Lever Loop → Finalize → Deploy.
2. **The Scientific Lens** — the optimizer's mentality of iterative, judged, repeatable improvement, anchored in MLflow as the evidence layer.

## Document Index

| # | Document | Description |
|---|----------|-------------|
| 01 | [Optimizer Mental Model](01-optimizer-mental-model.md) | Why the optimizer is a scientific experiment, not a prompt-tweaker |
| 02 | [The Six-Task DAG](02-six-task-dag.md) | The Databricks Job pipeline: Preflight → Baseline → Enrichment → Lever Loop → Finalize → Deploy |
| 03 | [Preflight, Benchmark, Enrichment](03-preflight-benchmark-enrichment.md) | How a raw Genie Space becomes a measurable experimental system |
| 04 | [Lever Loop and the RCA Process Spine](04-lever-loop-rca-process-spine.md) | The 11-stage iteration tape that powers every improvement attempt |
| 05 | [The Six Optimization Levers](05-six-optimization-levers.md) | Tables, Metric Views, TVFs, Joins, Instructions, SQL Expressions — what each lever changes |
| 06 | [Finalize, Repeatability, Deploy](06-finalize-repeatability-deploy.md) | Held-out checks, repeatability passes, and cross-workspace promotion |
| 07 | [MLflow Observability and Judges](07-mlflow-observability-and-judges.md) | The evidence room: experiments, datasets, traces, judges, artifacts, snapshots |
| 08 | [Slide Outline (SA Deep Dive)](08-slide-outline.md) | 20-slide presentation storyboard with visual generation prompts |

## Interactive Visualization

| File | Description |
|------|-------------|
| [interactive-optimizer-visualization.html](interactive-optimizer-visualization.html) | Standalone interactive microsite — open in any browser |

## Appendices

| # | Document | Description |
|---|----------|-------------|
| A | [Code Map](appendices/A-code-map.md) | Source-of-truth files for every concept in this doc set |
| B | [Visual Prompts](appendices/B-visual-prompts.md) | Reusable visual descriptions for designers and LLM image tools |
| C | [References](appendices/C-references.md) | Internal docs, code anchors, MLflow and Databricks references |

## The Optimizer In One Picture

```mermaid
flowchart LR
    A[Genie Space<br/>(unmeasured)] --> B[Preflight]
    B --> C[Baseline]
    C --> D[Enrichment]
    D --> E[Lever Loop<br/>(RCA-driven)]
    E --> F[Finalize<br/>(repeatability + held-out)]
    F --> G[Deploy<br/>(approved promotion)]
    G --> H[Genie Space<br/>(measurably better)]

    classDef start fill:#1e293b,stroke:#475569,color:#e2e8f0
    classDef stage fill:#0f172a,stroke:#06b6d4,color:#e2e8f0
    classDef end fill:#064e3b,stroke:#10b981,color:#ecfdf5
    class A start
    class B,C,D,E,F,G stage
    class H end
```

## Quick Reading Paths

| You are a... | Start here | Then read |
|---|---|---|
| **SA preparing a customer pitch** | [01 Mental Model](01-optimizer-mental-model.md) | [08 Slide Outline](08-slide-outline.md), [interactive HTML](interactive-optimizer-visualization.html) |
| **Engineer onboarding to GSO** | [02 Six-Task DAG](02-six-task-dag.md) | [04 Lever Loop](04-lever-loop-rca-process-spine.md), [Appendix A](appendices/A-code-map.md) |
| **Operator debugging a run** | [04 Process Spine](04-lever-loop-rca-process-spine.md) | [07 MLflow Evidence](07-mlflow-observability-and-judges.md) |
| **PM/leader explaining the science** | [01 Mental Model](01-optimizer-mental-model.md) | [07 MLflow Evidence](07-mlflow-observability-and-judges.md), [06 Finalize/Deploy](06-finalize-repeatability-deploy.md) |
| **Designer building visuals** | [Appendix B Visual Prompts](appendices/B-visual-prompts.md) | [08 Slide Outline](08-slide-outline.md) |

## The Five-Word Story

> **Measure. Diagnose. Intervene. Prove. Learn.**

Every concept in this documentation set is a refinement of these five words.

## Best Practices Showcased

| # | Best Practice | Implementation | Document |
|---|---------------|----------------|----------|
| 1 | Treat AI quality as measurable | Benchmark with judges and traces | [01](01-optimizer-mental-model.md), [07](07-mlflow-observability-and-judges.md) |
| 2 | Hold out a generalization set | Train vs `held_out` split, finalize evaluation | [03](03-preflight-benchmark-enrichment.md), [06](06-finalize-repeatability-deploy.md) |
| 3 | Make every patch a hypothesis | RCA evidence → action group → proposal | [04](04-lever-loop-rca-process-spine.md) |
| 4 | Reject improvements that don't survive judgment | `decide_acceptance` with gain floor | [04](04-lever-loop-rca-process-spine.md) |
| 5 | Version the system, not just the model | LoggedModel snapshots per iteration | [07](07-mlflow-observability-and-judges.md) |
| 6 | Promote only proven configurations | Cross-env deploy of UC registered model | [06](06-finalize-repeatability-deploy.md) |
| 7 | Make the trace the audit trail | `operator_transcript.md`, decision records, Phase H bundle | [04](04-lever-loop-rca-process-spine.md), [07](07-mlflow-observability-and-judges.md) |

## Key Statistics

| Metric | Value |
|--------|-------|
| Job tasks (Databricks DAG) | 6 (preflight, baseline, enrichment, lever_loop, finalize, deploy) |
| Process spine stages (per iteration) | 11 |
| Optimizer levers (selectable) | 6 (+ Lever 0 Proactive Enrichment, always-on) |
| LLM judges in the default panel | 6 |
| CODE judges in the default panel | 3 |
| Repeatability passes in finalize | 3 (default) |
| Documents in this set | 8 + 3 appendices + 1 interactive HTML |

## Related Documentation

- Repo-root [docs/07-auto-optimize.md](../../../../docs/07-auto-optimize.md) — operator-level overview
- Package [AGENTS.md](../../AGENTS.md) — engineering invariants (Bug #1–#4)
- Plan archive in [packages/genie-space-optimizer/docs/](..) — historical plans and run postmortems
