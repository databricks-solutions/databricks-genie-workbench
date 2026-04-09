# Introduction

## What is Genie Workbench?

Genie Workbench is a developer tool for Databricks Genie Spaces — the natural-language-to-SQL interface for business users. It addresses the gap between creating a Genie Space and having one that reliably produces correct SQL: most spaces start with incomplete metadata, missing instructions, and no benchmarks, leading to poor user experiences.

Genie Workbench provides five capabilities that form a continuous improvement loop:

1. **Create** — An AI agent that walks you from business requirements through data discovery, inspection, and plan generation to a fully configured Genie Space.
2. **Score** — A rule-based IQ Scanner that evaluates space quality across 12 checks and assigns a maturity tier.
3. **Fix** — An AI agent that reads scan findings and generates targeted JSON patches to fix configuration gaps.
4. **Optimize** — A benchmark-driven pipeline (Auto-Optimize / GSO) that measures real accuracy, diagnoses failures, and iteratively improves the space configuration.
5. **Track** — Persistent history of every scan, optimization run, and configuration change, stored in Lakebase.

## Target Audience

- **Genie Space developers** building and maintaining spaces for their organizations
- **Data platform teams** managing quality across multiple Genie Spaces
- **Workspace administrators** deploying and operating the Workbench app

## Key Concepts

| Term | Definition |
|------|-----------|
| **Genie Space** | A Databricks resource that lets business users ask data questions in natural language. Configured with tables, instructions, example SQL, and benchmarks. |
| **`serialized_space`** | The JSON configuration of a Genie Space, accessed via the Genie Conversation API. Contains `data_sources`, `instructions`, `config`, and `benchmarks` sections. |
| **IQ Score** | A 0–12 score based on 12 binary checks. Each check evaluates one aspect of space configuration quality. |
| **Maturity Tier** | One of three labels derived from the IQ Score: **Not Ready**, **Ready to Optimize**, or **Trusted**. |
| **Finding** | A specific configuration gap identified by the IQ Scanner (e.g., "No join specifications for multi-table space"). Findings feed the Fix Agent. |
| **Benchmark** | A question-answer pair used to measure Genie accuracy. The expected SQL is compared against Genie's generated SQL by specialized judges. |
| **Lever** | An optimization strategy category in Auto-Optimize. Five lever types: tables/columns, metric views, TVFs, join specs, and instructions/example SQL. |
| **Patch** | A targeted change to the `serialized_space` configuration, represented as a `field_path` + `new_value` pair. |
| **OBO (On-Behalf-Of)** | Authentication model where the app acts on behalf of the signed-in user. See [Authentication & Permissions](03-authentication-and-permissions.md). |
| **SP (Service Principal)** | The app's own identity, used for background jobs and API fallback. See [Authentication & Permissions](03-authentication-and-permissions.md). |
| **GSO (Genie Space Optimizer)** | The Auto-Optimize engine package that runs the benchmark-driven optimization pipeline. |

## Feature Workflow

The features form a lifecycle that can be entered at any point and repeated as the space evolves:

```
┌──────────┐     ┌──────────┐     ┌──────────┐     ┌──────────────┐     ┌──────────┐
│  Create   │────▶│ IQ Scan  │────▶│   Fix    │────▶│ Auto-Optimize│────▶│  Track   │
│  Agent    │     │ (Score)  │     │  Agent   │     │   (GSO)      │     │ (History)│
└──────────┘     └──────────┘     └──────────┘     └──────────────┘     └──────────┘
                       ▲                                                       │
                       └───────────────────────────────────────────────────────┘
                                        continuous improvement
```

- **Create Agent** builds a new space from scratch (or updates an existing one).
- **IQ Scanner** evaluates the space and produces findings.
- **Fix Agent** applies targeted patches based on those findings.
- **Auto-Optimize** runs a deeper benchmark-driven pipeline for accuracy improvement.
- **Track** persists all results to Lakebase so you can see progress over time.
- The cycle repeats: after optimization, re-scan to see the updated score.

## Next Steps

- [Architecture Overview](02-architecture-overview.md) — understand how the app is built
- [Authentication & Permissions](03-authentication-and-permissions.md) — understand the security model
- [Deployment Guide](08-deployment-guide.md) — deploy the app to your workspace
