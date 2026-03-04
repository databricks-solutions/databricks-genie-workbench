# Genie Workbench — CUJ & Product Analysis

## What it is

**Genie Workbench** is a Databricks App that acts as a quality control and optimization platform for Genie Space administrators. The core job-to-be-done: *help builders understand why their Genie Space isn't performing well and fix it.*

---

## User Personas

| Persona | Job to be Done |
|---|---|
| **Genie Space Builder** | Assess my single space, find gaps, fix them |
| **ML/AI Platform Admin** | Monitor all spaces across the org, triage the worst ones |

---

## Core User Journeys

**CUJ 1: Quick Health Check (2 min)**
> "Is my Genie Space in good shape?"

Space List → click space → "Run IQ Scan" → see score (0-100), maturity label (Nascent / Basic / Developing / Proficient / Optimized), 4-dimension score breakdown, findings, and ranked next steps.

**CUJ 2: Deep Analysis (5-15 min)**
> "What specifically is wrong and why?"

Space Detail → Analysis tab → LLM analyzes each section against a best-practices checklist → per-section checklist pass/fail + severity-tagged findings + cross-sectional synthesis with "Good to Go / Quick Wins / Foundation Needed" assessment.

**CUJ 3: Data-Driven Optimization (20-60 min)**
> "My benchmark questions are failing — help me fix the config"

Optimize tab → Select benchmark questions → Run them through Genie → Label each answer correct/incorrect + give feedback → AI generates specific config change suggestions (prioritized High/Medium/Low) → Preview diff → Create new optimized Genie Space.

**CUJ 4: One-Click AI Fix (2-5 min)**
> "Just fix the obvious problems automatically"

Score tab → see findings → "Fix with AI Agent" → Fix Agent streams patch operations → applies directly to the Databricks Genie API.

**CUJ 5: Fleet Management (admin, ongoing)**
> "Which spaces are in crisis across my org?"

Admin Dashboard → org-wide stats, maturity distribution, top/bottom leaderboard, critical alerts (score < 40) → click to drill into a space.

---

## Capability Map

| Capability | Mechanism |
|---|---|
| IQ Scoring | Rule-based engine, 4 dimensions (Foundation/Data Setup/SQL Assets/Optimization), 0-100 scale |
| Deep Analysis | LLM (DBRX/Claude via serving endpoint) against structured checklist, streaming progress |
| Fix Agent | LLM generates JSON patches → writes to Genie API directly |
| Benchmark Labeling | Runs questions through Genie API, user labels results correct/incorrect |
| Optimization Suggestions | LLM analyzes labeling feedback + current config, outputs field-level suggestions |
| New Space Creation | Merges optimized config, calls Genie POST API to create a new space |
| Score History | Persisted to Lakebase (in-memory fallback when Lakebase not configured) |
| MLflow Tracing | Optional, traces LLM analysis runs for observability |

---

## Product Gaps & UX Observations

**1. Two overlapping "analyze" paths with unclear relationship**
The IQ Scan (rule-based, instant) and the Analysis tab (LLM-based, streaming) are both "analyze this space" but produce different outputs with no cross-referencing. Users have no guidance on when to use which or how the results relate.

**2. Optimize tab entry UX is misleading**
The empty state says "Run IQ Scan first, then come back to optimize" — but the Optimize flow doesn't consume IQ Scan results. It starts an independent benchmark labeling loop. The hint is factually incorrect.

**3. Fix Agent vs Optimize are parallel "fix" paths with no navigation between them**
Fix Agent (Score tab, quick, auto-applies from scan findings) and Optimize flow (Optimize tab, slower, from benchmark feedback) both produce config fixes. There's no guidance on which to use when, and they're surfaced in separate tabs with no connection.

**4. "Create new Genie Space" as the optimize output is counterintuitive**
Most users want to update their existing space, not create a second one. Creating a copy is a workaround for lacking a direct update path, but it's presented as the primary action without explanation. Users are likely to end up with duplicate spaces.

**5. Optimize state is ephemeral**
Refreshing mid-labeling session loses all progress. No persistence of benchmark labeling sessions means the 20-60 min optimize flow can't be paused or resumed.

**6. History tab is a dead end without Lakebase**
Without Lakebase configured, the History tab shows "No scan history yet" with no indication that setup is required. Users will assume the feature is broken.

**7. Admin dashboard is observability-only**
Admins can monitor fleet health and navigate to individual spaces, but have no admin-level actions — no bulk scan, no export, no ability to assign remediation ownership.
