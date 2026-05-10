# `runid_analysis/` — Per-run evidence bundles

This directory is **gitignored** at the package level (except for `.gitkeep`
and this README). Each subdirectory is a per-optimization-run working area
produced by the `gso-evidence-bundle` CLI:

```
runid_analysis/<opt_run_id>/
├── evidence/        ← bundle output (manifest.json + everything pulled)
├── postmortem.md    ← analysis skill output (operator-readable)
└── intake.md        ← intake skill output (when applicable)
```

Bundles are large (hundreds of MB across iterations) and rebuildable
from `(job_id, run_id)` at any time. They are not source artifacts and
must not be committed.

When a postmortem proves load-bearing (e.g., it documents a real
defect or a lasting decision), copy the relevant section into a dated
plan or runbook under `packages/genie-space-optimizer/docs/` and
commit *that* document.

## Legacy report naming

Pre-bundle postmortems used the convention `<job_id>_<run_id>_analysis.md`
in this directory directly. New postmortems live under per-opt-run
subdirectories at the path above, written by `gso-lever-loop-run-analysis`.
