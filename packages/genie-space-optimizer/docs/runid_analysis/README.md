# Run ID Analysis Reports

This directory stores generated postmortems from the `gso-lever-loop-run-analysis` skill.

Report naming convention:

```text
<job_id>_<run_id>_analysis.md
<job_id>_<run_id>_analysis.json
```

Reports should be deterministic for the same source evidence. If re-analysis changes the conclusion, the report must explain what new evidence was added.

Reports should include short evidence snippets only. Do not include credentials, tokens, or full raw logs.
