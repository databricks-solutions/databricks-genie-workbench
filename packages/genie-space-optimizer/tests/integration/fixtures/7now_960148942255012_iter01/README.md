# 7now_960148942255012_iter01

| Field | Value |
|---|---|
| Workspace | 7now |
| Lever-loop task ID | `960148942255012` |
| Lever-loop attempt | 12 |
| Iteration | 1 |
| Run dir (postmortem) | `docs/runid_analysis/3b050ec5-4032-457f-a785-2d1a3942a097/` |
| Captured at | <FILL ON FIRST CAPTURE: ISO-8601 timestamp> |
| Captured by | <PR author handle> |
| Defects this anchor pins | D-5, D-7, D-8 (Chunk-D side) + forbidden-AG no-op loop (Chunk B side) |

## Why this iteration

Iter 1 of attempt 12 produces a `+8.1pp` candidate that is correctly
rolled back because target `gs_026` is rendered as `soft_passing`
rather than fixed. Iters 2-5 then repeat AG1 with zero proposals —
this anchor is the canonical evidence for the forbidden-AG no-op
loop (Phase 3, Task 3.3) and the journey-validator drift (D-8,
Phase 1, Task 1.10).

## Redactions applied

Same redaction list as `airline_1105451933925748_iter01/README.md`.
