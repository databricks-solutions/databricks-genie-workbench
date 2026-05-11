# RCO-4b Consolidating Trial Fixtures

Captured trial-run artifacts land here, organized one subdirectory
per anchor. The structure mirrors what `evidence-bundle` writes into
`docs/runid_analysis/<opt_run_id>/evidence/` so the postflight test
can read either path with the same code.

## Layout

```
rco4b_trial/
├── README.md                        ← this file
├── expected_outcomes.json           ← source of truth for assertions
├── f9_3b050ec5/                     ← created by Task 12 (post-trial)
│   ├── stdout.txt
│   ├── markers.json
│   └── replay_fixture.json
└── airline_clean/                   ← created by Task 13 (post-trial)
    ├── stdout.txt
    ├── markers.json
    └── replay_fixture.json
```

## Consumers

| Subdir | Consumed by |
|---|---|
| `expected_outcomes.json` | `tests/integration/test_rco4b_trial_postflight_artifact_capture.py` |
| `f9_3b050ec5/markers.json` | RCO-2b plan (drives the strict-mode flip) |
| `airline_clean/markers.json` | RCO-2b plan (asserts healthy posture) |
| `*/stdout.txt` | RCO-4c planning (alignment-gate extraction evidence) |
| `*/replay_fixture.json` | RCO-6 plan (replay/production parity anchor) |

## How fixtures get here

The operator runs `evidence-bundle` against the trial run, then runs
`scripts/promote_trial_anchor.sh` (created by Task 12) to copy the
relevant files into the per-anchor subdir.
