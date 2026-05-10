# Boundary fixtures

Production-shape input/output captures for every typed stage
(`optimization/stages/<stage_key>.py`). Layout:

```
fixtures/
├── airline_1105451933925748_iter01/
│   ├── README.md          (anchor metadata)
│   ├── evaluation/        (Stage 1)
│   │   ├── input.json
│   │   └── expected_output.json
│   ├── rca_evidence/      (Stage 2)
│   │   ├── input.json
│   │   └── expected_output.json
│   └── ...
└── 7now_960148942255012_iter01/
    └── ...
```

## Discipline

1. **Production-shape only.** Captured from real lever-loop runs via
   `scripts/capture_stage_fixture.py`. Synthetic fixtures are NOT
   permitted under this directory. Synthetic shapes belong under
   `tests/unit/fixtures/` and exercise the dataclass round-trip,
   not the integration replay.

2. **Refresh requires signed approval.** A PR that modifies any
   `expected_output.json` must include the token
   `[fixture-refresh]` in its title and a justification in the PR
   description (what behaviour changed, why the old golden no longer
   matches).

3. **PII redaction.** SQL bodies, question text, and any field
   carrying customer data are redacted at capture time. Each anchor's
   README enumerates the exact redactions applied.

4. **One anchor per workspace.** Today the registry is two anchors
   (airline + 7Now). New anchors register here when a new workspace
   produces a defect-class previously unrepresented.

## Normalization

Captured JSON is written with `sort_keys=True` so that:
- Diff-friendly replay-byte-stability tests compare canonical key order.
- A schema change that renames a field cannot accidentally pass because
  the diff falls in a different place.

If a future test needs to assert source key order (e.g. it depends on
JSON object ordering for some marshalling reason), capture the raw
bundle separately and compare against that — do NOT compare against
the fixture under `tests/integration/fixtures/`.
