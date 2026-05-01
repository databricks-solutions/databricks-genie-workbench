# Instruction Quality Eval

This eval guards the text-instruction publishability contract introduced for
Lever 5. It checks that instruction text is Genie-facing, canonical-sectioned,
asset-grounded, and free of RCA/AFS/ASI diagnostics.

Run from `packages/genie-space-optimizer`:

```bash
uv run python scripts/create_instruction_quality_dataset.py
uv run python scripts/run_instruction_quality_eval.py
```

Expected result: `exact_publishability` is 1.0.
