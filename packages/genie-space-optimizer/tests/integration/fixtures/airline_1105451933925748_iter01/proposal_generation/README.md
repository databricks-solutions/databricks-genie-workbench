# proposal_generation fixture — airline_1105451933925748_iter01

## Status: PII audit pending

Chunk C stages (proposal_generation, safety_gates, applied_patches) carry
`proposals_by_ag` which contains LLM-generated SQL text and may include
customer question text in `patch_text` / `value` fields.

The `input.json` / `expected_output.json` files will be vendored here once
the PII audit and redaction pass is complete (same audit protocol as the
Chunk D fixtures in `acceptance_decision/`, `bundle_assembly/`, etc.).

Until then, `test_chunk_c_replay.py::test_chunk_c_replay` auto-skips for
this stage via `cases_for_chunk()` returning no entries.
