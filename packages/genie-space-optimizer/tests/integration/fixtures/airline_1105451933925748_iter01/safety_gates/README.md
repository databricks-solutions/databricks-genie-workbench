# safety_gates fixture — airline_1105451933925748_iter01

## Status: PII audit pending

Same PII audit constraint as proposal_generation (see that directory's README).
The safety_gates input carries `proposals_by_ag` which may include
customer-sensitive patch text.

`test_chunk_c_replay.py` auto-skips when no `input.json`/`expected_output.json`
pair is present.
