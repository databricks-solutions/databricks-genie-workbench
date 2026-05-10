# applied_patches fixture — airline_1105451933925748_iter01

## Status: PII audit pending

Same PII audit constraint as proposal_generation (see that directory's README).
The applied_patches input carries `applied_entries_by_ag` which includes the
raw patch dicts that may contain LLM-generated SQL.

`test_chunk_c_replay.py` auto-skips when no `input.json`/`expected_output.json`
pair is present.
