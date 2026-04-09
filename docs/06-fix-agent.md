# Fix Agent

The Fix Agent is an AI-powered service that takes IQ Scanner findings and automatically generates targeted JSON patches to fix configuration gaps in a Genie Space. It addresses each finding individually, then applies all patches together in a single Databricks API call.

## How It Works

```
IQ Scanner findings
        │
        ▼
┌───────────────────────┐
│  Parallel LLM calls   │  One call per finding
│  (run_in_executor)    │  All run concurrently
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│  Patch validation     │  Check field_path against _VALID_FIELDS
│  + merge into config  │  Apply to mutable config copy
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│  Re-fetch + apply     │  Fresh GET → apply patches → PATCH API
│  with retry           │  Up to 3 attempts with back-off
└───────────────────────┘
```

## Parallel Patch Generation

To stay under the Databricks Apps proxy timeout (~120s), the Fix Agent launches **all LLM calls in parallel** rather than sequentially. Total wall time equals the slowest individual call instead of the sum.

Each finding gets its own LLM call with:
- The finding text
- The current space configuration (frozen snapshot)
- The Genie Space JSON schema reference

The LLM returns one or more patches in JSON format:

```json
{
  "patches": [
    {
      "field_path": "instructions.join_specs",
      "new_value": [...],
      "rationale": "Added join specification for orders-customers relationship"
    }
  ]
}
```

## Patch Format

Each patch has three fields:

| Field | Type | Description |
|-------|------|-------------|
| `field_path` | string | Dot-notation path into the `serialized_space` (e.g., `instructions.join_specs`, `data_sources.tables[0].description`) |
| `new_value` | any | The value to set at that path |
| `rationale` | string | Explanation of why this patch is needed |

### Field Path Validation

Every segment of `field_path` is validated against `_VALID_FIELDS` — a frozenset of all known Genie API field names. Patches with unknown field names are rejected and logged. This prevents the LLM from hallucinating invalid paths.

Array indices are supported: `data_sources.tables[0].column_configs[2].description`.

## ID Sanitization

The Genie API requires all `id` fields to be 32-character lowercase hex strings (UUID without hyphens). LLMs sometimes generate IDs with wrong formats, non-hex characters, or omit them entirely.

`_sanitize_ids()` recursively walks the config and:
- **Replaces** any `id` field that doesn't match the `^[0-9a-f]{32}$` pattern
- **Injects** missing `id` fields into entries within known ID-required arrays (`text_instructions`, `example_question_sqls`, `join_specs`, `filters`, `expressions`, `measures`, `questions`, `sql_functions`, `sample_questions`)

## Apply Flow

After all patches are generated and merged into a mutable config copy, the Fix Agent applies them to Databricks:

1. **Re-fetch** the space configuration via `get_serialized_space()` — this avoids "Space configuration has been modified since this export was taken" errors from stale configs.

2. **Apply** all patches to the fresh config.

3. **Sanitize** IDs and normalize join relationships.

4. **Deduplicate** column configs (reject duplicate `column_name`), instruction IDs (reject duplicates across all instruction arrays), sample questions, and benchmark questions.

5. **Clean and sort** via `_clean_config()` for API compliance.

6. **PATCH** to the Genie API: `PATCH /api/2.0/genie/spaces/{space_id}` with the updated `serialized_space`.

7. **Retry** on failure — up to 3 attempts with 2s and 4s delays, re-fetching the space config on each retry to handle concurrent modifications.

## SSE Events

The Fix Agent streams progress via Server-Sent Events:

| Event Status | Payload | When |
|-------------|---------|------|
| `thinking` | `message: "Analyzing N issue(s)..."` | Start of fix run |
| `thinking` | `message: "Fixing issue 1/N: ..."` | Before each finding's result |
| `patch` | `field_path`, `old_value`, `new_value`, `rationale` | After each finding produces patches |
| `applying` | `message: "Applying N fix(es)..."` | Before API call |
| `complete` | `patches_applied`, `summary`, `diff` | Success |
| `error` | `message` | Failure |

The `diff` in the `complete` event includes the full list of patches, the original config, and the updated config — enabling the frontend to show a before/after diff view.

## MLflow Tracing

All LLM calls and patch parsing are traced via MLflow:
- `fix_generate_patch` — LLM span for each finding
- `fix_parse_patch` — tool span for JSON parsing
- `fix_apply_config` — tool span for the API call

## Source Files

- `backend/services/fix_agent.py` — all fix agent logic
- `backend/prompts.py` — `get_fix_agent_single_prompt()` for per-finding prompts
- `backend/routers/spaces.py` — `POST /api/spaces/{id}/fix` SSE endpoint

## Related Documentation

- [IQ Scanner](05-iq-scanner.md) — produces the findings that feed the Fix Agent
- [Auto-Optimize](07-auto-optimize.md) — deeper optimization via benchmarks (independent path)
- [Authentication & Permissions](03-authentication-and-permissions.md) — Fix Agent uses OBO auth
