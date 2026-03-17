---
name: Plain-text instruction formatting
overview: Convert Genie Space instruction generation from Markdown to plain-text formatting by updating LLM prompts (template structure, few-shot examples, formatting rules) and adding a post-processing sanitizer to strip any residual Markdown before instructions reach the API.
todos:
  - id: update-holistic-prompt
    content: "Update LEVER_5_HOLISTIC_PROMPT in config.py: convert template structure from ## headers to ALL-CAPS: headers, update few-shot example, update output schema example, add explicit no-Markdown rule"
    status: completed
  - id: update-instruction-prompt
    content: "Update LEVER_5_INSTRUCTION_PROMPT in config.py: change formatting rule from '## headers' to 'ALL-CAPS HEADERS with colon, no Markdown'"
    status: completed
  - id: add-sanitizer
    content: Add _sanitize_plaintext_instructions() in optimizer.py and apply it in both the holistic and per-cluster Lever 5 instruction paths
    status: completed
  - id: verify-and-test
    content: Run apx dev check and unit tests to verify no regressions from prompt and code changes
    status: completed
isProject: false
---

# Plain-Text Instruction Formatting

## Problem

The Genie Space `general_instructions` field is rendered as **plain text** (not Markdown), but the LLM prompts (`LEVER_5_HOLISTIC_PROMPT` and `LEVER_5_INSTRUCTION_PROMPT`) explicitly instruct the LLM to produce Markdown formatting (`## Purpose`, `**bold**`, backtick code spans). This makes the instructions hard to read in the Genie Space UI, where `##` symbols and `**` markers appear as literal characters.

## Root Cause

Three places in [config.py](src/genie_space_optimizer/common/config.py) steer the LLM toward Markdown:

1. **Template structure** (lines 724-740): Uses `## Purpose`, `## Asset Routing`, etc. as Markdown headers
2. **Few-shot example** (lines 696-701): Demonstrates output with `## Purpose\n...` Markdown headers
3. **Formatting rule** (line 616): `"Use ## headers, - bullets, short lines"`
4. **Output schema example** (line 767): Shows `"## Purpose\\n..."` as the expected format

There is **no post-processing** anywhere in the pipeline to strip Markdown before the text reaches the API.

## Solution

### 1. Update `LEVER_5_HOLISTIC_PROMPT` in [config.py](src/genie_space_optimizer/common/config.py) (lines 654-782)

Replace Markdown-style template with plain-text formatting:

- **Template structure**: Change `## Purpose` to `PURPOSE:`, `## Asset Routing` to `ASSET ROUTING:`, etc. (ALL-CAPS section headers followed by a colon)
- **Few-shot example**: Update the example `instruction_text` to use the new format:

```
"instruction_text": "PURPOSE:\nThis Genie Space covers hotel booking analytics.\n\nASSET ROUTING:\n- Booking summaries: use catalog.schema.get_booking_summary TVF\n- Detailed bookings: use catalog.schema.fact_bookings\n\nQUERY RULES:\n- Always filter fact_bookings by booking_date for performance\n- Use is_current = true when joining dim_hotel"
```

- **Output schema example**: Change `"## Purpose\\n..."` to `"PURPOSE:\\n..."`
- **Instruction rules**: Add explicit rule: "Use ALL-CAPS SECTION HEADERS followed by a colon. Use - for bullet points. Use blank lines between sections. Do NOT use Markdown (no ##, no **, no backticks)."

### 2. Update `LEVER_5_INSTRUCTION_PROMPT` in [config.py](src/genie_space_optimizer/common/config.py) (lines 563-652)

- Change line 616 from `"Use ## headers, - bullets, short lines"` to `"Use ALL-CAPS HEADERS with colon (e.g. ASSET ROUTING:), - bullets, short lines. No Markdown."`

### 3. Add `_sanitize_plaintext_instructions()` in [optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)

A safety-net function applied after LLM response and JSON extraction, before validation:

```python
import re

def _sanitize_plaintext_instructions(text: str) -> str:
    """Strip residual Markdown from instruction text for plain-text display."""
    # Convert Markdown headers (## Foo) to ALL-CAPS HEADERS (FOO:)
    text = re.sub(r'^#{1,4}\s+(.+)$', lambda m: m.group(1).upper().rstrip() + ':', text, flags=re.MULTILINE)
    # Remove bold markers
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)
    # Remove italic markers
    text = re.sub(r'\*(.+?)\*', r'\1', text)
    # Remove inline code backticks
    text = re.sub(r'`([^`]+)`', r'\1', text)
    # Normalize multiple blank lines to at most two
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()
```

### 4. Apply sanitizer in `_call_llm_for_holistic_instructions()` in [optimizer.py](src/genie_space_optimizer/optimization/optimizer.py)

Insert the call after JSON extraction of `instruction_text` and before the length truncation/validation:

```python
instruction_text = _sanitize_plaintext_instructions(instruction_text)
```

Also apply it in the per-cluster Lever 5 path where `text_instruction` proposals are created (where `instruction_type == "text_instruction"` and `instruction_text` is extracted from the LLM response).

### 5. Newline preservation (verified, no changes needed)

The serialization pipeline already preserves newlines correctly:
- `_set_general_instructions()` splits text on `\n` into `list[str]`
- API payload serializes via `json.dumps()` preserving the array structure
- `_get_general_instructions()` joins with `\n` when reading back

No changes are needed in `applier.py` or `genie_client.py`.

## Files Changed

| File | Change |
|------|--------|
| `src/genie_space_optimizer/common/config.py` | Update `LEVER_5_HOLISTIC_PROMPT` (template, example, output schema, rules) and `LEVER_5_INSTRUCTION_PROMPT` (formatting rule) |
| `src/genie_space_optimizer/optimization/optimizer.py` | Add `_sanitize_plaintext_instructions()` and call it after instruction text extraction |
