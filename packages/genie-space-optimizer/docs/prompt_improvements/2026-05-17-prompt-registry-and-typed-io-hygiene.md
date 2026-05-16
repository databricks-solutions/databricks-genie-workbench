# Prompt Registry + Typed I/O Hygiene Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring every actively-firing LLM prompt into the `LEVER_PROMPTS` registry with `_link_prompt_to_trace()` wiring, migrate the inline `_generate_lever1_rca_proposal` f-string to a `SKILL.md` file, resolve the phantom `LEVER_1_2_COLUMN_PROMPT_FOR_L2` constant, and add typed Pydantic output contracts (with `response_format={"type": "json_schema"}` enforcement at every callsite) so prompt input/output drift fails fast instead of silently corrupting downstream state.

**Architecture:** Seven phases, executed in order. Phase A adds a regression-prevention guardrail test that fails for any future drift (registry, inline prompts, callsite linkage). Phase B fills the registry. Phase C migrates the inline RCA-bridge prompt to a `SKILL.md`. Phase D resolves the L2 phantom constant by making L2 a metadata-only SKILL.md (the L1 body remains the canonical template — they were byte-identical, so the L2 body was dead code). Phase E builds the typed-I/O infrastructure (`prompt_io.py` module + `_traced_llm_call(response_model=...)` extension) — Databricks Foundation Model APIs support `response_format={"type": "json_schema"}` but only a subset of JSON Schema keywords (no `pattern`, no `anyOf`/`oneOf`/`allOf`/`$ref`, no length constraints), so the helper strips/flattens unsupported keywords. Phase F defines one Pydantic output model per active prompt and wires it at the callsite — Tasks 14-18 cover the high-volume tier (Stage 1, Lever 6, RCA bridge, Lever 1/2, strategist family), Task 19 covers the medium-volume tier (Lever 4 + Lever 5), Task 20 documents the explicit deferred allowlist for the low-volume preflight/enrichment tier. Phase G updates the developer docs with the new contract.

**Tech Stack:** Python 3.11+, Pydantic v2 (already in `pyproject.toml`), OpenAI Python SDK (already used by `_traced_llm_call`), MLflow GenAI Prompt Registry (already used by `register_judge_prompts`), pytest, `_SKILL_LOADER.load_prompt()` (already used by 9 SKILL.md files in `skills/`).

---

## Pre-flight: empirical evidence the plan responds to

Numbers below come from `mlflow.client.search_traces` against the Trial-5 experiments (`3044964578604055` airline, `3044964578604062` 7now) on `fevm-prashanth-subrahmanyam.cloud.databricks.com`.

| Prompt | Loaded from | In `LEVER_PROMPTS`? | Trial-5 LLM calls |
|---|---|---|---|
| `STRATEGIST_PROMPT` | inline string `common/config.py:2807` | yes | many |
| `STRATEGIST_TRIAGE_PROMPT` | inline string `common/config.py:2968` | yes | many |
| `STRATEGIST_DETAIL_PROMPT` | inline string `common/config.py:3106` | yes | many |
| `ADAPTIVE_STRATEGIST_PROMPT` | `adaptive-strategist/SKILL.md` | yes | when Stage-1 returns empty |
| `LEVER_1_2_COLUMN_PROMPT` | `lever-1-table-column-description/SKILL.md` | yes | 0 (dormant) |
| `LEVER_4_JOIN_SPEC_PROMPT` | `lever-4-join-spec/SKILL.md` | yes | medium |
| `LEVER_4_JOIN_DISCOVERY_PROMPT` | inline string `common/config.py:2514` | yes | medium |
| `LEVER_5_INSTRUCTION_PROMPT` | `lever-5-instruction/SKILL.md` | yes | medium |
| `LEVER_5A_INSTRUCTION_PROMPT` | `lever-5a-instructions/SKILL.md` | **NO** | low |
| `LEVER_5_HOLISTIC_PROMPT` | inline string `common/config.py:2666` | yes | medium |
| `LEVER_6_SQL_EXPRESSION_PROMPT` | `lever-6-sql-expression/SKILL.md` | **NO** | 48 |
| `STAGE_1_DISCOVERY_PROMPT` | `stage-1-discovery/SKILL.md` | **NO** | every AG |
| `_SYNTHESIS_PROMPT_TEMPLATE` (lever-5b example SQL) | `lever-5b-example-sql/SKILL.md` | **NO** | every cluster-driven synthesis |
| `_generate_lever1_rca_proposal` inline | f-string in `optimizer.py:12511-12534` (no SKILL.md exists) | **NO** | 12 (only firing L1 LLM path) |
| `PROPOSAL_GENERATION_PROMPT` (lever-3-tvf-routing) | `lever-3-tvf-routing/SKILL.md` | yes | low |

Other observations the plan must address:

- L2 SKILL.md (`lever-2-mv-column-refinement/SKILL.md`) declares `prompt_constant_name: LEVER_1_2_COLUMN_PROMPT_FOR_L2` but that constant is **never loaded** — `optimizer.py:8280-8281` routes both `lever=1` and `lever=2` through `LEVER_1_2_COLUMN_PROMPT`. The L2 prompt body is byte-identical dead code.
- `CATALOGUE.md:10` documents the same phantom constant.
- No prompt has a Pydantic / JSON-schema output contract today. `proposal_shape.validate_column_proposal_shape()` is the only typed output validator and it covers only two patch types (`update_column_description`, `add_column_synonym`) with bare-dict checks.
- `_traced_llm_call()` already supports `response_validator` (retries with backoff on validation failure) and `max_tokens`, but does **not** yet pass `response_format` to the OpenAI SDK.
- Databricks Foundation Model APIs support `response_format={"type": "json_schema"}` but reject `pattern`, `anyOf`, `oneOf`, `allOf`, `prefixItems`, `$ref`, and length-constraint keywords. Pydantic v2's default `model_json_schema()` emits some of these for Optional/Union types.

---

## File structure

**Created files:**

| Path | Responsibility |
|---|---|
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py` | Base `LLMOutputContract` Pydantic class, `build_response_format()` (flattens Pydantic schema to Databricks-supported subset), `validate_and_parse()` helper, per-prompt output models. |
| `packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-1-rca-bridge/SKILL.md` | Extracted RCA-bridge prompt (previously the inline f-string in `_generate_lever1_rca_proposal`). |
| `packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py` | Regression-prevention test: every prompt template that fires an LLM call must be in `LEVER_PROMPTS`. Fails fast on future drift. |
| `packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py` | Unit tests for `LLMOutputContract`, `build_response_format()`, and each per-prompt output model. |

**Modified files:**

| Path | Change |
|---|---|
| `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py` | Add `LEVER_1_RCA_BRIDGE_PROMPT` constant, add 5 entries to `LEVER_PROMPTS` dict, lock `_REGISTERED_PROMPT_NAMES` invariant. |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py` | Extend `_traced_llm_call()` with `response_format` + `response_model` kwargs; extend `_call_llm_for_proposal()` similarly; replace inline f-string in `_generate_lever1_rca_proposal()` with `format_mlflow_template(LEVER_1_RCA_BRIDGE_PROMPT, ...)`; wire `_link_prompt_to_trace()` + `response_model=` at 4 callsites (Stage 1, Lever 6, RCA bridge, Lever 1/2). |
| `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/three_stage_pipeline.py` | Wire `response_model=` at the Stage-1 discovery call. |
| `packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-2-mv-column-refinement/SKILL.md` | Remove the body; keep only frontmatter (skill_id, when_to_pick, target_kind, target_min_count). Add a one-line `# This SKILL.md is metadata-only. The LLM template is shared with lever-1-table-column-description (LEVER_1_2_COLUMN_PROMPT).` |
| `packages/genie-space-optimizer/src/genie_space_optimizer/skills/CATALOGUE.md` | Fix the phantom `LEVER_1_2_COLUMN_PROMPT_FOR_L2` reference; document the metadata-only L2 SKILL pattern. |
| `packages/genie-space-optimizer/docs/skill-catalogue.md` | New section: "Prompt registry invariants" and "Typed output contracts". |

---

## Phase A — Guardrail (lock the gap, fail-fast on drift)

### Task 1: Registry-inventory regression test

**Files:**
- Create: `packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py`

- [ ] **Step 1: Write the failing test**

```python
"""Prompt registry coverage tests.

These tests fail-fast when a new prompt is added without being registered
in ``LEVER_PROMPTS`` (or without being wired through
``_link_prompt_to_trace()`` at its callsite). They are the regression
guardrail that makes the rest of this plan stick.

A failure here means one of three things happened:
  1. Someone added a new ``_SKILL_LOADER.load_prompt(...)`` constant and
     forgot to add it to ``LEVER_PROMPTS``.
  2. Someone added a new ``_traced_llm_call(... prompt=...)`` with an
     inline f-string prompt instead of loading from a ``SKILL.md`` file.
  3. Someone added a new callsite that does not call
     ``_link_prompt_to_trace()`` before the LLM call — so the Linked
     Prompts tab in the MLflow trace UI will show nothing.

Remediation for each is in the docstring of the failing assertion.
"""
from __future__ import annotations

import ast
import pathlib

import pytest

from genie_space_optimizer.common import config as cfg

SRC = pathlib.Path(__file__).resolve().parents[2] / "src" / "genie_space_optimizer"


def _all_skill_loader_constants() -> set[str]:
    """Return every constant assigned via ``_SKILL_LOADER.load_prompt(...)``.

    Walks the AST of every .py file under src/ to find assignments of the
    shape ``FOO_PROMPT = _SKILL_LOADER.load_prompt("skill_id",
    expected_constant_name="FOO_PROMPT")``. Returns the set of constant
    names — these are the SKILL.md-backed prompts.
    """
    names: set[str] = set()
    for py_file in SRC.rglob("*.py"):
        try:
            tree = ast.parse(py_file.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if not (
                isinstance(node.value, ast.Call)
                and isinstance(node.value.func, ast.Attribute)
                and node.value.func.attr == "load_prompt"
                and isinstance(node.value.func.value, ast.Name)
                and node.value.func.value.id == "_SKILL_LOADER"
            ):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    names.add(target.id)
    return names


def test_every_skill_loader_constant_is_in_LEVER_PROMPTS():
    """Every ``_SKILL_LOADER.load_prompt(...)`` constant must be in
    ``LEVER_PROMPTS``.

    Remediation if this fails: add the constant to ``LEVER_PROMPTS`` in
    ``common/config.py``. The registered name is the lowercase snake_case
    of the constant minus the ``_PROMPT`` suffix.
    """
    skill_constants = _all_skill_loader_constants()
    registered_constants = {
        id(template): name for name, template in cfg.LEVER_PROMPTS.items()
    }
    missing: list[str] = []
    for const_name in sorted(skill_constants):
        const_value = getattr(cfg, const_name, None)
        if const_value is None:
            # Constant lives in another module (e.g. synthesis.py). Skip
            # for the in-module check; covered by Task 4 separately.
            continue
        if id(const_value) not in registered_constants:
            missing.append(const_name)
    assert not missing, (
        f"These _SKILL_LOADER constants are NOT in LEVER_PROMPTS, so they "
        f"will not be registered to MLflow Prompt Registry and the Linked "
        f"Prompts tab will be blank for their traces: {missing}. Add an "
        f"entry to LEVER_PROMPTS in common/config.py."
    )


def test_no_inline_fstring_LLM_prompts_in_optimization_module():
    """Inline f-string LLM prompts are forbidden — every prompt must live
    in a ``SKILL.md`` file (so it gets versioning, registry, tracing,
    and A/B-testable rollout).

    Detection heuristic: search for ``_traced_llm_call(`` callsites whose
    ``prompt=`` argument is a Python f-string literal (``f"..."`` or
    ``f'''...'''``) longer than 200 chars. Short prompts (system message
    like ``"You are a metadata curator."``) are exempt — only the user
    prompt is checked.

    Allowlist: tests/, demos/, and any callsite that loads its template
    via ``_SKILL_LOADER.load_prompt(...)`` is fine.
    """
    optimization_dir = SRC / "optimization"
    violations: list[str] = []
    for py_file in optimization_dir.rglob("*.py"):
        try:
            tree = ast.parse(py_file.read_text())
        except SyntaxError:
            continue
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            func_name = (
                func.attr if isinstance(func, ast.Attribute) else
                func.id if isinstance(func, ast.Name) else None
            )
            if func_name not in {"_traced_llm_call", "_call_llm_for_proposal"}:
                continue
            # Find the prompt arg (positional index varies; check keyword first).
            prompt_arg = None
            for kw in node.keywords or []:
                if kw.arg == "prompt":
                    prompt_arg = kw.value
                    break
            # _traced_llm_call signature: (w, system_msg, prompt, *, span_name, ...)
            # so positional index 2 is prompt.
            if prompt_arg is None and func_name == "_traced_llm_call" and len(node.args) >= 3:
                prompt_arg = node.args[2]
            if prompt_arg is None:
                continue
            if isinstance(prompt_arg, ast.JoinedStr):
                # f-string literal in call site.
                rendered_chars = sum(
                    len(v.value) for v in prompt_arg.values if isinstance(v, ast.Constant)
                )
                if rendered_chars > 200:
                    violations.append(
                        f"{py_file.relative_to(SRC.parent)}:{node.lineno} — "
                        f"inline f-string prompt ({rendered_chars} chars of "
                        f"static text). Extract to a SKILL.md file under "
                        f"skills/<skill-name>/SKILL.md and load via "
                        f"_SKILL_LOADER.load_prompt(...)."
                    )
    assert not violations, (
        "Inline f-string LLM prompts found — every prompt must live in a "
        "SKILL.md file:\n" + "\n".join(violations)
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py -v`

Expected: BOTH tests FAIL.
- `test_every_skill_loader_constant_is_in_LEVER_PROMPTS` reports `['LEVER_5A_INSTRUCTION_PROMPT', 'LEVER_6_SQL_EXPRESSION_PROMPT', 'STAGE_1_DISCOVERY_PROMPT']` as missing.
- `test_no_inline_fstring_LLM_prompts_in_optimization_module` reports the inline f-string at `optimizer.py:12511`.

These are the gaps Phase B–F will close. Keep this test failing until those phases land.

- [ ] **Step 3: Commit the failing test as the regression guardrail**

```bash
git add packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py
git commit -m "test: add prompt-registry inventory guardrail (intentionally failing — closes via plan 2026-05-17-prompt-registry-and-typed-io-hygiene.md)"
```

The intentional failure is documented in the commit message. Phases B/C will make it pass.

---

## Phase B — Registry completeness (extend `LEVER_PROMPTS`)

### Task 2: Register `LEVER_6_SQL_EXPRESSION_PROMPT`

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py:4460-4485`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:13000-13003`
- Test: `packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py`

- [ ] **Step 1: Write the failing test**

Add to `test_prompt_registry_inventory.py`:

```python
def test_lever_6_sql_expression_prompt_is_registered():
    assert "lever_6_sql_expression" in cfg.LEVER_PROMPTS, (
        "LEVER_6_SQL_EXPRESSION_PROMPT is the active Stage-2 prompt for "
        "lever-6 (48 LLM calls in Trial-5) but is not in LEVER_PROMPTS, "
        "so it is never registered to MLflow Prompt Registry. Add "
        "'lever_6_sql_expression': LEVER_6_SQL_EXPRESSION_PROMPT to "
        "LEVER_PROMPTS in common/config.py."
    )
    assert cfg.LEVER_PROMPTS["lever_6_sql_expression"] is cfg.LEVER_6_SQL_EXPRESSION_PROMPT
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_lever_6_sql_expression_prompt_is_registered -v`
Expected: FAIL with `AssertionError: LEVER_6_SQL_EXPRESSION_PROMPT is the active Stage-2 prompt for lever-6...`

- [ ] **Step 3: Add the registry entry**

In `common/config.py`, locate the `LEVER_PROMPTS` dict at line 4460 and add an entry after `"lever_5_holistic"`:

```python
LEVER_PROMPTS: dict[str, str] = {
    "strategist": STRATEGIST_PROMPT,
    "strategist_triage": STRATEGIST_TRIAGE_PROMPT,
    "strategist_detail": STRATEGIST_DETAIL_PROMPT,
    "adaptive_strategist": ADAPTIVE_STRATEGIST_PROMPT,
    "lever_1_2_column": LEVER_1_2_COLUMN_PROMPT,
    "lever_4_join_spec": LEVER_4_JOIN_SPEC_PROMPT,
    "lever_4_join_discovery": LEVER_4_JOIN_DISCOVERY_PROMPT,
    "lever_5_instruction": LEVER_5_INSTRUCTION_PROMPT,
    "lever_5_holistic": LEVER_5_HOLISTIC_PROMPT,
    "lever_6_sql_expression": LEVER_6_SQL_EXPRESSION_PROMPT,
    "proposal_generation": PROPOSAL_GENERATION_PROMPT,
    "description_enrichment": DESCRIPTION_ENRICHMENT_PROMPT,
    "table_description_enrichment": TABLE_DESCRIPTION_ENRICHMENT_PROMPT,
    "proactive_instruction": PROACTIVE_INSTRUCTION_PROMPT,
    "expand_instruction": EXPAND_INSTRUCTION_PROMPT,
    "space_description": SPACE_DESCRIPTION_PROMPT,
    "sample_questions": SAMPLE_QUESTIONS_PROMPT,
    "gt_repair": GT_REPAIR_PROMPT,
}
```

- [ ] **Step 4: Wire `_link_prompt_to_trace()` at the lever-6 callsite**

In `optimizer.py:13000`, locate the `_traced_llm_call(... span_name="lever6_llm", ...)` callsite inside `_generate_lever6_proposal()`. Add a `_link_prompt_to_trace` call immediately before the LLM call:

```python
# G2 (2026-05-17 prompt-registry-and-typed-io-hygiene plan, Task 2)
# Link the registered lever_6_sql_expression prompt to the active trace
# so the Linked Prompts tab in the MLflow trace UI surfaces it.
from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
_link_prompt_to_trace("lever_6_sql_expression")

try:
    raw_text, _ = _traced_llm_call(
        w, "You are a SQL expression expert.", prompt,
        span_name="lever6_llm",
        max_tokens=LEVER_6_MAX_TOKENS,
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_lever_6_sql_expression_prompt_is_registered packages/genie-space-optimizer/tests/optimization/test_lever6_prompt.py -v`
Expected: both PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py
git commit -m "feat: register LEVER_6_SQL_EXPRESSION_PROMPT and wire trace linkage"
```

### Task 3: Register `STAGE_1_DISCOVERY_PROMPT`

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py:4460-4485`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:_call_llm_for_stage_1_discovery`

- [ ] **Step 1: Write the failing test**

Add to `test_prompt_registry_inventory.py`:

```python
def test_stage_1_discovery_prompt_is_registered():
    assert "stage_1_discovery" in cfg.LEVER_PROMPTS, (
        "STAGE_1_DISCOVERY_PROMPT is the routing brain for the entire "
        "three-stage pipeline (fires once per AG) but is not in "
        "LEVER_PROMPTS, so it is never registered to MLflow Prompt "
        "Registry. Add 'stage_1_discovery': STAGE_1_DISCOVERY_PROMPT to "
        "LEVER_PROMPTS in common/config.py."
    )
    assert cfg.LEVER_PROMPTS["stage_1_discovery"] is cfg.STAGE_1_DISCOVERY_PROMPT
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_stage_1_discovery_prompt_is_registered -v`
Expected: FAIL.

- [ ] **Step 3: Add the registry entry**

In `common/config.py`, locate the `LEVER_PROMPTS` dict and insert `stage_1_discovery` immediately after `adaptive_strategist` (preserving the strategist/discovery → lever-loop reading order):

```python
LEVER_PROMPTS: dict[str, str] = {
    "strategist": STRATEGIST_PROMPT,
    "strategist_triage": STRATEGIST_TRIAGE_PROMPT,
    "strategist_detail": STRATEGIST_DETAIL_PROMPT,
    "adaptive_strategist": ADAPTIVE_STRATEGIST_PROMPT,
    "stage_1_discovery": STAGE_1_DISCOVERY_PROMPT,
    "lever_1_2_column": LEVER_1_2_COLUMN_PROMPT,
    ...
}
```

Note: `STAGE_1_DISCOVERY_PROMPT` is defined at `common/config.py:5447`, which is *after* `LEVER_PROMPTS` is constructed at line 4460. You will hit a `NameError`. The fix is to move the `STAGE_1_DISCOVERY_PROMPT = _SKILL_LOADER.load_prompt(...)` assignment to immediately before `LEVER_PROMPTS` (i.e. relocate from `common/config.py:5447` to immediately after `LEVER_6_SQL_EXPRESSION_PROMPT` at line 4699). Verify the relocation does not introduce a forward reference to anything `STAGE_1_DISCOVERY_PROMPT` depends on (it depends only on `_SKILL_LOADER`, which is defined at the top of the file).

- [ ] **Step 4: Wire `_link_prompt_to_trace()` at the stage-1 callsite**

In `optimizer.py`, locate `_call_llm_for_stage_1_discovery()` (around line 10883). Add the trace-link before the `_traced_llm_call` (or `_call_llm_openai`) invocation:

```python
# G3 (2026-05-17 prompt-registry-and-typed-io-hygiene plan, Task 3)
# Link the registered stage_1_discovery prompt to the active trace.
from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
_link_prompt_to_trace("stage_1_discovery")

# ... existing _traced_llm_call / _call_llm_openai invocation ...
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_stage_1_discovery_prompt_is_registered -v`
Expected: PASS.
Run: `pytest packages/genie-space-optimizer/tests/optimization/ -v -k stage_1 -x`
Expected: all existing Stage-1 tests still PASS (the linkage is additive).

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py
git commit -m "feat: register STAGE_1_DISCOVERY_PROMPT and wire trace linkage"
```

### Task 4: Register `LEVER_5A_INSTRUCTION_PROMPT`

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py:4460-4485`
- Modify: the `_traced_llm_call` callsite that uses `LEVER_5A_INSTRUCTION_PROMPT` (search the optimizer for it)

- [ ] **Step 1: Find every callsite that uses LEVER_5A_INSTRUCTION_PROMPT**

Run: `rg "LEVER_5A_INSTRUCTION_PROMPT" packages/genie-space-optimizer/src/`

Record the callsite line numbers. The expected result is the constant definition at `common/config.py:2800-2802` plus one or more callers in `optimizer.py` or `harness.py` (the precise filename varies; do not assume).

- [ ] **Step 2: Write the failing test**

```python
def test_lever_5a_instruction_prompt_is_registered():
    assert "lever_5a_instruction" in cfg.LEVER_PROMPTS, (
        "LEVER_5A_INSTRUCTION_PROMPT is the Plan-2 split path for "
        "instructions (separate from LEVER_5_HOLISTIC_PROMPT) but is not "
        "in LEVER_PROMPTS. Add 'lever_5a_instruction': "
        "LEVER_5A_INSTRUCTION_PROMPT to LEVER_PROMPTS in common/config.py."
    )
    assert cfg.LEVER_PROMPTS["lever_5a_instruction"] is cfg.LEVER_5A_INSTRUCTION_PROMPT
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_lever_5a_instruction_prompt_is_registered -v`
Expected: FAIL.

- [ ] **Step 4: Add the registry entry**

In `common/config.py`, insert into the `LEVER_PROMPTS` dict:

```python
    "lever_5_instruction": LEVER_5_INSTRUCTION_PROMPT,
    "lever_5_holistic": LEVER_5_HOLISTIC_PROMPT,
    "lever_5a_instruction": LEVER_5A_INSTRUCTION_PROMPT,
    "lever_6_sql_expression": LEVER_6_SQL_EXPRESSION_PROMPT,
```

- [ ] **Step 5: Wire `_link_prompt_to_trace("lever_5a_instruction")` at the callsite found in Step 1**

For each callsite found in Step 1, add immediately before the `_traced_llm_call` invocation:

```python
from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
_link_prompt_to_trace("lever_5a_instruction")
```

- [ ] **Step 6: Run tests**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_lever_5a_instruction_prompt_is_registered -v`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/ packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py
git commit -m "feat: register LEVER_5A_INSTRUCTION_PROMPT and wire trace linkage"
```

### Task 5: Register `_SYNTHESIS_PROMPT_TEMPLATE` (lever-5b example SQL)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py:4460-4485`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py:909-912, 1060-1063`

- [ ] **Step 1: Write the failing test**

Note: `_SYNTHESIS_PROMPT_TEMPLATE` lives in `optimization/synthesis.py`, not `common/config.py`. The registry expects all entries in one dict; we need to import the constant into `config.py` for the registry entry.

```python
def test_lever_5b_example_sql_prompt_is_registered():
    """The lever-5b example-SQL synthesis prompt fires for every cluster-
    driven synthesis attempt — high volume and high blast radius.
    """
    assert "lever_5b_example_sql" in cfg.LEVER_PROMPTS, (
        "_SYNTHESIS_PROMPT_TEMPLATE (loaded from "
        "lever-5b-example-sql/SKILL.md) is the active example-SQL "
        "synthesis prompt but is not in LEVER_PROMPTS. Import it into "
        "common/config.py and add 'lever_5b_example_sql': "
        "_SYNTHESIS_PROMPT_TEMPLATE to LEVER_PROMPTS."
    )
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_lever_5b_example_sql_prompt_is_registered -v`
Expected: FAIL.

- [ ] **Step 3: Lift the loader to `common/config.py`**

The current state has `_SYNTHESIS_PROMPT_TEMPLATE` defined in `optimization/synthesis.py:90-93`. The registry pattern requires all template constants to be discoverable from `common/config.py`. Lift the definition:

In `common/config.py`, near the other `_SKILL_LOADER.load_prompt(...)` calls (e.g. immediately after `LEVER_6_SQL_EXPRESSION_PROMPT` at line 4699):

```python
# G5 (2026-05-17 prompt-registry-and-typed-io-hygiene plan, Task 5)
# Lifted from optimization/synthesis.py so the prompt registry can find
# it. synthesis.py now imports this constant from common.config.
LEVER_5B_EXAMPLE_SQL_PROMPT = _SKILL_LOADER.load_prompt(
    "lever-5b-example-sql",
    expected_constant_name="LEVER_5B_EXAMPLE_SQL_PROMPT",
)
```

Then in `optimization/synthesis.py:90-93`, replace the local definition:

```python
# G5 — Template lifted to common.config so the prompt registry can
# discover and register it. Backwards-compat alias preserved for any
# in-flight refactor that still imports the old name.
from genie_space_optimizer.common.config import LEVER_5B_EXAMPLE_SQL_PROMPT
_SYNTHESIS_PROMPT_TEMPLATE = LEVER_5B_EXAMPLE_SQL_PROMPT
```

Note: the `expected_constant_name` in the loader call changed from `_SYNTHESIS_PROMPT_TEMPLATE` to `LEVER_5B_EXAMPLE_SQL_PROMPT` — verify the loader does not assert on this name strictly (it is a sanity-check tag, not a binding identifier). If it does enforce, update `lever-5b-example-sql/SKILL.md` frontmatter `prompt_constant_name:` to match.

- [ ] **Step 4: Add the registry entry**

In `common/config.py`'s `LEVER_PROMPTS` dict:

```python
    "lever_5_instruction": LEVER_5_INSTRUCTION_PROMPT,
    "lever_5_holistic": LEVER_5_HOLISTIC_PROMPT,
    "lever_5a_instruction": LEVER_5A_INSTRUCTION_PROMPT,
    "lever_5b_example_sql": LEVER_5B_EXAMPLE_SQL_PROMPT,
    "lever_6_sql_expression": LEVER_6_SQL_EXPRESSION_PROMPT,
```

- [ ] **Step 5: Wire `_link_prompt_to_trace()` at the two callsites**

In `optimization/synthesis.py:909-912` and `:1060-1063`, add before each `_traced_llm_call`:

```python
from genie_space_optimizer.optimization.evaluation import _link_prompt_to_trace
_link_prompt_to_trace("lever_5b_example_sql")

raw, _ = _traced_llm_call(
    w, "You are a SQL example author.", p,
    span_name="lever_5b_example_sql",
)
```

- [ ] **Step 6: Run tests**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_lever_5b_example_sql_prompt_is_registered packages/genie-space-optimizer/tests/optimization/test_synthesis.py -v`
Expected: PASS for the new test; PASS for any existing synthesis tests (the changes are additive).

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py
git commit -m "feat: lift _SYNTHESIS_PROMPT_TEMPLATE to common.config, register as lever_5b_example_sql"
```

### Task 6: Verify Phase B by re-running the inventory guardrail

**Files:**
- Test only: `packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py`

- [ ] **Step 1: Run the full inventory test**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py -v`

Expected: 5 tests PASS:
- `test_every_skill_loader_constant_is_in_LEVER_PROMPTS`
- `test_lever_6_sql_expression_prompt_is_registered`
- `test_stage_1_discovery_prompt_is_registered`
- `test_lever_5a_instruction_prompt_is_registered`
- `test_lever_5b_example_sql_prompt_is_registered`

Expected: 1 test still FAILS:
- `test_no_inline_fstring_LLM_prompts_in_optimization_module` (still complains about `_generate_lever1_rca_proposal` at `optimizer.py:12511`). Phase C closes this.

- [ ] **Step 2: No commit** — checkpoint only.

---

## Phase C — Inline RCA-bridge → SKILL.md migration

### Task 7: Create the `lever-1-rca-bridge/SKILL.md`

**Files:**
- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-1-rca-bridge/SKILL.md`

- [ ] **Step 1: Write the SKILL.md file**

The inline prompt to extract lives at `optimizer.py:12511-12534`. It is built with f-strings; the template has 5 dynamic slots:
- `{{ target }}` — `"table <name>"` or `"<table>.<column>"`
- `{{ intent }}` — RCA-derived intent string
- `{{ expected_objects }}` — `list[str]`
- `{{ actual_objects }}` — `list[str]`
- `{{ failure_context_json }}` — JSON-serialized list of AFS projections
- `{{ existing_description }}` — current description (may be empty)
- `{{ existing_synonyms }}` — current synonyms (may be `[]`)
- `{{ is_table_level }}` — boolean controlling whether the synonyms instruction is included

Plus a static role + ruleset. Create the file:

```markdown
---
skill_id: lever-1-rca-bridge
prompt_constant_name: LEVER_1_RCA_BRIDGE_PROMPT
causal_or_non_causal: causal
pickable_by_stage_1: false
description: RCA-bridge metadata curator — generates description (+ synonyms for column-level) for a table or column based on RCA theme evidence.
when_to_pick: This skill is NOT picked by Stage-1; it is invoked by the RCA-bridge path inside _generate_proposals_for_ag when ENABLE_RCA_LEVER1_BRIDGE is true and an RCA theme has Lever-1 patches.
target_kind: base_table
target_min_count: 0
---
<role>
You are a metadata curator for a Genie SQL space. An RCA theme has identified that the following column/table needs metadata improvements based on a class of failed eval rows.
</role>

<context>
TARGET: {{ target }}
INTENT: {{ intent }}
EXPECTED (correct) objects: {{ expected_objects }}
ACTUAL (wrongly chosen) objects: {{ actual_objects }}
FAILURE CONTEXT (sanitized): {{ failure_context_json }}
EXISTING DESCRIPTION: {{ existing_description }}
EXISTING SYNONYMS: {{ existing_synonyms }}
</context>

<instructions>
Produce a JSON object with these keys:
- `description`: a 1-3 sentence description that strengthens the intended semantics and (if relevant) contrasts with the wrongly chosen objects. Do not contradict the existing description; extend it.
{{ synonyms_instruction_block }}
Return ONLY the JSON object, no prose.
</instructions>

<output_schema>
Respond with ONLY a JSON object. No analysis or commentary.

{"description": "<1-3 sentence description>"{{ synonyms_schema_field }}}
</output_schema>
```

The `{{ synonyms_instruction_block }}` and `{{ synonyms_schema_field }}` slots are populated conditionally by the caller — when `is_table_level=True` they are empty strings; when `is_table_level=False` (column target) they expand to the synonyms instruction and the `,"synonyms": ["term1", ...]` schema fragment respectively. This preserves the exact behaviour of the current inline f-string at `optimizer.py:12527-12532`.

- [ ] **Step 2: Verify the file loads via `_SKILL_LOADER.load_prompt`**

Run a one-line check:

```bash
cd packages/genie-space-optimizer
python -c "from genie_space_optimizer.common.skill_loader import _SKILL_LOADER; t = _SKILL_LOADER.load_prompt('lever-1-rca-bridge', expected_constant_name='LEVER_1_RCA_BRIDGE_PROMPT'); print(f'OK loaded {len(t)} chars')"
```

Expected: `OK loaded <N> chars` with N > 700.

- [ ] **Step 3: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-1-rca-bridge/SKILL.md
git commit -m "feat: extract lever-1 RCA-bridge prompt to SKILL.md (template-only, callsite migration in Task 8-9)"
```

### Task 8: Load the new prompt as a constant and add it to the registry

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py:4699` (after `LEVER_6_SQL_EXPRESSION_PROMPT`)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py:4460-4485` (the `LEVER_PROMPTS` dict)

- [ ] **Step 1: Write the failing test**

Add to `test_prompt_registry_inventory.py`:

```python
def test_lever_1_rca_bridge_prompt_is_loaded_and_registered():
    """The RCA-bridge prompt (formerly an inline f-string in
    _generate_lever1_rca_proposal) must be loadable from
    lever-1-rca-bridge/SKILL.md and registered in LEVER_PROMPTS.
    """
    assert hasattr(cfg, "LEVER_1_RCA_BRIDGE_PROMPT"), (
        "LEVER_1_RCA_BRIDGE_PROMPT constant is missing from common.config. "
        "Add LEVER_1_RCA_BRIDGE_PROMPT = _SKILL_LOADER.load_prompt("
        "'lever-1-rca-bridge', expected_constant_name="
        "'LEVER_1_RCA_BRIDGE_PROMPT')."
    )
    assert "lever_1_rca_bridge" in cfg.LEVER_PROMPTS, (
        "LEVER_1_RCA_BRIDGE_PROMPT is not in LEVER_PROMPTS. Add "
        "'lever_1_rca_bridge': LEVER_1_RCA_BRIDGE_PROMPT to LEVER_PROMPTS."
    )
    assert cfg.LEVER_PROMPTS["lever_1_rca_bridge"] is cfg.LEVER_1_RCA_BRIDGE_PROMPT
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_lever_1_rca_bridge_prompt_is_loaded_and_registered -v`
Expected: FAIL.

- [ ] **Step 3: Define the constant**

In `common/config.py`, immediately after `LEVER_6_SQL_EXPRESSION_PROMPT = _SKILL_LOADER.load_prompt(...)` at line 4699-4702:

```python
LEVER_1_RCA_BRIDGE_PROMPT = _SKILL_LOADER.load_prompt(
    "lever-1-rca-bridge",
    expected_constant_name="LEVER_1_RCA_BRIDGE_PROMPT",
)
```

- [ ] **Step 4: Add the registry entry**

In the `LEVER_PROMPTS` dict, add:

```python
    "lever_1_2_column": LEVER_1_2_COLUMN_PROMPT,
    "lever_1_rca_bridge": LEVER_1_RCA_BRIDGE_PROMPT,
    "lever_4_join_spec": LEVER_4_JOIN_SPEC_PROMPT,
```

- [ ] **Step 5: Run tests**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_lever_1_rca_bridge_prompt_is_loaded_and_registered -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/common/config.py packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py
git commit -m "feat: load LEVER_1_RCA_BRIDGE_PROMPT constant and register"
```

### Task 9: Replace the inline f-string at `optimizer.py:12511` with a templated render

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:12511-12534` and `:12617`
- Test: `packages/genie-space-optimizer/tests/optimization/test_lever1_rca_bridge_prompt.py` (new file)

- [ ] **Step 1: Write the failing test**

Create `tests/optimization/test_lever1_rca_bridge_prompt.py`:

```python
"""Tests for the lever-1 RCA-bridge prompt rendering.

Before Task 9: the prompt was an inline f-string in
``_generate_lever1_rca_proposal``. After Task 9: the prompt is a
templated render of ``LEVER_1_RCA_BRIDGE_PROMPT`` via
``format_mlflow_template``. These tests lock in the rendered byte shape
for table-level and column-level targets so a future template edit cannot
silently drift the LLM input.
"""
from __future__ import annotations

import json
import re
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization.optimizer import (
    _generate_lever1_rca_proposal,
)


def _stub_theme(rca_id: str = "RCA_T1", target_qids=("Q1", "Q2")):
    theme = MagicMock()
    theme.rca_id = rca_id
    theme.target_qids = target_qids
    return theme


def _capture_prompt(mock_traced_call):
    """Return the user-prompt string captured by the mocked _traced_llm_call."""
    call_args = mock_traced_call.call_args
    # signature: _traced_llm_call(w, system_msg, prompt, *, span_name=...)
    return call_args.args[2] if len(call_args.args) >= 3 else call_args.kwargs["prompt"]


@patch("genie_space_optimizer.optimization.optimizer._traced_llm_call")
def test_table_level_target_renders_template_slots(mock_call):
    mock_call.return_value = ('{"description": "fact table for sales"}', None)
    theme = _stub_theme()
    patch_dict = {
        "type": "update_description",
        "target": "catalog.schema.fact_sales",
        "intent": "mark as default sales asset",
        "expected_objects": [],
        "actual_objects": [],
    }
    metadata_snapshot = {
        "tables": [{"identifier": "catalog.schema.fact_sales", "description": "", "columns": []}],
        "_failure_clusters": [{"question_ids": ["Q1"], "failure_type": "plural_top_n_collapse"}],
    }
    result = _generate_lever1_rca_proposal(theme, patch_dict, metadata_snapshot)
    assert result is not None
    prompt = _capture_prompt(mock_call)
    assert "TARGET: table catalog.schema.fact_sales" in prompt
    assert "INTENT: mark as default sales asset" in prompt
    assert "EXPECTED (correct) objects: []" in prompt
    assert "ACTUAL (wrongly chosen) objects: []" in prompt
    assert "FAILURE CONTEXT (sanitized):" in prompt
    # Table-level: synonyms instruction MUST NOT appear
    assert "synonyms" not in prompt.lower(), (
        "Table-level RCA-bridge prompt accidentally emitted the synonyms "
        "instruction. The synonyms_instruction_block slot should be empty "
        "for table-level targets."
    )


@patch("genie_space_optimizer.optimization.optimizer._traced_llm_call")
def test_column_level_target_renders_template_slots(mock_call):
    mock_call.return_value = (
        '{"description": "store name", "synonyms": ["store"]}', None,
    )
    theme = _stub_theme()
    patch_dict = {
        "type": "update_column_description",
        "table": "catalog.schema.dim_store",
        "column": "store_name",
        "intent": "clarify store name semantics",
        "expected_objects": ["dim_store.store_name"],
        "actual_objects": ["dim_store.store_id"],
    }
    metadata_snapshot = {
        "tables": [{
            "identifier": "catalog.schema.dim_store",
            "columns": [{"name": "store_name", "description": "", "synonyms": []}],
        }],
        "_failure_clusters": [],
    }
    result = _generate_lever1_rca_proposal(theme, patch_dict, metadata_snapshot)
    assert result is not None
    prompt = _capture_prompt(mock_call)
    assert "TARGET: catalog.schema.dim_store.store_name" in prompt
    # Column-level: synonyms instruction MUST appear
    assert "synonyms" in prompt.lower()
    # And the JSON schema fragment MUST mention synonyms
    assert '"synonyms"' in prompt


def test_loaded_prompt_template_has_required_slots():
    """The SKILL.md template must declare every slot the caller fills."""
    from genie_space_optimizer.common.config import LEVER_1_RCA_BRIDGE_PROMPT
    required_slots = {
        "{{ target }}",
        "{{ intent }}",
        "{{ expected_objects }}",
        "{{ actual_objects }}",
        "{{ failure_context_json }}",
        "{{ existing_description }}",
        "{{ existing_synonyms }}",
        "{{ synonyms_instruction_block }}",
        "{{ synonyms_schema_field }}",
    }
    missing = sorted(s for s in required_slots if s not in LEVER_1_RCA_BRIDGE_PROMPT)
    assert not missing, (
        f"lever-1-rca-bridge/SKILL.md is missing slots that the caller "
        f"fills: {missing}. Either add the slots to the template or "
        f"update the caller to stop passing them."
    )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_lever1_rca_bridge_prompt.py -v`
Expected: FAIL (the helpers reference the new template path but the caller is still using the inline f-string).

- [ ] **Step 3: Replace the inline f-string in `_generate_lever1_rca_proposal`**

In `optimizer.py:12511-12534`, delete the inline f-string and replace with a template render. The new body for that block becomes:

```python
    # G9 (2026-05-17 prompt-registry-and-typed-io-hygiene plan, Task 9)
    # Inline f-string replaced with template render against
    # LEVER_1_RCA_BRIDGE_PROMPT (loaded from lever-1-rca-bridge/SKILL.md).
    # See test_lever1_rca_bridge_prompt.py for byte-shape locks.
    from genie_space_optimizer.common.config import LEVER_1_RCA_BRIDGE_PROMPT
    from genie_space_optimizer.optimization.evaluation import (
        _link_prompt_to_trace,
        format_mlflow_template,
    )
    if is_table_level:
        target = f"table {table}"
        synonyms_instruction_block = ""
        synonyms_schema_field = ""
    else:
        target = f"{table}.{column}"
        synonyms_instruction_block = (
            '- `synonyms`: a list of 2-5 lowercase NL phrases users might '
            'say that should route to this column. Derive from FAILURE '
            'CONTEXT phrases and EXPECTED/ACTUAL identifiers. Do not '
            'include phrases already in EXISTING SYNONYMS. Avoid SQL '
            'identifiers (snake_case, ALL_CAPS).'
        )
        synonyms_schema_field = ',"synonyms": ["term1", "term2"]'

    prompt = format_mlflow_template(
        LEVER_1_RCA_BRIDGE_PROMPT,
        target=target,
        intent=intent,
        expected_objects=expected_objects,
        actual_objects=actual_objects,
        failure_context_json=_json.dumps(afs_projections, default=str),
        existing_description=existing_description[:300],
        existing_synonyms=existing_synonyms,
        synonyms_instruction_block=synonyms_instruction_block,
        synonyms_schema_field=synonyms_schema_field,
    )
    _link_prompt_to_trace("lever_1_rca_bridge")
```

The downstream `_traced_llm_call(w, "You are a metadata curator.", prompt, span_name="lever1_rca_proposal")` invocation stays as-is.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_lever1_rca_bridge_prompt.py packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py -v`
Expected: all PASS. In particular, `test_no_inline_fstring_LLM_prompts_in_optimization_module` should now PASS because the only previously-flagged inline f-string is gone.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py packages/genie-space-optimizer/tests/optimization/test_lever1_rca_bridge_prompt.py
git commit -m "refactor: migrate _generate_lever1_rca_proposal inline f-string to LEVER_1_RCA_BRIDGE_PROMPT template"
```

---

## Phase D — L2 phantom constant cleanup

### Task 10: Convert `lever-2-mv-column-refinement/SKILL.md` to metadata-only

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-2-mv-column-refinement/SKILL.md`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/skills/CATALOGUE.md:10`
- Test: `packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py`

- [ ] **Step 1: Write the failing test**

Add to `test_prompt_registry_inventory.py`:

```python
def test_lever_2_skill_md_is_metadata_only():
    """L2's SKILL.md body was byte-identical dead code that nobody
    loaded — optimizer.py:8281 routes both lever=1 and lever=2 to
    LEVER_1_2_COLUMN_PROMPT (the L1 body). The L2 body's existence is a
    maintenance landmine: editing it does nothing, but it looks like it
    should.

    The fix is to make L2's SKILL.md metadata-only (frontmatter for
    Stage-1 discovery + a comment explaining the shared template), with
    no <role>/<context>/<instructions> body.

    There must be no `prompt_constant_name:` field with a value other
    than the literal string ``LEVER_1_2_COLUMN_PROMPT (shared with
    lever-1-table-column-description)``.
    """
    import pathlib
    l2_path = (
        pathlib.Path(__file__).resolve().parents[2]
        / "src" / "genie_space_optimizer" / "skills"
        / "lever-2-mv-column-refinement" / "SKILL.md"
    )
    body = l2_path.read_text()
    # No <role> tag → it's metadata-only.
    assert "<role>" not in body, (
        "L2 SKILL.md still contains a <role> body. The L2 body is dead "
        "code because optimizer.py:8281 routes lever=2 to "
        "LEVER_1_2_COLUMN_PROMPT (the L1 body). Delete the body and "
        "leave only frontmatter + the explanatory comment. See plan "
        "2026-05-17-prompt-registry-and-typed-io-hygiene.md Task 10."
    )
    # The frontmatter must explicitly document the shared template.
    assert (
        "shared with lever-1-table-column-description" in body
        or "shared with LEVER_1_2_COLUMN_PROMPT" in body
    ), (
        "L2 SKILL.md must contain an explanatory comment naming the "
        "shared template (LEVER_1_2_COLUMN_PROMPT). This prevents the "
        "next reader from re-introducing a dead L2 body."
    )


def test_no_phantom_LEVER_1_2_COLUMN_PROMPT_FOR_L2_references():
    """The constant LEVER_1_2_COLUMN_PROMPT_FOR_L2 was referenced by L2's
    SKILL.md frontmatter and CATALOGUE.md but never defined in code. It
    should not appear anywhere after Task 10.
    """
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[2] / "src"
    skills = src / "genie_space_optimizer" / "skills"
    bad = []
    for f in skills.rglob("*.md"):
        if "LEVER_1_2_COLUMN_PROMPT_FOR_L2" in f.read_text():
            bad.append(str(f.relative_to(src.parent.parent)))
    assert not bad, (
        f"Phantom constant LEVER_1_2_COLUMN_PROMPT_FOR_L2 still "
        f"referenced in: {bad}. It is never defined in code. Remove the "
        f"references."
    )
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_lever_2_skill_md_is_metadata_only packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_no_phantom_LEVER_1_2_COLUMN_PROMPT_FOR_L2_references -v`
Expected: BOTH FAIL.

- [ ] **Step 3: Convert L2 SKILL.md to metadata-only**

Replace the entire contents of `packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-2-mv-column-refinement/SKILL.md` with:

```markdown
---
skill_id: lever-2-mv-column-refinement
causal_or_non_causal: causal
pickable_by_stage_1: true
description: Refine metric-view column metadata — definitions, synonyms, important_filters.
when_to_pick: Failure stems from missing or weak metric-view metadata; the right MV column exists but Genie picks the wrong one or applies wrong filter semantics.
target_kind: metric_view
target_min_count: 0
---

<!--
This SKILL.md is metadata-only. The LLM template is shared with
lever-1-table-column-description (LEVER_1_2_COLUMN_PROMPT) — both
lever=1 and lever=2 route through the same template at
optimizer.py:8280-8281. Do not add a <role>/<context>/<instructions>
body here; it will be dead code that nobody loads.

If you need a separate L2 template in the future:
  1. Create LEVER_2_MV_COLUMN_PROMPT in common/config.py via
     _SKILL_LOADER.load_prompt("lever-2-mv-column-refinement",
     expected_constant_name="LEVER_2_MV_COLUMN_PROMPT").
  2. Split prompt_map[2] in _call_llm_for_proposal to point at the new
     constant.
  3. Register the new constant in LEVER_PROMPTS.
  4. Replace this comment with the new body.
-->
```

Note: the `prompt_constant_name:` frontmatter field is intentionally removed because no constant is loaded from this file. If the SKILL loader requires the field, fall back to a placeholder value `prompt_constant_name: shared:LEVER_1_2_COLUMN_PROMPT` and update the loader's contract documentation. Verify by running `grep -r "prompt_constant_name" packages/genie-space-optimizer/src/genie_space_optimizer/common/` to confirm the loader does not strictly enforce a one-to-one mapping.

- [ ] **Step 4: Fix `CATALOGUE.md`**

In `packages/genie-space-optimizer/src/genie_space_optimizer/skills/CATALOGUE.md:10`, replace the row:

```markdown
| `lever-2-mv-column-refinement` | `LEVER_1_2_COLUMN_PROMPT_FOR_L2` | causal | true | pass-through (N=3) |
```

with:

```markdown
| `lever-2-mv-column-refinement` | shared: `LEVER_1_2_COLUMN_PROMPT` | causal | true | pass-through (N=3) |
```

And add a new paragraph above the table:

```markdown
**Metadata-only SKILLs.** A SKILL.md row with `shared: <CONSTANT_NAME>` in
the second column means that skill participates in Stage-1 discovery
(its frontmatter selects it) but reuses the LLM template owned by
another skill. The body of the SKILL.md is intentionally empty — editing
it has no effect. To split a metadata-only SKILL into its own template,
see the comment inside the SKILL.md file.
```

- [ ] **Step 5: Run tests**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_lever_2_skill_md_is_metadata_only packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_no_phantom_LEVER_1_2_COLUMN_PROMPT_FOR_L2_references packages/genie-space-optimizer/tests/optimization/ -v -k "lever_2 or stage_2_l2"`
Expected: new tests PASS. Existing L2 tests still PASS (L2 LLM behaviour is unchanged — still uses `LEVER_1_2_COLUMN_PROMPT`).

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-2-mv-column-refinement/SKILL.md packages/genie-space-optimizer/src/genie_space_optimizer/skills/CATALOGUE.md packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py
git commit -m "fix: resolve L2 phantom constant — make L2 SKILL.md metadata-only, fix CATALOGUE"
```

---

## Phase E — Typed I/O infrastructure

### Task 11: Create `prompt_io.py` — Pydantic base + Databricks-aware schema flattener

**Files:**
- Create: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py`
- Create: `packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py`

- [ ] **Step 1: Write the failing test**

Create `tests/optimization/test_prompt_io_contracts.py`:

```python
"""Tests for the typed prompt I/O contract infrastructure."""
from __future__ import annotations

import pytest
from pydantic import Field

from genie_space_optimizer.optimization.prompt_io import (
    LLMOutputContract,
    build_response_format,
    validate_and_parse,
)


class _Example(LLMOutputContract):
    description: str
    synonyms: list[str] = Field(default_factory=list)


def test_build_response_format_returns_json_schema_payload():
    rf = build_response_format(_Example)
    assert rf["type"] == "json_schema"
    assert rf["json_schema"]["name"] == "_Example"
    schema = rf["json_schema"]["schema"]
    assert schema["type"] == "object"
    assert "description" in schema["properties"]
    assert "synonyms" in schema["properties"]


def test_build_response_format_strips_unsupported_keywords():
    """Databricks Foundation Model APIs reject pattern, anyOf, oneOf,
    allOf, prefixItems, $ref, maxProperties, minProperties, maxLength.
    The flattener must strip them.
    """
    rf = build_response_format(_Example)
    serialized = str(rf)
    for forbidden in ["pattern", "anyOf", "oneOf", "allOf", "prefixItems", "$ref", "maxLength"]:
        assert forbidden not in serialized, (
            f"build_response_format leaked unsupported JSON-schema "
            f"keyword {forbidden!r}; Databricks will reject the call. "
            f"Flattened schema: {rf}"
        )


def test_build_response_format_marks_strict_true():
    rf = build_response_format(_Example)
    assert rf["json_schema"]["strict"] is True, (
        "strict=True is required for the Databricks JSON-schema mode to "
        "actually enforce the schema (instead of treating it as a hint)."
    )


def test_validate_and_parse_returns_model_on_valid_json():
    parsed = validate_and_parse(
        '{"description": "a fact table", "synonyms": ["sales"]}', _Example,
    )
    assert isinstance(parsed, _Example)
    assert parsed.description == "a fact table"
    assert parsed.synonyms == ["sales"]


def test_validate_and_parse_extracts_json_from_code_fence():
    """When the model wraps the JSON in ```json ... ``` the extractor
    must still find the object."""
    parsed = validate_and_parse(
        '```json\n{"description": "x"}\n```', _Example,
    )
    assert parsed.description == "x"


def test_validate_and_parse_raises_on_missing_required_field():
    with pytest.raises(ValueError):
        validate_and_parse('{"synonyms": ["x"]}', _Example)


def test_validate_and_parse_raises_on_non_json():
    with pytest.raises(ValueError):
        validate_and_parse("not json at all", _Example)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py -v`
Expected: FAIL (module does not exist).

- [ ] **Step 3: Create `prompt_io.py`**

Create `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py`:

```python
"""Typed I/O contracts for LLM prompts.

Every active prompt has a Pydantic ``LLMOutputContract`` subclass that
declares the expected JSON shape of the model's response. The shape is
fed to Databricks Foundation Model APIs as ``response_format={"type":
"json_schema"}`` to ENFORCE the contract server-side, and the response
text is then re-parsed locally as a defense-in-depth check.

Databricks Foundation Model APIs support a SUBSET of JSON Schema:
  * Supported: type, properties, required, items (single type), enum,
    description.
  * Not supported: pattern, anyOf, oneOf, allOf, prefixItems, $ref,
    maxProperties, minProperties, maxLength.
  * Special case: ``[type, "null"]`` for nullable fields is OK.

Pydantic v2's default ``model_json_schema()`` emits some of these for
Optional/Union types and nested models. ``build_response_format()``
flattens / strips them so the payload is Databricks-safe.
"""
from __future__ import annotations

import json
import re
from typing import Any, TypeVar

from pydantic import BaseModel

_T = TypeVar("_T", bound="LLMOutputContract")


class LLMOutputContract(BaseModel):
    """Base class for every LLM output contract.

    Subclasses MUST declare every field that the prompt's
    ``<output_schema>`` block in ``SKILL.md`` documents. The Pydantic
    schema is the source of truth — if the SKILL.md and the contract
    disagree, the contract wins (and a test under
    ``test_prompt_io_contracts.py`` should pin the SKILL.md to match).
    """

    model_config = {
        "extra": "forbid",  # reject unexpected fields server-side too
        "str_strip_whitespace": True,
    }


_UNSUPPORTED_KEYWORDS: frozenset[str] = frozenset({
    "pattern",
    "anyOf",
    "oneOf",
    "allOf",
    "prefixItems",
    "$ref",
    "maxProperties",
    "minProperties",
    "maxLength",
    "minLength",
    "maxItems",
    "minItems",
    "format",  # date-time etc. — Databricks may reject specific formats
})


def _strip_unsupported(node: Any) -> Any:
    """Recursively strip Databricks-unsupported JSON Schema keywords."""
    if isinstance(node, dict):
        return {
            k: _strip_unsupported(v)
            for k, v in node.items()
            if k not in _UNSUPPORTED_KEYWORDS
        }
    if isinstance(node, list):
        return [_strip_unsupported(item) for item in node]
    return node


def build_response_format(model_cls: type[BaseModel]) -> dict[str, Any]:
    """Build a Databricks-safe ``response_format`` payload from a
    Pydantic model.

    Returns the dict you pass directly to
    ``openai.chat.completions.create(..., response_format=...)``.
    """
    schema = model_cls.model_json_schema()
    # Pydantic emits $defs for nested models; inline them shallowly by
    # walking refs once. For Stage 1+ output shapes we keep nested
    # models simple (no recursive refs), so a single pass suffices.
    defs = schema.pop("$defs", {})
    def _inline(node: Any) -> Any:
        if isinstance(node, dict):
            ref = node.get("$ref")
            if isinstance(ref, str) and ref.startswith("#/$defs/"):
                target = defs.get(ref.removeprefix("#/$defs/"))
                if target is not None:
                    return _inline(target)
            return {k: _inline(v) for k, v in node.items() if k != "$ref"}
        if isinstance(node, list):
            return [_inline(item) for item in node]
        return node
    schema = _inline(schema)
    schema = _strip_unsupported(schema)
    # Databricks requires top-level "object" type.
    schema.setdefault("type", "object")
    return {
        "type": "json_schema",
        "json_schema": {
            "name": model_cls.__name__,
            "schema": schema,
            "strict": True,
        },
    }


_JSON_BRACE_RE = re.compile(r"\{[\s\S]*\}")
_CODE_FENCE_RE = re.compile(r"```(?:json)?\s*([\s\S]*?)```")


def _extract_json_text(raw: str) -> str:
    """Return the first JSON-object substring in ``raw``.

    Handles code-fence wrappers (```json ... ```) and stray prose.
    """
    fence = _CODE_FENCE_RE.search(raw)
    if fence:
        raw = fence.group(1)
    match = _JSON_BRACE_RE.search(raw)
    if not match:
        raise ValueError(f"no JSON object found in response (first 200 chars: {raw[:200]!r})")
    return match.group(0)


def validate_and_parse(raw_text: str, model_cls: type[_T]) -> _T:
    """Parse ``raw_text`` as JSON, validate against ``model_cls``.

    Raises ``ValueError`` on any failure. The caller can pass this
    function as ``response_validator=`` to ``_traced_llm_call`` so
    malformed responses retry with exponential backoff before
    propagating.
    """
    json_text = _extract_json_text(raw_text)
    try:
        return model_cls.model_validate_json(json_text)
    except Exception as exc:
        raise ValueError(
            f"LLM response did not match {model_cls.__name__} contract: {exc}"
        ) from exc
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py -v`
Expected: ALL 7 tests PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py
git commit -m "feat: add prompt_io module — LLMOutputContract base, Databricks-safe response_format builder"
```

### Task 12: Extend `_traced_llm_call()` with `response_format` + `response_model` kwargs

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:189-220`
- Test: `packages/genie-space-optimizer/tests/optimization/test_traced_llm_call_response_model.py` (new)

- [ ] **Step 1: Write the failing test**

Create `tests/optimization/test_traced_llm_call_response_model.py`:

```python
"""Tests for the response_model extension of _traced_llm_call."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from genie_space_optimizer.optimization.optimizer import _traced_llm_call
from genie_space_optimizer.optimization.prompt_io import LLMOutputContract


class _Example(LLMOutputContract):
    description: str
    synonyms: list[str] = []


@patch("genie_space_optimizer.optimization.optimizer._get_openai_client")
def test_response_model_sets_response_format_on_openai_call(mock_client_factory):
    """When response_model is passed, response_format must be injected
    into the OpenAI call kwargs."""
    fake_client = MagicMock()
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=MagicMock(content='{"description": "x", "synonyms": []}'))]
    fake_client.chat.completions.create.return_value = fake_resp
    mock_client_factory.return_value = fake_client

    text, _ = _traced_llm_call(
        None, "sys", "prompt", span_name="t",
        response_model=_Example,
    )
    create_kwargs = fake_client.chat.completions.create.call_args.kwargs
    assert "response_format" in create_kwargs
    rf = create_kwargs["response_format"]
    assert rf["type"] == "json_schema"
    assert rf["json_schema"]["name"] == "_Example"
    assert rf["json_schema"]["strict"] is True


@patch("genie_space_optimizer.optimization.optimizer._get_openai_client")
def test_response_model_retries_on_invalid_json_response(mock_client_factory):
    """If the LLM somehow returns malformed JSON despite response_format,
    the response_validator path must retry."""
    fake_client = MagicMock()
    bad_resp = MagicMock()
    bad_resp.choices = [MagicMock(message=MagicMock(content="this is not json"))]
    good_resp = MagicMock()
    good_resp.choices = [MagicMock(message=MagicMock(content='{"description": "x"}'))]
    fake_client.chat.completions.create.side_effect = [bad_resp, good_resp]
    mock_client_factory.return_value = fake_client

    text, _ = _traced_llm_call(
        None, "sys", "prompt", span_name="t",
        response_model=_Example, max_retries=3,
    )
    assert fake_client.chat.completions.create.call_count == 2
    assert "description" in text


@patch("genie_space_optimizer.optimization.optimizer._get_openai_client")
def test_no_response_model_preserves_legacy_behaviour(mock_client_factory):
    """When response_model is None (the default), no response_format is
    sent — preserving every existing callsite."""
    fake_client = MagicMock()
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=MagicMock(content="plain text"))]
    fake_client.chat.completions.create.return_value = fake_resp
    mock_client_factory.return_value = fake_client

    text, _ = _traced_llm_call(None, "sys", "prompt", span_name="t")
    create_kwargs = fake_client.chat.completions.create.call_args.kwargs
    assert "response_format" not in create_kwargs
    assert text == "plain text"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_traced_llm_call_response_model.py -v`
Expected: 2 FAIL (`response_model` is not yet a parameter), 1 PASS (`test_no_response_model_preserves_legacy_behaviour` passes because the kwarg doesn't exist so the call works without `response_format`).

- [ ] **Step 3: Extend `_traced_llm_call`**

In `optimizer.py:189-220`, modify the signature and body:

```python
def _traced_llm_call(
    w: WorkspaceClient | None,
    system_msg: str,
    prompt: str,
    *,
    span_name: str,
    max_retries: int = LLM_MAX_RETRIES,
    temperature: float = LLM_TEMPERATURE,
    max_tokens: int | None = None,
    response_validator: Callable[[str], Any] | None = None,
    response_format: dict[str, Any] | None = None,
    response_model: type | None = None,
) -> tuple[str, Any]:
    """Execute an LLM call via the OpenAI SDK with automatic MLflow tracing.

    ... (existing docstring) ...

    ``response_model`` (optional): a Pydantic ``LLMOutputContract``
    subclass. When set, ``build_response_format(response_model)`` is
    used to populate ``response_format`` (server-side enforcement), and
    ``validate_and_parse(text, response_model)`` is used as the
    ``response_validator`` (defense-in-depth + auto-retry on malformed
    responses).

    ``response_format`` (optional): direct override of the
    ``response_format`` kwarg passed to ``client.chat.completions.create``.
    If both ``response_model`` and ``response_format`` are passed,
    ``response_format`` wins (the caller has explicitly opted out of
    auto-construction).
    """
    import time

    import mlflow
    from mlflow.entities import SpanEvent, SpanType

    # G12 (2026-05-17 prompt-registry-and-typed-io-hygiene plan, Task 12)
    # When response_model is set, derive response_format + response_validator
    # from it so callers get server-side schema enforcement AND client-side
    # validation + retry for free.
    if response_model is not None:
        from genie_space_optimizer.optimization.prompt_io import (
            build_response_format,
            validate_and_parse,
        )
        if response_format is None:
            response_format = build_response_format(response_model)
        if response_validator is None:
            response_validator = lambda txt: validate_and_parse(txt, response_model)

    with mlflow.start_span(name=span_name, span_type=SpanType.CHAIN) as span:
        span.set_inputs({
            "model": LLM_ENDPOINT,
            "temperature": temperature,
            "prompt_chars": len(prompt),
            "response_model": response_model.__name__ if response_model else None,
        })

        client = _get_openai_client(w)
        text = ""
        last_response_text: str = ""
        last_err: Exception | None = None

        for attempt in range(max_retries):
            try:
                messages: list[dict[str, str]] = []
                if system_msg and system_msg.strip():
                    messages.append({"role": "system", "content": system_msg})
                messages.append({"role": "user", "content": prompt})
                call_kwargs: dict[str, Any] = {
                    "model": LLM_ENDPOINT,
                    "messages": messages,
                    "temperature": temperature,
                }
                if max_tokens is not None:
                    call_kwargs["max_tokens"] = max_tokens
                if response_format is not None:
                    call_kwargs["response_format"] = response_format

                response = client.chat.completions.create(**call_kwargs)
                # ... (rest of existing body unchanged) ...
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_traced_llm_call_response_model.py -v`
Expected: ALL 3 PASS.
Run: `pytest packages/genie-space-optimizer/tests/optimization/ -k "traced or llm_call" -v`
Expected: all existing tests still PASS (the kwargs are keyword-only and default to None).

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py packages/genie-space-optimizer/tests/optimization/test_traced_llm_call_response_model.py
git commit -m "feat: _traced_llm_call accepts response_model and auto-wires response_format"
```

### Task 13: Extend `_call_llm_for_proposal()` to accept `response_model`

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:8229-8450` (the `_call_llm_for_proposal` function and its inner LLM call)

- [ ] **Step 1: Find the LLM call inside `_call_llm_for_proposal`**

The function definition starts at `optimizer.py:8229`. Around line 8424 it invokes `_call_llm_openai(w, messages=...)`. The fix is to route through `_traced_llm_call` (which now supports `response_model`) when a `response_model` is passed.

- [ ] **Step 2: Write the failing test**

Add to `tests/optimization/test_traced_llm_call_response_model.py`:

```python
@patch("genie_space_optimizer.optimization.optimizer._get_openai_client")
def test_call_llm_for_proposal_uses_response_model_when_passed(mock_client_factory):
    """_call_llm_for_proposal must route response_model through to the
    LLM call so Stage-2 callsites can opt into structured outputs."""
    from genie_space_optimizer.optimization.optimizer import _call_llm_for_proposal

    fake_client = MagicMock()
    fake_resp = MagicMock()
    fake_resp.choices = [MagicMock(message=MagicMock(content='{"changes": [], "table_changes": [], "rationale": "x"}'))]
    fake_client.chat.completions.create.return_value = fake_resp
    mock_client_factory.return_value = fake_client

    class _StageProposal(LLMOutputContract):
        changes: list = []
        table_changes: list = []
        rationale: str

    cluster = {"asi_blame_set": ["t.c"], "asi_failure_type": "wrong_column"}
    metadata_snapshot = {"tables": [], "data_sources": {}}
    _call_llm_for_proposal(
        cluster, metadata_snapshot, "update_column_description", lever=1,
        response_model=_StageProposal,
    )
    create_kwargs = fake_client.chat.completions.create.call_args.kwargs
    assert "response_format" in create_kwargs
    assert create_kwargs["response_format"]["json_schema"]["name"] == "_StageProposal"
```

- [ ] **Step 3: Run test to verify it fails**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_traced_llm_call_response_model.py::test_call_llm_for_proposal_uses_response_model_when_passed -v`
Expected: FAIL (`response_model` is not yet a parameter of `_call_llm_for_proposal`).

- [ ] **Step 4: Extend `_call_llm_for_proposal`**

In `optimizer.py:8229`, change the function signature to add the kwarg:

```python
def _call_llm_for_proposal(
    cluster: dict,
    metadata_snapshot: dict,
    patch_type: str,
    lever: int,
    w: WorkspaceClient | None = None,
    *,
    raw_evidence: tuple[dict, ...] = (),
    response_model: type | None = None,
) -> dict:
```

Then in the LLM-call block (around `optimizer.py:8422-8430`), replace the direct `_call_llm_openai(...)` invocation with `_traced_llm_call(...)` and pass `response_model`:

```python
    text = ""
    for attempt in range(LLM_MAX_RETRIES):
        try:
            text, _response = _traced_llm_call(
                w, _proposal_system_msg, prompt,
                span_name=f"call_llm_for_proposal_lever_{lever}",
                response_model=response_model,
            )
            break
        except Exception:
            if attempt == LLM_MAX_RETRIES - 1:
                raise
            time.sleep(2**attempt)
```

Note: this is a behaviour change in addition to the additive one — the legacy path used `_call_llm_openai` directly; the new path uses `_traced_llm_call`. Verify the migration is clean by checking that `_call_llm_openai` returns the same `(text, response)` tuple shape as `_traced_llm_call` and that downstream code only consumes `text`. If `_call_llm_openai` has any unique semantics (e.g. different retry behaviour) that the existing callers depend on, expose a `legacy_openai=True` opt-out and migrate to the new path only when `response_model` is None — preserving the legacy path until Task 14+ migrate per-prompt.

- [ ] **Step 5: Run tests**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_traced_llm_call_response_model.py packages/genie-space-optimizer/tests/optimization/test_optimizer.py -v -k "proposal"`
Expected: new test PASS; existing proposal tests PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py packages/genie-space-optimizer/tests/optimization/test_traced_llm_call_response_model.py
git commit -m "feat: _call_llm_for_proposal accepts response_model; routes through _traced_llm_call"
```

---

## Phase F — Pydantic output models for active prompts

### Task 14: Stage-1 discovery output contract (`Stage1DiscoveryOutput`)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py` (append the model)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:_call_llm_for_stage_1_discovery`
- Test: `packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py` (append)

- [ ] **Step 1: Read the SKILL.md output schema for stage-1-discovery**

The output schema lives at the bottom of `packages/genie-space-optimizer/src/genie_space_optimizer/skills/stage-1-discovery/SKILL.md`. The expected shape (per the existing template) is:

```json
{
  "applicable_skills": [
    {
      "skill_id": "lever-6-sql-expression",
      "target_objects": ["catalog.schema.table"],
      "expected_impact_qids": ["Q42"],
      "evidence_refs": ["H001"],
      "why": "wrong_aggregation needs a new reusable expression",
      "priority": 1
    }
  ],
  "reasoning": "..."
}
```

The exact shape is what the test will lock in.

- [ ] **Step 2: Write the failing test**

Append to `tests/optimization/test_prompt_io_contracts.py`:

```python
from genie_space_optimizer.optimization.prompt_io import (
    Stage1DiscoveryOutput,
    Stage1SkillPick,
)


def test_stage_1_discovery_output_parses_canonical_response():
    raw = """
    {
      "applicable_skills": [
        {
          "skill_id": "lever-6-sql-expression",
          "target_objects": ["catalog.schema.fact_sales"],
          "expected_impact_qids": ["Q1", "Q2"],
          "evidence_refs": ["H001"],
          "why": "wrong_aggregation",
          "priority": 1
        }
      ],
      "reasoning": "all clusters point at fact_sales"
    }
    """
    parsed = validate_and_parse(raw, Stage1DiscoveryOutput)
    assert len(parsed.applicable_skills) == 1
    pick = parsed.applicable_skills[0]
    assert pick.skill_id == "lever-6-sql-expression"
    assert pick.target_objects == ["catalog.schema.fact_sales"]
    assert pick.priority == 1


def test_stage_1_discovery_output_rejects_unknown_priority():
    """priority must be in {1,2,3}; anything else fails validation."""
    raw = """{"applicable_skills": [{"skill_id": "x", "target_objects": [], "expected_impact_qids": [], "evidence_refs": [], "why": "x", "priority": 99}], "reasoning": ""}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, Stage1DiscoveryOutput)


def test_stage_1_discovery_output_allows_empty_skills_list():
    """Stage-1 may emit an empty list when no skill applies (the
    no-fit branch)."""
    raw = '{"applicable_skills": [], "reasoning": "no actionable target"}'
    parsed = validate_and_parse(raw, Stage1DiscoveryOutput)
    assert parsed.applicable_skills == []
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py -v -k "stage_1"`
Expected: FAIL (the model is not defined yet).

- [ ] **Step 4: Define the model**

Append to `prompt_io.py`:

```python
from typing import Literal

from pydantic import Field


class Stage1SkillPick(LLMOutputContract):
    """One pick in the Stage-1 discovery output."""

    skill_id: str = Field(
        description="One of the skill_ids declared as pickable_by_stage_1=true",
    )
    target_objects: list[str] = Field(
        default_factory=list,
        description="Fully-qualified table/column/MV/function identifiers",
    )
    expected_impact_qids: list[str] = Field(
        default_factory=list,
        description="Question IDs from the cluster_briefs Question IDs: line",
    )
    evidence_refs: list[str] = Field(
        default_factory=list,
        description="Cluster IDs (e.g. H001) the pick was derived from",
    )
    why: str = Field(description="One-line routing rationale")
    priority: Literal[1, 2, 3] = Field(
        description="1 = blocker, 2 = major, 3 = preventive",
    )


class Stage1DiscoveryOutput(LLMOutputContract):
    """Top-level Stage-1 discovery output."""

    applicable_skills: list[Stage1SkillPick] = Field(default_factory=list)
    reasoning: str = Field(default="")
```

- [ ] **Step 5: Wire it at the Stage-1 callsite**

In `optimizer.py:_call_llm_for_stage_1_discovery` (around line 10883-10928), pass `response_model=Stage1DiscoveryOutput` to the `_traced_llm_call` (or `_call_llm_openai`) invocation:

```python
from genie_space_optimizer.optimization.prompt_io import Stage1DiscoveryOutput

text, _response = _traced_llm_call(
    w, system_msg, prompt,
    span_name="stage_1_discovery_llm",
    max_tokens=STAGE_1_DISCOVERY_MAX_TOKENS,
    response_model=Stage1DiscoveryOutput,
)
```

- [ ] **Step 6: Run tests**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py packages/genie-space-optimizer/tests/optimization/ -v -k "stage_1"`
Expected: new tests PASS; existing Stage-1 tests PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py
git commit -m "feat: Stage1DiscoveryOutput Pydantic contract + response_format= at Stage-1 callsite"
```

### Task 15: Lever-6 SQL-expression output contract (`Lever6SqlExpressionOutput`)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:_generate_lever6_proposal` (around line 13000)
- Test: `packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py`

- [ ] **Step 1: Read the Lever-6 output schema**

The output schema is in `packages/genie-space-optimizer/src/genie_space_optimizer/skills/lever-6-sql-expression/SKILL.md` at the bottom (`<output_schema>` block). The shape is roughly:

```json
{
  "proposals": [
    {
      "snippet_type": "filter|measure|expression",
      "name": "snippet_name",
      "value": "SQL fragment",
      "target_table": "catalog.schema.table",
      "rationale": "...",
      "affected_questions": ["Q1"]
    }
  ]
}
```

Verify the exact field names and required-vs-optional split against the SKILL.md before writing the model — the SKILL.md is the source of truth.

- [ ] **Step 2: Write the failing test**

Append:

```python
from genie_space_optimizer.optimization.prompt_io import (
    Lever6Proposal,
    Lever6SqlExpressionOutput,
)


def test_lever_6_output_parses_canonical_proposal():
    raw = """
    {
      "proposals": [
        {
          "snippet_type": "expression",
          "name": "top_n_by_rank",
          "value": "ROW_NUMBER() OVER (ORDER BY x DESC)",
          "target_table": "catalog.schema.fact_sales",
          "rationale": "wrong_aggregation",
          "affected_questions": ["Q1"]
        }
      ]
    }
    """
    parsed = validate_and_parse(raw, Lever6SqlExpressionOutput)
    assert len(parsed.proposals) == 1
    p = parsed.proposals[0]
    assert p.snippet_type == "expression"


def test_lever_6_output_rejects_invalid_snippet_type():
    raw = """{"proposals": [{"snippet_type": "join_spec", "name": "x", "value": "y", "target_table": "t", "rationale": "z", "affected_questions": []}]}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever6SqlExpressionOutput)
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py -v -k "lever_6"`
Expected: FAIL.

- [ ] **Step 4: Define the model**

Append to `prompt_io.py`:

```python
class Lever6Proposal(LLMOutputContract):
    snippet_type: Literal["filter", "measure", "expression"]
    name: str
    value: str
    target_table: str
    rationale: str
    affected_questions: list[str] = Field(default_factory=list)


class Lever6SqlExpressionOutput(LLMOutputContract):
    proposals: list[Lever6Proposal] = Field(default_factory=list)
```

- [ ] **Step 5: Wire it at the Lever-6 callsite**

In `optimizer.py:13000-13003`:

```python
from genie_space_optimizer.optimization.prompt_io import Lever6SqlExpressionOutput

raw_text, _ = _traced_llm_call(
    w, "You are a SQL expression expert.", prompt,
    span_name="lever6_llm",
    max_tokens=LEVER_6_MAX_TOKENS,
    response_model=Lever6SqlExpressionOutput,
)
```

- [ ] **Step 6: Run tests**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py packages/genie-space-optimizer/tests/optimization/test_lever6_prompt.py -v`
Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py
git commit -m "feat: Lever6SqlExpressionOutput Pydantic contract + response_format= at lever-6 callsite"
```

### Task 16: Lever-1 RCA-bridge output contract (`Lever1RcaBridgeOutput`)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py:_generate_lever1_rca_proposal` (around line 12617)
- Test: `packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py`

- [ ] **Step 1: Write the failing test**

Append:

```python
from genie_space_optimizer.optimization.prompt_io import Lever1RcaBridgeOutput


def test_lever_1_rca_bridge_table_level_parses_description_only():
    raw = '{"description": "fact table for sales transactions"}'
    parsed = validate_and_parse(raw, Lever1RcaBridgeOutput)
    assert parsed.description == "fact table for sales transactions"
    assert parsed.synonyms == []  # default for table-level


def test_lever_1_rca_bridge_column_level_parses_synonyms():
    raw = '{"description": "store name", "synonyms": ["store", "outlet"]}'
    parsed = validate_and_parse(raw, Lever1RcaBridgeOutput)
    assert parsed.description == "store name"
    assert parsed.synonyms == ["store", "outlet"]


def test_lever_1_rca_bridge_rejects_extra_fields():
    """extra='forbid' on LLMOutputContract prevents the model from
    inventing a "hash" or "debug" field that we'd then have to filter
    out downstream."""
    raw = '{"description": "x", "debug_hash": "abc"}'
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever1RcaBridgeOutput)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py -v -k "rca_bridge"`
Expected: FAIL.

- [ ] **Step 3: Define the model**

Append to `prompt_io.py`:

```python
class Lever1RcaBridgeOutput(LLMOutputContract):
    """Output contract for the lever-1-rca-bridge prompt.

    Table-level targets emit only ``description``; column-level targets
    also emit ``synonyms``. The optionality of ``synonyms`` is enforced
    by the caller (the prompt template conditionally renders the
    synonyms instruction), not by this model.
    """

    description: str = Field(description="1-3 sentence description")
    synonyms: list[str] = Field(
        default_factory=list,
        description="Lowercase NL phrases (column-level only)",
    )
```

- [ ] **Step 4: Wire it at the RCA-bridge callsite**

In `optimizer.py:12617`:

```python
from genie_space_optimizer.optimization.prompt_io import Lever1RcaBridgeOutput

raw_text, _ = _traced_llm_call(
    w, "You are a metadata curator.", prompt,
    span_name="lever1_rca_proposal",
    response_model=Lever1RcaBridgeOutput,
)
```

- [ ] **Step 5: Run tests**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py packages/genie-space-optimizer/tests/optimization/test_lever1_rca_bridge_prompt.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py
git commit -m "feat: Lever1RcaBridgeOutput Pydantic contract + response_format= at RCA-bridge callsite"
```

### Task 17: Lever-1/2 column-description output contract (`Lever12ColumnOutput`)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/three_stage_pipeline.py:_stage_2_l1` and `:_stage_2_l2`
- Test: `packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py`

- [ ] **Step 1: Read the output schema from `lever-1-table-column-description/SKILL.md`**

From the SKILL.md (see Phase B inventory):

```json
{
  "changes": [
    {
      "table": "<fqn>",
      "column": "<col>",
      "entity_type": "column_dim|column_measure|column_key",
      "sections": {"definition": "...", "synonyms": "term1, term2"}
    }
  ],
  "table_changes": [
    {
      "table": "<fqn>",
      "sections": {"purpose": "...", "best_for": "...", "grain": "..."}
    }
  ],
  "rationale": "..."
}
```

- [ ] **Step 2: Write the failing test**

Append:

```python
from genie_space_optimizer.optimization.prompt_io import (
    Lever12ChangeEntry,
    Lever12ColumnOutput,
    Lever12TableChangeEntry,
)


def test_lever_1_2_column_output_parses_canonical_response():
    raw = """
    {
      "changes": [
        {
          "table": "catalog.schema.dim_store",
          "column": "location_id",
          "entity_type": "column_key",
          "sections": {"synonyms": "store id, store number"}
        }
      ],
      "table_changes": [
        {"table": "catalog.schema.dim_store", "sections": {"purpose": "store dimension"}}
      ],
      "rationale": "store_id vs location_id ambiguity"
    }
    """
    parsed = validate_and_parse(raw, Lever12ColumnOutput)
    assert len(parsed.changes) == 1
    assert parsed.changes[0].entity_type == "column_key"
    assert parsed.changes[0].sections["synonyms"] == "store id, store number"


def test_lever_1_2_column_output_rejects_invalid_entity_type():
    raw = """{"changes": [{"table": "t", "column": "c", "entity_type": "unknown_kind", "sections": {}}], "table_changes": [], "rationale": ""}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever12ColumnOutput)


def test_lever_1_2_column_output_allows_empty_arrays():
    """The model may legitimately emit no changes (e.g. when blame_set
    has no actionable target). Empty arrays must parse."""
    raw = '{"changes": [], "table_changes": [], "rationale": "no actionable changes"}'
    parsed = validate_and_parse(raw, Lever12ColumnOutput)
    assert parsed.changes == []
    assert parsed.table_changes == []
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py -v -k "lever_1_2"`
Expected: FAIL.

- [ ] **Step 4: Define the model**

Append to `prompt_io.py`:

```python
class Lever12ChangeEntry(LLMOutputContract):
    table: str
    column: str
    entity_type: Literal["column_dim", "column_measure", "column_key"]
    sections: dict[str, str] = Field(default_factory=dict)


class Lever12TableChangeEntry(LLMOutputContract):
    table: str
    sections: dict[str, str] = Field(default_factory=dict)


class Lever12ColumnOutput(LLMOutputContract):
    changes: list[Lever12ChangeEntry] = Field(default_factory=list)
    table_changes: list[Lever12TableChangeEntry] = Field(default_factory=list)
    rationale: str = Field(default="")
```

Note: the `sections` field is intentionally `dict[str, str]` (not a nested model with named keys) because the valid section keys differ for table vs column (see SKILL.md instructions). Stricter validation would require splitting into `Lever12ColumnSections` and `Lever12TableSections` Pydantic models with optional fields per section name — that is a follow-up if section-key correctness becomes an issue in production.

- [ ] **Step 5: Wire it at the Lever-1 and Lever-2 callsites**

The Stage-2 adapters live in `three_stage_pipeline.py:_stage_2_l1` (line 365) and `:_stage_2_l2` (line 406). Each calls `_call_llm_for_proposal(...)`. Pass `response_model=Lever12ColumnOutput`:

```python
# In _stage_2_l1 (line 383-388):
from genie_space_optimizer.optimization.prompt_io import Lever12ColumnOutput

p = _call_llm_for_proposal(
    cluster_afs, bundle.metadata_snapshot,
    patch_type, lever=1, w=w,
    raw_evidence=bundle.raw_evidence,
    response_model=Lever12ColumnOutput,
)
```

And the same in `_stage_2_l2` (line 419-424).

- [ ] **Step 6: Run tests**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py packages/genie-space-optimizer/tests/optimization/test_three_stage_pipeline.py -v -k "lever_1 or lever_2"`
Expected: new tests PASS; existing tests PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/three_stage_pipeline.py packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py
git commit -m "feat: Lever12ColumnOutput Pydantic contract + response_format= at L1/L2 callsites"
```

### Task 18: Strategist output contracts (`StrategistTriageOutput`, `StrategistDetailOutput`, `AdaptiveStrategistOutput`)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py` at the three strategist callsites (line 11299, 11454, and the adaptive strategist callsite around 10049 / 10665)
- Test: `packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py`

- [ ] **Step 1: Read each strategist prompt's `<output_schema>`**

The three strategist prompts are inline in `common/config.py`:
- `STRATEGIST_TRIAGE_PROMPT` (line 2968) — its output is `{"action_groups": [{"ag_id": "...", "primary_cluster_id": "...", "source_cluster_ids": [...], "affected_questions": [...], "rca_kind": "...", "severity": "...", "rationale": "..."}, ...]}`. Read the actual `<output_schema>` block to confirm.
- `STRATEGIST_DETAIL_PROMPT` (line 3106) — its output is per-AG detail like `{"ag_id": "...", "selected_levers": [...], "rationale": "...", "expected_outcome": "..."}`. Confirm against the file.
- `ADAPTIVE_STRATEGIST_PROMPT` (loaded from `adaptive-strategist/SKILL.md`) — its output is a fallback skill-picks list, similar to `Stage1DiscoveryOutput`. Confirm against the file.

The exact field names go into the models below; pin them against the live prompt text rather than inventing.

- [ ] **Step 2: Write the failing tests (one per model)**

Append to `tests/optimization/test_prompt_io_contracts.py`. For each model, write:
1. A canonical-parse test (a representative valid response parses).
2. A reject-invalid-enum test (any Literal field rejects bad values).
3. An allow-empty-arrays test (the model may legitimately emit empty lists).

Use the structure from Tasks 14-17. Example for `StrategistTriageOutput`:

```python
from genie_space_optimizer.optimization.prompt_io import (
    StrategistActionGroup,
    StrategistTriageOutput,
)


def test_strategist_triage_output_parses_canonical_response():
    raw = """
    {
      "action_groups": [
        {
          "ag_id": "AG_PIPELINE",
          "primary_cluster_id": "H001",
          "source_cluster_ids": ["H001", "H002"],
          "affected_questions": ["Q1", "Q2"],
          "rca_kind": "missing_filter",
          "severity": "major",
          "rationale": "shared filter defect"
        }
      ]
    }
    """
    parsed = validate_and_parse(raw, StrategistTriageOutput)
    assert len(parsed.action_groups) == 1
    ag = parsed.action_groups[0]
    assert ag.ag_id == "AG_PIPELINE"
    assert ag.severity == "major"


def test_strategist_triage_output_rejects_invalid_severity():
    raw = """{"action_groups": [{"ag_id": "X", "primary_cluster_id": "H1", "source_cluster_ids": ["H1"], "affected_questions": [], "rca_kind": "x", "severity": "EXTREME", "rationale": ""}]}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, StrategistTriageOutput)


def test_strategist_triage_output_allows_empty_action_groups():
    parsed = validate_and_parse('{"action_groups": []}', StrategistTriageOutput)
    assert parsed.action_groups == []
```

Write the same three-test pattern for `StrategistDetailOutput`:

```python
from genie_space_optimizer.optimization.prompt_io import (
    StrategistDetailOutput,
    StrategistLeverSelection,
)


def test_strategist_detail_output_parses_canonical_response():
    raw = """
    {
      "ag_id": "AG_PIPELINE",
      "selected_levers": [
        {"lever": 6, "target_objects": ["catalog.schema.fact_sales"], "rationale": "missing window expression"}
      ],
      "rationale": "lever-6 best fits the wrong_aggregation pattern",
      "expected_outcome": "ROW_NUMBER snippet resolves top-N collapse"
    }
    """
    parsed = validate_and_parse(raw, StrategistDetailOutput)
    assert parsed.ag_id == "AG_PIPELINE"
    assert len(parsed.selected_levers) == 1
    assert parsed.selected_levers[0].lever == 6


def test_strategist_detail_output_rejects_invalid_lever_number():
    raw = """{"ag_id": "X", "selected_levers": [{"lever": 99, "target_objects": [], "rationale": ""}], "rationale": "", "expected_outcome": ""}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, StrategistDetailOutput)


def test_strategist_detail_output_allows_empty_levers():
    raw = '{"ag_id": "X", "selected_levers": [], "rationale": "no actionable lever", "expected_outcome": "skip"}'
    parsed = validate_and_parse(raw, StrategistDetailOutput)
    assert parsed.selected_levers == []
```

And the same for `AdaptiveStrategistOutput`:

```python
from genie_space_optimizer.optimization.prompt_io import AdaptiveStrategistOutput


def test_adaptive_strategist_output_parses_canonical_response():
    raw = """
    {
      "applicable_skills": [
        {
          "skill_id": "lever-4-join-discovery",
          "target_objects": ["catalog.schema.dim_store"],
          "expected_impact_qids": ["Q5"],
          "evidence_refs": ["H003"],
          "why": "missing join after Stage-1 empty",
          "priority": 2
        }
      ],
      "reasoning": "Stage-1 returned empty; falling back to join discovery"
    }
    """
    parsed = validate_and_parse(raw, AdaptiveStrategistOutput)
    assert len(parsed.applicable_skills) == 1
    assert parsed.applicable_skills[0].skill_id == "lever-4-join-discovery"


def test_adaptive_strategist_output_rejects_invalid_priority():
    raw = """{"applicable_skills": [{"skill_id": "x", "target_objects": [], "expected_impact_qids": [], "evidence_refs": [], "why": "x", "priority": 7}], "reasoning": ""}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, AdaptiveStrategistOutput)


def test_adaptive_strategist_output_allows_empty_skills():
    raw = '{"applicable_skills": [], "reasoning": "no actionable skill found"}'
    parsed = validate_and_parse(raw, AdaptiveStrategistOutput)
    assert parsed.applicable_skills == []
```

The full set is 9 new tests (3 each for the three strategist models).

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py -v -k "strategist"`
Expected: 9 FAIL.

- [ ] **Step 4: Define the models in `prompt_io.py`**

Append to `prompt_io.py`:

```python
_SeverityT = Literal["blocker", "major", "minor", "preventive"]


class StrategistActionGroup(LLMOutputContract):
    ag_id: str
    primary_cluster_id: str
    source_cluster_ids: list[str] = Field(default_factory=list)
    affected_questions: list[str] = Field(default_factory=list)
    rca_kind: str = ""
    severity: _SeverityT = "major"
    rationale: str = ""


class StrategistTriageOutput(LLMOutputContract):
    action_groups: list[StrategistActionGroup] = Field(default_factory=list)


class StrategistLeverSelection(LLMOutputContract):
    """One lever entry in the Stage-2 strategist detail output."""

    lever: int = Field(ge=1, le=6)
    target_objects: list[str] = Field(default_factory=list)
    rationale: str = ""


class StrategistDetailOutput(LLMOutputContract):
    ag_id: str
    selected_levers: list[StrategistLeverSelection] = Field(default_factory=list)
    rationale: str = ""
    expected_outcome: str = ""


class AdaptiveStrategistOutput(LLMOutputContract):
    """Fallback strategist picks; mirrors Stage1DiscoveryOutput shape."""

    applicable_skills: list[Stage1SkillPick] = Field(default_factory=list)
    reasoning: str = ""
```

If the actual SKILL.md / inline-string shape differs from the above, the tests in Step 2 will reveal that — update the model fields to match the prompt's `<output_schema>` block before moving on.

- [ ] **Step 5: Wire the models at the callsites**

- `optimizer.py:11299` (phase_1a_triage callsite for `STRATEGIST_TRIAGE_PROMPT`): pass `response_model=StrategistTriageOutput`.
- `optimizer.py:11454` (phase_1b_detail callsite for `STRATEGIST_DETAIL_PROMPT`): pass `response_model=StrategistDetailOutput`.
- `optimizer.py:10049` (monolithic_strategy_fallback for `STRATEGIST_PROMPT`): the monolithic strategist emits a different shape — write a `StrategistMonolithicOutput` model if the SKILL.md schema differs, OR opt out (`response_model=None`) and document why in a code comment.
- `optimizer.py:10665` (adaptive_strategist callsite for `ADAPTIVE_STRATEGIST_PROMPT`): pass `response_model=AdaptiveStrategistOutput`.

In each case the edit is identical to Task 14 Step 5 — add the import + the `response_model=` kwarg.

- [ ] **Step 6: Run tests**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py packages/genie-space-optimizer/tests/optimization/ -v -k "strategist or triage or detail"`
Expected: new 9 tests PASS; existing strategist tests PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py
git commit -m "feat: Strategist Pydantic output contracts + response_format= at 4 strategist callsites"
```

---

### Task 19: Lever-4 + Lever-5 output contracts (medium-volume active prompts)

**Files:**
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py` (lever-4 + lever-5 callsites)
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py` (lever-5b callsites at lines 909, 1060)
- Test: `packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py`

Rationale: Lever 4 (join spec + discovery) and Lever 5 (instruction + holistic + 5a + 5b) fire at medium frequency in Trial-5 and the strategist plans the executor is working through will produce more of them. Covering them here closes the spec requirement "every active LLM callsite" for the high+medium tier.

- [ ] **Step 1: Read each prompt's `<output_schema>` block**

For each of the five prompts below, open the SKILL.md (or the inline string in `common/config.py` for `LEVER_4_JOIN_DISCOVERY_PROMPT` and `LEVER_5_HOLISTIC_PROMPT`) and copy the canonical output JSON shape:

| Prompt | Source | Output shape key |
|---|---|---|
| `LEVER_4_JOIN_SPEC_PROMPT` | `lever-4-join-spec/SKILL.md` | `{"join_specs": [{...}], "rationale": "..."}` |
| `LEVER_4_JOIN_DISCOVERY_PROMPT` | inline at `common/config.py:2514` | `{"discovered_joins": [{"left_table": "...", "right_table": "...", "on": "...", "rationale": "..."}], ...}` |
| `LEVER_5_INSTRUCTION_PROMPT` | `lever-5-instruction/SKILL.md` | `{"instructions": [{"text": "...", "scope": "..."}], "rationale": "..."}` |
| `LEVER_5_HOLISTIC_PROMPT` | inline at `common/config.py:2666` | `{"sections": [...], "rationale": "..."}` |
| `LEVER_5A_INSTRUCTION_PROMPT` | `lever-5a-instructions/SKILL.md` | similar to 5 instruction |
| `LEVER_5B_EXAMPLE_SQL_PROMPT` | `lever-5b-example-sql/SKILL.md` | `{"examples": [{"question": "...", "sql": "..."}], "rationale": "..."}` |

The exact field names go into the Pydantic models below; pin them against the live `<output_schema>` text rather than inventing.

- [ ] **Step 2: Write the failing tests (one model × 3 tests = 18 tests)**

Append to `tests/optimization/test_prompt_io_contracts.py`. For each of the six models, write the same three-test pattern (canonical-parse, reject-invalid-enum-or-shape, allow-empty). Example for `Lever4JoinSpecOutput`:

```python
from genie_space_optimizer.optimization.prompt_io import (
    Lever4JoinSpec,
    Lever4JoinSpecOutput,
)


def test_lever_4_join_spec_output_parses_canonical_response():
    raw = """
    {
      "join_specs": [
        {
          "left_table": "catalog.schema.fact_sales",
          "right_table": "catalog.schema.dim_store",
          "on": "fact_sales.store_id = dim_store.location_id",
          "join_type": "INNER",
          "rationale": "store dimension lookup"
        }
      ],
      "rationale": "missing join after Stage-1 empty"
    }
    """
    parsed = validate_and_parse(raw, Lever4JoinSpecOutput)
    assert len(parsed.join_specs) == 1
    assert parsed.join_specs[0].join_type == "INNER"


def test_lever_4_join_spec_output_rejects_invalid_join_type():
    raw = """{"join_specs": [{"left_table": "t1", "right_table": "t2", "on": "x=y", "join_type": "CROSS_APPLY", "rationale": ""}], "rationale": ""}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever4JoinSpecOutput)


def test_lever_4_join_spec_output_allows_empty_join_specs():
    parsed = validate_and_parse('{"join_specs": [], "rationale": "no join needed"}', Lever4JoinSpecOutput)
    assert parsed.join_specs == []
```

Write the same three-test shape for each of the other five models. Use these specific canonical responses, invalid responses, and empty-array cases:

```python
from genie_space_optimizer.optimization.prompt_io import (
    Lever4DiscoveredJoin,
    Lever4JoinDiscoveryOutput,
)


def test_lever_4_join_discovery_output_parses_canonical_response():
    raw = """
    {
      "discovered_joins": [
        {"left_table": "catalog.schema.fact_sales", "right_table": "catalog.schema.dim_store",
         "on": "fact_sales.store_id = dim_store.location_id", "rationale": "store dimension"}
      ],
      "rationale": "missing FK relationship between fact and dim"
    }
    """
    parsed = validate_and_parse(raw, Lever4JoinDiscoveryOutput)
    assert len(parsed.discovered_joins) == 1
    assert parsed.discovered_joins[0].on.startswith("fact_sales.store_id")


def test_lever_4_join_discovery_output_rejects_missing_required_field():
    # 'right_table' is required; omitting it must fail.
    raw = """{"discovered_joins": [{"left_table": "t1", "on": "x=y", "rationale": ""}], "rationale": ""}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever4JoinDiscoveryOutput)


def test_lever_4_join_discovery_output_allows_empty_joins():
    parsed = validate_and_parse('{"discovered_joins": [], "rationale": "no join needed"}', Lever4JoinDiscoveryOutput)
    assert parsed.discovered_joins == []


from genie_space_optimizer.optimization.prompt_io import (
    Lever5InstructionEntry,
    Lever5InstructionOutput,
)


def test_lever_5_instruction_output_parses_canonical_response():
    raw = """
    {
      "instructions": [
        {"text": "Always filter store_status='OPEN' for active store queries.",
         "scope": "global", "rationale": "stale stores were skewing aggregates"}
      ],
      "rationale": "missing filter cluster on fact_sales"
    }
    """
    parsed = validate_and_parse(raw, Lever5InstructionOutput)
    assert len(parsed.instructions) == 1
    assert parsed.instructions[0].scope == "global"


def test_lever_5_instruction_output_rejects_invalid_scope():
    raw = """{"instructions": [{"text": "x", "scope": "EVERYWHERE", "rationale": ""}], "rationale": ""}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever5InstructionOutput)


def test_lever_5_instruction_output_allows_empty_instructions():
    parsed = validate_and_parse('{"instructions": [], "rationale": "no instruction needed"}', Lever5InstructionOutput)
    assert parsed.instructions == []


from genie_space_optimizer.optimization.prompt_io import (
    Lever5HolisticOutput,
    Lever5HolisticSection,
)


def test_lever_5_holistic_output_parses_canonical_response():
    raw = """
    {
      "sections": [
        {"section_name": "domain_context", "content": "Sales transactions ..."},
        {"section_name": "join_patterns", "content": "fact_sales JOIN dim_store ON ..."}
      ],
      "rationale": "consolidated 7 micro-instructions into 2 sections"
    }
    """
    parsed = validate_and_parse(raw, Lever5HolisticOutput)
    assert len(parsed.sections) == 2
    assert parsed.sections[0].section_name == "domain_context"


def test_lever_5_holistic_output_rejects_missing_section_name():
    raw = """{"sections": [{"content": "x"}], "rationale": ""}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever5HolisticOutput)


def test_lever_5_holistic_output_allows_empty_sections():
    parsed = validate_and_parse('{"sections": [], "rationale": "no consolidation needed"}', Lever5HolisticOutput)
    assert parsed.sections == []


from genie_space_optimizer.optimization.prompt_io import Lever5aInstructionOutput


def test_lever_5a_instruction_output_parses_canonical_response():
    raw = """
    {
      "instructions": [
        {"text": "Use ROW_NUMBER for ranking, not LIMIT alone.",
         "scope": "scoped", "rationale": "top-N with ties needs ranking"}
      ],
      "rationale": "ranking semantics missing for top-N questions"
    }
    """
    parsed = validate_and_parse(raw, Lever5aInstructionOutput)
    assert len(parsed.instructions) == 1


def test_lever_5a_instruction_output_rejects_invalid_scope():
    raw = """{"instructions": [{"text": "x", "scope": "UNKNOWN", "rationale": ""}], "rationale": ""}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever5aInstructionOutput)


def test_lever_5a_instruction_output_allows_empty_instructions():
    parsed = validate_and_parse('{"instructions": [], "rationale": "covered by 5-holistic"}', Lever5aInstructionOutput)
    assert parsed.instructions == []


from genie_space_optimizer.optimization.prompt_io import (
    Lever5bExample,
    Lever5bExampleSqlOutput,
)


def test_lever_5b_example_sql_output_parses_canonical_response():
    raw = """
    {
      "examples": [
        {"question": "What were total sales last quarter?",
         "sql": "SELECT SUM(amount) FROM fact_sales WHERE quarter = 'Q3'",
         "rationale": "demonstrates filter + aggregate pattern"}
      ],
      "rationale": "missing aggregate-by-period example"
    }
    """
    parsed = validate_and_parse(raw, Lever5bExampleSqlOutput)
    assert len(parsed.examples) == 1
    assert parsed.examples[0].sql.startswith("SELECT SUM")


def test_lever_5b_example_sql_output_rejects_missing_sql():
    raw = """{"examples": [{"question": "x", "rationale": ""}], "rationale": ""}"""
    with pytest.raises(ValueError):
        validate_and_parse(raw, Lever5bExampleSqlOutput)


def test_lever_5b_example_sql_output_allows_empty_examples():
    parsed = validate_and_parse('{"examples": [], "rationale": "no new example needed"}', Lever5bExampleSqlOutput)
    assert parsed.examples == []
```

The full set is 18 new tests (3 × 6 models, including the canonical `Lever4JoinSpecOutput` set at the top of this Step 2).

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py -v -k "lever_4 or lever_5"`
Expected: 18 FAIL.

- [ ] **Step 4: Define the models in `prompt_io.py`**

Append to `prompt_io.py`:

```python
class Lever4JoinSpec(LLMOutputContract):
    left_table: str
    right_table: str
    on: str
    join_type: Literal["INNER", "LEFT", "RIGHT", "FULL"] = "INNER"
    rationale: str = ""


class Lever4JoinSpecOutput(LLMOutputContract):
    join_specs: list[Lever4JoinSpec] = Field(default_factory=list)
    rationale: str = ""


class Lever4DiscoveredJoin(LLMOutputContract):
    left_table: str
    right_table: str
    on: str
    rationale: str = ""


class Lever4JoinDiscoveryOutput(LLMOutputContract):
    discovered_joins: list[Lever4DiscoveredJoin] = Field(default_factory=list)
    rationale: str = ""


class Lever5InstructionEntry(LLMOutputContract):
    text: str
    scope: Literal["global", "scoped"] = "scoped"
    rationale: str = ""


class Lever5InstructionOutput(LLMOutputContract):
    instructions: list[Lever5InstructionEntry] = Field(default_factory=list)
    rationale: str = ""


class Lever5HolisticSection(LLMOutputContract):
    section_name: str
    content: str


class Lever5HolisticOutput(LLMOutputContract):
    sections: list[Lever5HolisticSection] = Field(default_factory=list)
    rationale: str = ""


class Lever5aInstructionOutput(LLMOutputContract):
    """Mirrors Lever5InstructionOutput shape; separate model so the
    SKILL.md schemas can diverge in the future without breaking
    callers."""

    instructions: list[Lever5InstructionEntry] = Field(default_factory=list)
    rationale: str = ""


class Lever5bExample(LLMOutputContract):
    question: str
    sql: str
    rationale: str = ""


class Lever5bExampleSqlOutput(LLMOutputContract):
    examples: list[Lever5bExample] = Field(default_factory=list)
    rationale: str = ""
```

If the actual SKILL.md shapes differ from the above, the tests in Step 2 will fail in a specific way — adjust the model fields to match each prompt's `<output_schema>` block. The SKILL.md is the source of truth.

- [ ] **Step 5: Wire the models at the callsites**

For each prompt, locate the `_traced_llm_call` (or `_call_llm_for_proposal`) invocation and pass `response_model=`:

| Prompt | Callsite | Add |
|---|---|---|
| `LEVER_4_JOIN_SPEC_PROMPT` | `optimizer.py:_call_llm_for_proposal` (lever=4 branch) | `response_model=Lever4JoinSpecOutput` |
| `LEVER_4_JOIN_DISCOVERY_PROMPT` | search `rg "LEVER_4_JOIN_DISCOVERY_PROMPT" packages/genie-space-optimizer/src/` for the callsite | `response_model=Lever4JoinDiscoveryOutput` |
| `LEVER_5_INSTRUCTION_PROMPT` | `optimizer.py:_call_llm_for_proposal` (lever=5 branch) | `response_model=Lever5InstructionOutput` |
| `LEVER_5_HOLISTIC_PROMPT` | search `rg "LEVER_5_HOLISTIC_PROMPT" packages/genie-space-optimizer/src/` | `response_model=Lever5HolisticOutput` |
| `LEVER_5A_INSTRUCTION_PROMPT` | search `rg "LEVER_5A_INSTRUCTION_PROMPT" packages/genie-space-optimizer/src/` | `response_model=Lever5aInstructionOutput` |
| `LEVER_5B_EXAMPLE_SQL_PROMPT` | `synthesis.py:909, 1060`; also `cluster_driven_synthesis.py:958, 1070` | `response_model=Lever5bExampleSqlOutput` |

For each callsite the edit pattern is identical:

```python
from genie_space_optimizer.optimization.prompt_io import Lever4JoinSpecOutput
# (or the matching model name for this prompt)

raw_text, _ = _traced_llm_call(
    w, system_msg, prompt,
    span_name="<existing_span_name>",
    response_model=Lever4JoinSpecOutput,  # the matching model
)
```

- [ ] **Step 6: Run tests**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py packages/genie-space-optimizer/tests/optimization/ -v -k "lever_4 or lever_5"`
Expected: 18 new tests PASS; existing tests PASS.

- [ ] **Step 7: Commit**

```bash
git add packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/optimizer.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/synthesis.py packages/genie-space-optimizer/src/genie_space_optimizer/optimization/cluster_driven_synthesis.py packages/genie-space-optimizer/tests/optimization/test_prompt_io_contracts.py
git commit -m "feat: Lever-4 + Lever-5 Pydantic output contracts + response_format= at 6 callsites"
```

---

### Task 20: Explicit deferred allowlist for low-priority prompts

**Files:**
- Modify: `packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py`
- Modify: `packages/genie-space-optimizer/src/genie_space_optimizer/optimization/prompt_io.py`

The following prompts fire LLM calls but are demonstrably low-volume in Trial-5 (preflight, enrichment, or one-off setup). We document the deferral here so future maintainers know they were intentionally left without a typed output contract, and so the inventory test allows them through.

**Deferred (no Pydantic output contract required):**

| Prompt | Callsite | Reason for deferral |
|---|---|---|
| `DESCRIPTION_ENRICHMENT_PROMPT` | `optimizer.py:3610` (`enrich_column_descriptions_batch`) | Batch enrichment of UC column descriptions; runs once at space setup. Output is a list of description strings keyed by column FQN — already validated downstream by UC schema enforcement when written. Low blast radius. |
| `TABLE_DESCRIPTION_ENRICHMENT_PROMPT` | `optimizer.py:3838` | Same rationale as above for table descriptions. |
| `SPACE_DESCRIPTION_PROMPT` | `optimizer.py:3990` | One-line space-level description; downstream consumer is the Genie Space metadata page (free-form). |
| `SAMPLE_QUESTIONS_PROMPT` | `optimizer.py:4378` | Generates 5-10 NL sample questions; output is a string list; consumer is the Genie UI sample-question chiplet. |
| `PROACTIVE_INSTRUCTION_PROMPT` | `optimizer.py:4072` | Preflight-only; generates initial instructions before any lever loop runs. |
| `EXPAND_INSTRUCTION_PROMPT` | `optimizer.py:4253` | Preflight-only; expands a terse instruction into a full one. |
| `GT_REPAIR_PROMPT` | `optimizer.py:?` (rare) | Repairs benchmark ground-truth SQL; fires only in `_repair_ground_truth_sql` which is gated by a debug flag. |
| `PROSE_RULE_MINING_PROMPT` | `optimizer.py:13558` | Mines prose-style rules from the corpus; output is a list of free-form text rules. Output schema is loosely structured. |
| `SQL_EXPRESSION_SEEDING_PROMPT` | `optimizer.py:14369` | Preflight-only seeding pass. |
| `PROPOSAL_GENERATION_PROMPT` (lever-3-tvf-routing) | `optimizer.py:_call_llm_for_proposal` (lever=3 branch) | Legacy fallback prompt; Stage-1 routing now bypasses it in nearly all cases. |
| Inline prompts in `archetype_learning.py:337`, `cluster_driven_synthesis.py:958/1070`, `preflight_synthesis.py:1942` | various | Some are preflight, some are debug. Future review may convert them; tracked under follow-up plan. |

- [ ] **Step 1: Add the allowlist constant**

In `tests/optimization/test_prompt_registry_inventory.py`, add:

```python
# The prompts in this allowlist may fire LLM calls without a Pydantic
# response_model. Adding a new entry requires a paragraph of
# justification in 2026-05-17-prompt-registry-and-typed-io-hygiene.md
# Task 20. The list is reviewed quarterly; entries are demoted to
# "needs typed output" if their call volume grows.
TYPED_OUTPUT_DEFERRED_ALLOWLIST: frozenset[str] = frozenset({
    "description_enrichment",
    "table_description_enrichment",
    "space_description",
    "sample_questions",
    "proactive_instruction",
    "expand_instruction",
    "gt_repair",
    "prose_rule_mining",
    "preflight_example_synthesis",
    "proposal_generation",
})
```

- [ ] **Step 2: Write the failing test for typed-output coverage**

```python
def test_every_active_lever_prompt_has_typed_output_contract():
    """Every LEVER_PROMPTS entry (except the explicit deferred
    allowlist) must have a corresponding Pydantic model in prompt_io.py.

    A failing test here means: a new prompt was added to LEVER_PROMPTS,
    but no Pydantic output model exists for it. Either add the model
    (and wire response_model= at the callsite) or add the prompt to
    TYPED_OUTPUT_DEFERRED_ALLOWLIST with a documented rationale in
    2026-05-17-prompt-registry-and-typed-io-hygiene.md Task 20.
    """
    from genie_space_optimizer.optimization import prompt_io as pio

    # Map prompt registry name -> expected Pydantic model class name.
    # Convention: lever_6_sql_expression -> Lever6SqlExpressionOutput.
    expected_models = {
        name: "".join(part.capitalize() for part in name.split("_")) + "Output"
        for name in cfg.LEVER_PROMPTS
        if name not in TYPED_OUTPUT_DEFERRED_ALLOWLIST
    }
    missing = [
        f"{registry_name} -> expected pio.{model_name}"
        for registry_name, model_name in expected_models.items()
        if not hasattr(pio, model_name)
    ]
    assert not missing, (
        f"These LEVER_PROMPTS entries are missing a Pydantic output "
        f"contract in prompt_io.py: {missing}. Either add the model + "
        f"wire response_model= at the callsite, or add the prompt name "
        f"to TYPED_OUTPUT_DEFERRED_ALLOWLIST with a documented "
        f"rationale in the plan."
    )
```

Note: the auto-derived class name convention (snake_case → CamelCase + "Output") MUST match the class names defined in Tasks 14-19. Verify:
- `stage_1_discovery` → `Stage1DiscoveryOutput` ✓
- `lever_6_sql_expression` → `Lever6SqlExpressionOutput` ✓
- `lever_1_rca_bridge` → `Lever1RcaBridgeOutput` ✓
- `lever_1_2_column` → `Lever12ColumnOutput` ✓ (note: `1_2` collapses to `12`; the test code uses `"".join(part.capitalize() for part in name.split("_"))`, which produces `Lever12Column`. Append `Output` → `Lever12ColumnOutput`. Match.)
- `strategist_triage` → `StrategistTriageOutput` ✓
- `strategist_detail` → `StrategistDetailOutput` ✓
- `adaptive_strategist` → `AdaptiveStrategistOutput` ✓
- `lever_4_join_spec` → `Lever4JoinSpecOutput` ✓
- `lever_4_join_discovery` → `Lever4JoinDiscoveryOutput` ✓
- `lever_5_instruction` → `Lever5InstructionOutput` ✓
- `lever_5_holistic` → `Lever5HolisticOutput` ✓
- `lever_5a_instruction` → `Lever5aInstructionOutput` ✓ (note: lowercase `a` in `5a`; the test convention emits `Lever5a`. Make sure the class name in Task 19 matches.)
- `lever_5b_example_sql` → `Lever5bExampleSqlOutput` ✓
- `strategist` (monolithic) → `StrategistOutput`. If Task 18 did not define this (per the "opt out" branch), add `strategist` to `TYPED_OUTPUT_DEFERRED_ALLOWLIST` with the rationale "monolithic fallback; superseded by triage+detail split in production".

- [ ] **Step 3: Run test to verify it passes (or surfaces the one expected miss)**

Run: `pytest packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py::test_every_active_lever_prompt_has_typed_output_contract -v`
Expected: PASS if all 13 active prompts have matching Pydantic models defined in Tasks 14-19. If `strategist` (monolithic) lacks a model, the test will name it — add it to the allowlist (Step 1) with the documented rationale.

- [ ] **Step 4: Commit**

```bash
git add packages/genie-space-optimizer/tests/optimization/test_prompt_registry_inventory.py
git commit -m "test: lock typed-output coverage for active prompts + document deferred allowlist"
```

---

## Phase G — Documentation

### Task 21: Update `docs/skill-catalogue.md` and add an architecture note

**Files:**
- Modify: `packages/genie-space-optimizer/docs/skill-catalogue.md` (or create `packages/genie-space-optimizer/docs/prompt-registry-and-typed-io.md` if `skill-catalogue.md` is owned by another concern)
- Modify: `packages/genie-space-optimizer/AGENTS.md` (if it exists) or `CLAUDE.md` to document the invariant

- [ ] **Step 1: Verify which doc file is the right home**

Run: `ls packages/genie-space-optimizer/docs/`

Expected: `skill-catalogue.md` exists. If not, create `prompt-registry-and-typed-io.md`.

- [ ] **Step 2: Write the new section**

Append to `packages/genie-space-optimizer/docs/skill-catalogue.md` (or create the new file):

```markdown
## Prompt registry invariants

Every prompt template that fires a Databricks Claude (or other LLM) call
in production MUST:

1. Live in a `skills/<skill-name>/SKILL.md` file, loaded via
   `_SKILL_LOADER.load_prompt("<skill-name>",
   expected_constant_name="FOO_PROMPT")` in `common/config.py`.
   Inline f-string prompts in the optimization module are forbidden —
   the `tests/optimization/test_prompt_registry_inventory.py` guardrail
   fails fast on any new inline LLM prompt.

2. Be present in the `LEVER_PROMPTS` dict in `common/config.py`. This
   dict is the source of truth for `register_judge_prompts(...)` and the
   MLflow Prompt Registry under the configured UC schema (versioned,
   aliased, A/B-testable).

3. Have a `_link_prompt_to_trace("<registry_name>")` call immediately
   before its `_traced_llm_call()` (or `_call_llm_for_proposal()`)
   invocation so the trace's "Linked Prompts" tab in the MLflow UI
   shows the prompt version that produced the response.

Failure mode if any of the three is missed:

- (1) missed → editing the prompt requires a code change rather than a
  registry rollback, and the SKILL.md `target_kind`/`when_to_pick` 
  metadata are not available to Stage-1 discovery.
- (2) missed → the prompt is never versioned in MLflow; you cannot 
  compare runs across two prompt versions.
- (3) missed → traces show the wrong prompt (or no prompt) in the 
  Linked Prompts tab, making debugging across versions hard.

## Typed output contracts

Every active LLM callsite passes a Pydantic `LLMOutputContract` 
subclass via `response_model=` to `_traced_llm_call()`. The wrapper:

1. Builds `response_format={"type": "json_schema", "json_schema":
   {"name": "<ModelName>", "schema": <schema>, "strict": True}}` from
   the Pydantic model and passes it to Databricks Foundation Model APIs
   for server-side enforcement.

2. Strips unsupported JSON Schema keywords (`pattern`, `anyOf`,
   `oneOf`, `allOf`, `prefixItems`, `$ref`, `maxProperties`,
   `minProperties`, `maxLength`, `format`) before sending — Databricks
   rejects them.

3. Re-parses and re-validates the response text against the same model
   client-side (defense in depth — server-side enforcement can miss
   constraints not expressible in the supported subset, e.g. mutual
   exclusion).

4. Retries with exponential backoff on validation failure (via the
   existing `response_validator` machinery in `_traced_llm_call`).

Models live in `optimization/prompt_io.py`. Adding a new prompt
output contract:

1. Declare a subclass of `LLMOutputContract` with fields matching the
   prompt's `<output_schema>` block in `SKILL.md`.
2. Pass `response_model=<NewModel>` at the callsite.
3. Add a test under `tests/optimization/test_prompt_io_contracts.py`
   asserting: (a) the canonical example parses, (b) at least one
   invalid example raises `ValueError`, (c) any empty-array cases that
   legitimately occur in production also parse.
```

- [ ] **Step 3: Optionally update `AGENTS.md`**

If `packages/genie-space-optimizer/AGENTS.md` (or `CLAUDE.md`) exists and documents project conventions, add a one-line entry under "Critical Rules":

```markdown
- **All LLM prompts MUST be loaded from `skills/*/SKILL.md` files** —
  inline f-string prompts are forbidden by the
  `test_prompt_registry_inventory.py` guardrail. Every prompt must
  appear in `LEVER_PROMPTS` (`common/config.py`) AND have a
  `_link_prompt_to_trace("<name>")` call at every callsite. See
  `docs/skill-catalogue.md#prompt-registry-invariants`.
```

- [ ] **Step 4: Run the full test suite as a final smoke test**

Run: `pytest packages/genie-space-optimizer/tests/ -v -x --ignore=packages/genie-space-optimizer/tests/integration`

Expected: all tests PASS. (Skip the `integration/` directory if it requires live Databricks credentials.)

- [ ] **Step 5: Commit**

```bash
git add packages/genie-space-optimizer/docs/skill-catalogue.md packages/genie-space-optimizer/AGENTS.md
git commit -m "docs: document prompt registry invariants and typed I/O contract pattern"
```

---

## Post-plan verification

After all 21 tasks are committed:

1. The new guardrail test (`test_prompt_registry_inventory.py`) passes — every loader constant is in `LEVER_PROMPTS`, no inline f-string LLM prompts exist in `optimization/`, the phantom L2 constant is gone.

2. The Lever-1/2 hardening plans (paused at the start of this work) can resume with the new typed I/O contracts already in place. Specifically:
   - The dormant `LEVER_1_2_COLUMN_PROMPT` callsites (`_stage_2_l1`, `_stage_2_l2`) now pass `response_model=Lever12ColumnOutput`, so when the Stage-1 fixes land and populate `target_objects`, the LLM responses are already enforced.
   - The actually-firing `_generate_lever1_rca_proposal` callsite now passes `response_model=Lever1RcaBridgeOutput`, so the RCA-bridge LLM cannot silently emit unexpected fields like `debug_hash`.

3. Future prompt additions (e.g. when someone introduces `lever-7-...`) automatically fail the guardrail until added to `LEVER_PROMPTS`, given a Pydantic output model, and wired with `_link_prompt_to_trace()`. The plan is self-enforcing from here on.

---

## Follow-up Plan — Active-Callsite Typed-Output Wiring

Phase F of this plan defined the Pydantic models in `prompt_io.py` and
extended `_traced_llm_call()` to accept `response_model=`, but the wiring
at most callsites was deferred. The follow-up plan
`2026-05-17-active-callsite-typed-output-wiring.md` closed that gap by:

1. Making `build_response_format()` strict-mode aware so permissive
   strategist bases stop being rejected with HTTP 400.
2. Wiring `response_model=` at all 9 unwired active callsites (Stage-1,
   Lever-6, strategist triage/detail/adaptive, lever-5b synthesis ×2,
   lever-5b cluster-driven ×2, lever-4 join discovery, lever-5a
   instructions). Stage-1 / Lever-4 / Lever-5a were migrated from
   `_call_llm_openai` to `_traced_llm_call` as part of the wiring.
3. Adding `test_typed_output_wiring.py` as a golden AST guard.
