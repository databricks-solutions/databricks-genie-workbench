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

Plan: docs/prompt_improvements/2026-05-17-prompt-registry-and-typed-io-hygiene.md
"""
from __future__ import annotations

import ast
import pathlib

import pytest

from genie_space_optimizer.common import config as cfg

SRC = pathlib.Path(__file__).resolve().parents[3] / "src" / "genie_space_optimizer"


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
            # for the in-module check; covered by Task 4/5 separately.
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


def test_lever_6_sql_expression_prompt_is_registered():
    assert "lever_6_sql_expression" in cfg.LEVER_PROMPTS, (
        "LEVER_6_SQL_EXPRESSION_PROMPT is the active Stage-2 prompt for "
        "lever-6 (48 LLM calls in Trial-5) but is not in LEVER_PROMPTS, "
        "so it is never registered to MLflow Prompt Registry. Add "
        "'lever_6_sql_expression': LEVER_6_SQL_EXPRESSION_PROMPT to "
        "LEVER_PROMPTS in common/config.py."
    )
    assert cfg.LEVER_PROMPTS["lever_6_sql_expression"] is cfg.LEVER_6_SQL_EXPRESSION_PROMPT


def test_stage_1_discovery_prompt_is_registered():
    assert "stage_1_discovery" in cfg.LEVER_PROMPTS, (
        "STAGE_1_DISCOVERY_PROMPT is the routing brain for the entire "
        "three-stage pipeline (fires once per AG) but is not in "
        "LEVER_PROMPTS, so it is never registered to MLflow Prompt "
        "Registry. Add 'stage_1_discovery': STAGE_1_DISCOVERY_PROMPT to "
        "LEVER_PROMPTS in common/config.py."
    )
    assert cfg.LEVER_PROMPTS["stage_1_discovery"] is cfg.STAGE_1_DISCOVERY_PROMPT


def test_lever_5a_instruction_prompt_is_registered():
    # Key matches the existing _link_prompt_to_trace("lever_5a_instructions")
    # call at optimizer.py:9387 (plural form).
    assert "lever_5a_instructions" in cfg.LEVER_PROMPTS, (
        "LEVER_5A_INSTRUCTION_PROMPT is the Plan-2 split path for "
        "instructions (separate from LEVER_5_HOLISTIC_PROMPT) but is not "
        "in LEVER_PROMPTS. Add 'lever_5a_instructions': "
        "LEVER_5A_INSTRUCTION_PROMPT to LEVER_PROMPTS in common/config.py."
    )
    assert cfg.LEVER_PROMPTS["lever_5a_instructions"] is cfg.LEVER_5A_INSTRUCTION_PROMPT


def test_lever_5b_example_sql_prompt_is_registered():
    """The lever-5b example-SQL synthesis prompt fires for every cluster-
    driven synthesis attempt — high volume and high blast radius.
    """
    assert "lever_5b_example_sql" in cfg.LEVER_PROMPTS, (
        "_SYNTHESIS_PROMPT_TEMPLATE (loaded from "
        "lever-5b-example-sql/SKILL.md) is the active example-SQL "
        "synthesis prompt but is not in LEVER_PROMPTS. Import it into "
        "common/config.py and add 'lever_5b_example_sql': "
        "LEVER_5B_EXAMPLE_SQL_PROMPT to LEVER_PROMPTS."
    )


def test_lever_1_rca_bridge_prompt_is_loaded_and_registered():
    """The RCA-bridge prompt (formerly an inline f-string in
    _generate_lever1_rca_proposal) must be loadable from
    lever-1-rca-bridge/SKILL.md and registered in LEVER_PROMPTS.
    """
    assert hasattr(cfg, "LEVER_1_RCA_BRIDGE_PROMPT"), (
        "LEVER_1_RCA_BRIDGE_PROMPT constant is missing from common.config."
    )
    assert "lever_1_rca_bridge" in cfg.LEVER_PROMPTS, (
        "LEVER_1_RCA_BRIDGE_PROMPT is not in LEVER_PROMPTS."
    )
    assert cfg.LEVER_PROMPTS["lever_1_rca_bridge"] is cfg.LEVER_1_RCA_BRIDGE_PROMPT


def test_lever_2_skill_md_is_metadata_only():
    """L2's SKILL.md body was byte-identical dead code that nobody
    loaded — optimizer.py routes both lever=1 and lever=2 to
    LEVER_1_2_COLUMN_PROMPT (the L1 body)."""
    l2_path = (
        pathlib.Path(__file__).resolve().parents[3]
        / "src" / "genie_space_optimizer" / "skills"
        / "lever-2-mv-column-refinement" / "SKILL.md"
    )
    body = l2_path.read_text()
    assert "<role>" not in body, (
        "L2 SKILL.md still contains a <role> body. The L2 body is dead "
        "code because optimizer.py routes lever=2 to LEVER_1_2_COLUMN_PROMPT "
        "(the L1 body). Delete the body and leave only frontmatter + the "
        "explanatory comment. See plan "
        "2026-05-17-prompt-registry-and-typed-io-hygiene.md Task 10."
    )
    assert (
        "shared with lever-1-table-column-description" in body
        or "shared with LEVER_1_2_COLUMN_PROMPT" in body
    ), (
        "L2 SKILL.md must contain an explanatory comment naming the "
        "shared template (LEVER_1_2_COLUMN_PROMPT)."
    )


def test_no_phantom_LEVER_1_2_COLUMN_PROMPT_FOR_L2_references():
    """The constant LEVER_1_2_COLUMN_PROMPT_FOR_L2 was referenced by L2's
    SKILL.md frontmatter and CATALOGUE.md but never defined in code."""
    src = pathlib.Path(__file__).resolve().parents[3] / "src"
    skills = src / "genie_space_optimizer" / "skills"
    bad = []
    for f in skills.rglob("*.md"):
        if "LEVER_1_2_COLUMN_PROMPT_FOR_L2" in f.read_text():
            bad.append(str(f.relative_to(src.parent)))
    assert not bad, (
        f"Phantom constant LEVER_1_2_COLUMN_PROMPT_FOR_L2 still "
        f"referenced in: {bad}. It is never defined in code."
    )


# Plan 2026-05-17-prompt-registry-and-typed-io-hygiene Task 20 — the
# prompts in this allowlist may fire LLM calls without a Pydantic
# response_model. Adding a new entry requires a paragraph of
# justification in the plan doc. The list is reviewed quarterly; entries
# are demoted to "needs typed output" if their call volume grows.
TYPED_OUTPUT_DEFERRED_ALLOWLIST: frozenset[str] = frozenset({
    # Preflight / enrichment — low volume, low blast radius:
    "description_enrichment",
    "table_description_enrichment",
    "space_description",
    "sample_questions",
    "proactive_instruction",
    "expand_instruction",
    "gt_repair",
    "prose_rule_mining",
    "instruction_to_sql_expression",
    "preflight_example_synthesis",
    "proposal_generation",
    # Cluster-driven teaching-kit synthesis. The TeachingKitOutput
    # Pydantic contract now exists and is wired at both call sites
    # (Tasks 1+5 of 2026-05-17-cluster-driven-example-synthesis-
    # hardening.md). The entry stays on the allowlist because the
    # prompt is not yet registered in LEVER_PROMPTS (Task 3 of that
    # plan, still deferred); the inventory test only checks names
    # that ARE in LEVER_PROMPTS, so removing this entry is a no-op
    # today but keeping it documented avoids re-deriving the name
    # convention when Task 3 lands.
    "cluster_driven_example_synthesis",
    # Strategist family monolith — superseded by triage+detail split
    # which are contracted (StrategistTriageOutput / StrategistDetailOutput):
    "strategist",
    # Lever-5 holistic — legacy path superseded by lever_5_instruction
    # in production:
    "lever_5_holistic",
    # Lever-4 join_spec — non-pickable scaffolding prompt; output shape
    # is shared with Lever-4 join_discovery which IS contracted as
    # Lever4JoinDiscoveryOutput:
    "lever_4_join_spec",
})


def test_every_active_lever_prompt_has_typed_output_contract():
    """Every LEVER_PROMPTS entry (except the explicit deferred allowlist)
    must have a corresponding Pydantic model in prompt_io.py."""
    from genie_space_optimizer.optimization import prompt_io as pio

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
        f"to TYPED_OUTPUT_DEFERRED_ALLOWLIST with a documented rationale "
        f"in 2026-05-17-prompt-registry-and-typed-io-hygiene.md Task 20."
    )
