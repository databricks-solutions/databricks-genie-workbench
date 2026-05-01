"""Tests for the fix agent prompts and field validation.

Validates that prompts contain correct Genie API field names,
do not contain known hallucinated field names, and that the
field path validator catches invalid paths.
"""

import pytest

from backend.prompts import get_fix_agent_prompt, get_fix_agent_single_prompt
from backend.services.fix_agent import _validate_field_path


@pytest.fixture
def sample_prompt():
    """Generate a fix agent prompt with minimal inputs."""
    return get_fix_agent_prompt(
        space_id="test-space-id",
        findings=["No text instructions configured", "No example SQL questions configured"],
        space_config={"data_sources": {"tables": []}, "instructions": {}},
    )


class TestFixAgentPromptFieldNames:
    """Ensure the prompt guides the LLM to use correct Genie API field names."""

    def test_contains_example_question_sqls(self, sample_prompt):
        assert "example_question_sqls" in sample_prompt

    def test_contains_text_instructions(self, sample_prompt):
        assert "text_instructions" in sample_prompt

    def test_contains_join_specs(self, sample_prompt):
        assert "join_specs" in sample_prompt

    def test_contains_join_specs_left(self, sample_prompt):
        assert "join_specs[N].left" in sample_prompt

    def test_contains_join_specs_right(self, sample_prompt):
        assert "join_specs[N].right" in sample_prompt

    def test_contains_sql_snippets(self, sample_prompt):
        assert "sql_snippets" in sample_prompt

    def test_contains_sample_questions(self, sample_prompt):
        assert "sample_questions" in sample_prompt

    def test_contains_column_configs(self, sample_prompt):
        assert "column_configs" in sample_prompt

    def test_contains_metric_views(self, sample_prompt):
        assert "metric_views" in sample_prompt

    def test_warns_against_sql_examples(self, sample_prompt):
        """The prompt must explicitly warn against the common hallucination."""
        assert "NOT `sql_examples`" in sample_prompt

    def test_no_bare_sql_examples_as_valid_path(self, sample_prompt):
        """sql_examples should only appear in the warning, never as a valid path."""
        # Remove the warning section, then check sql_examples doesn't appear
        # as a valid field path
        lines = sample_prompt.split("\n")
        valid_path_lines = [
            l for l in lines
            if l.strip().startswith("- `") and "sql_examples" in l
        ]
        assert len(valid_path_lines) == 0, (
            f"sql_examples appears as a valid path: {valid_path_lines}"
        )


class TestFixAgentPromptStructure:
    """Ensure the prompt has the required structural sections."""

    def test_includes_findings(self, sample_prompt):
        assert "No text instructions configured" in sample_prompt
        assert "No example SQL questions configured" in sample_prompt

    def test_includes_config_json(self, sample_prompt):
        assert '"data_sources"' in sample_prompt

    def test_includes_valid_field_paths_section(self, sample_prompt):
        assert "Valid Field Paths" in sample_prompt

    def test_includes_output_format(self, sample_prompt):
        assert '"patches"' in sample_prompt
        assert '"field_path"' in sample_prompt
        assert '"new_value"' in sample_prompt
        assert '"rationale"' in sample_prompt


# ---------------------------------------------------------------------------
# Single-finding prompt (used by the per-issue fix flow)
# ---------------------------------------------------------------------------

@pytest.fixture
def single_prompt():
    """Generate a single-finding fix agent prompt."""
    return get_fix_agent_single_prompt(
        space_id="test-space-id",
        finding="No text instructions configured",
        space_config={"data_sources": {"tables": []}, "instructions": {}},
    )


class TestSinglePromptFieldNames:
    """Ensure the single-finding prompt uses correct field names."""

    def test_contains_example_question_sqls(self, single_prompt):
        assert "example_question_sqls" in single_prompt

    def test_contains_text_instructions(self, single_prompt):
        assert "text_instructions" in single_prompt

    def test_warns_against_sql_examples(self, single_prompt):
        assert "NOT `sql_examples`" in single_prompt


class TestSinglePromptStructure:
    """Ensure the single-finding prompt has the right structure."""

    def test_includes_single_finding(self, single_prompt):
        assert "No text instructions configured" in single_prompt

    def test_includes_config_json(self, single_prompt):
        assert '"data_sources"' in single_prompt

    def test_asks_for_json_only_output(self, single_prompt):
        assert "ONLY a JSON object" in single_prompt or "ONLY valid JSON" in single_prompt

    def test_output_format_is_flat_not_array(self, single_prompt):
        """Single prompt should ask for a flat {field_path, new_value, rationale}, not {patches: [...]}."""
        assert '"field_path"' in single_prompt
        assert '"new_value"' in single_prompt


# ---------------------------------------------------------------------------
# GSL section-header preservation (near-term, epic #87)
# ---------------------------------------------------------------------------
# The Fix Agent must preserve canonical `## Section` headers in
# text_instructions content and must know how to decline a patch that would
# erase one. See docs/gsl-instruction-schema.md.

CANONICAL_GSL_SECTIONS = [
    "## PURPOSE",
    "## DISAMBIGUATION",
    "## DATA QUALITY NOTES",
    "## CONSTRAINTS",
    "## Instructions you must follow when providing summaries",
]


class TestFixAgentGslSectionPreservation:
    """Both Fix Agent prompts must teach the LLM to preserve canonical GSL section headers."""

    def test_single_prompt_mentions_all_canonical_sections(self, single_prompt):
        for section in CANONICAL_GSL_SECTIONS:
            assert section in single_prompt, (
                f"Single-finding fix prompt missing canonical GSL section header: {section!r}. "
                f"See docs/gsl-instruction-schema.md."
            )

    def test_batch_prompt_mentions_canonical_sections(self, sample_prompt):
        # The batch prompt only needs to reference the existence of canonical
        # sections via the _VALID_FIELD_PATHS_BLOCK and its rules block.
        assert "## PURPOSE" in sample_prompt
        assert "## CONSTRAINTS" in sample_prompt
        assert "preserve" in sample_prompt.lower()

    def test_single_prompt_teaches_decline_shape(self, single_prompt):
        """When a fix would erase a canonical section, the agent must decline, not apply."""
        assert '"decline": true' in single_prompt
        assert "DECLINE" in single_prompt

    def test_single_prompt_explains_section_order(self, single_prompt):
        """The canonical order must be spelled out so the LLM can insert new sections correctly."""
        assert "PURPOSE" in single_prompt
        assert "CONSTRAINTS" in single_prompt
        # The ordering should appear as a sequence (either arrow-separated or similar)
        idx_purpose = single_prompt.find("PURPOSE")
        idx_constraints = single_prompt.find("CONSTRAINTS")
        assert idx_purpose < idx_constraints, (
            "Canonical ordering should present PURPOSE before CONSTRAINTS in the prompt"
        )

    def test_batch_prompt_allows_skipping_when_section_would_be_erased(self, sample_prompt):
        """Batch prompt uses empty field_path + rationale as its skip mechanism."""
        assert '"field_path"' in sample_prompt
        # It should explicitly acknowledge that skipping is an option for the section-preservation rule.
        assert "SKIP" in sample_prompt or "skip the patch" in sample_prompt.lower()

    def test_valid_paths_block_documents_section_preservation(self, single_prompt):
        """The valid-paths list entry for text_instructions[N].content should remind
        the LLM to preserve headers (belt-and-suspenders so the rule lands even if
        the Rules block gets truncated)."""
        # Find the text_instructions entry and check it explains preservation
        lines = single_prompt.split("\n")
        ti_lines = [l for l in lines if "text_instructions[N].content" in l]
        assert len(ti_lines) >= 1, "Expected text_instructions[N].content path entry"
        combined = " ".join(ti_lines)
        assert "preserve" in combined.lower() or "Section" in combined


# ---------------------------------------------------------------------------
# Field path validation
# ---------------------------------------------------------------------------

class TestValidateFieldPath:
    """Ensure _validate_field_path catches invalid Genie API field names."""

    @pytest.mark.parametrize("path", [
        "instructions.text_instructions[0].content",
        "instructions.example_question_sqls[0].question",
        "instructions.example_question_sqls[0].sql",
        "data_sources.tables[0].description",
        "data_sources.tables[0].column_configs[0].synonyms",
        "data_sources.tables[0].column_configs[0].description",
        "instructions.join_specs[0].left",
        "instructions.join_specs[0].right",
        "instructions.join_specs[0].sql",
        "instructions.sql_snippets.filters[0].sql",
        "instructions.sql_snippets.measures[0].alias",
        "instructions.sql_snippets.expressions[0].display_name",
        "config.sample_questions[0].question",
        "benchmarks.questions[0].question",
        "data_sources.metric_views[0].identifier",
    ])
    def test_valid_paths(self, path):
        assert _validate_field_path(path) is True

    @pytest.mark.parametrize("path,reason", [
        ("instructions.sql_examples[0].question", "hallucinated field name"),
        ("instructions.example_sqls[0].sql", "hallucinated field name"),
        ("instructions.general_instructions[0].content", "hallucinated field name"),
        ("data_sources.views[0].identifier", "hallucinated field name"),
        ("instructions.sql_queries[0].sql", "hallucinated field name"),
        ("settings.theme", "nonexistent top-level field"),
    ])
    def test_invalid_paths(self, path, reason):
        assert _validate_field_path(path) is False, f"Should reject: {reason}"
