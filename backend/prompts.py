"""Prompts for the Genie Space Analyzer agent."""

import json


def get_checklist_evaluation_prompt(
    section_name: str,
    section_data: dict | list | None,
    checklist_items: list[dict],
) -> str:
    """Build the prompt for LLM to evaluate qualitative checklist items.

    Args:
        section_name: Name of the section being analyzed
        section_data: The actual data from the Genie Space section to analyze
        checklist_items: List of dicts with 'id' and 'description' for each item to evaluate

    Returns:
        The formatted prompt string
    """
    items_text = "\n".join(
        f"- {item['id']}: {item['description']}"
        for item in checklist_items
    )

    data_json = json.dumps(section_data, indent=2) if section_data else "null (section not configured)"

    return f"""You are evaluating a Databricks Genie Space configuration section against specific checklist criteria.

## Section: {section_name}

## Data to Analyze:
```json
{data_json}
```

## Checklist Items to Evaluate:
{items_text}

## Instructions:
For each checklist item, determine if the configuration passes or fails the criterion.
Be fair but thorough - a check should pass if the configuration reasonably meets the criterion.
If the section data is empty/null, most quality checks should fail (except those that are N/A).

Output your evaluation as JSON with this exact structure:
{{
  "evaluations": [
    {{
      "id": "item_id_here",
      "passed": true | false,
      "details": "Brief explanation of why it passed or failed"
    }}
  ],
  "findings": [
    {{
      "category": "best_practice" | "warning" | "suggestion",
      "severity": "high" | "medium" | "low",
      "description": "Description of the issue (only for failed items)",
      "recommendation": "Specific actionable recommendation",
      "reference": "Related checklist item ID"
    }}
  ],
  "summary": "Brief overall summary of the section's compliance"
}}

Only include findings for checklist items that FAILED. Do not create findings for passing items.
Match finding severity to the importance of the failed check:
- high: Critical functionality or major best practice violation
- medium: Recommended practice not followed
- low: Minor improvement opportunity"""


def get_optimization_prompt(
    space_data: dict,
    labeling_feedback: list[dict],
    checklist_content: str,
    schema_content: str,
) -> str:
    """Build the prompt for generating optimization suggestions based on labeling feedback.

    Args:
        space_data: The full Genie Space configuration
        labeling_feedback: List of dicts with question_text, is_correct, feedback_text
        checklist_content: The best practices checklist markdown
        schema_content: The Genie Space JSON schema documentation

    Returns:
        The formatted prompt string
    """
    # Separate correct and incorrect questions
    incorrect_questions = [f for f in labeling_feedback if f.get("is_correct") is False]
    correct_questions = [f for f in labeling_feedback if f.get("is_correct") is True]

    # Format feedback for the prompt
    feedback_lines = []
    for i, item in enumerate(labeling_feedback, 1):
        status = "CORRECT" if item.get("is_correct") else "INCORRECT" if item.get("is_correct") is False else "NOT LABELED"
        line = f"{i}. [{status}] {item.get('question_text', '')}"
        if item.get("feedback_text"):
            line += f"\n   Feedback: {item['feedback_text']}"
        feedback_lines.append(line)

    feedback_text = "\n".join(feedback_lines)

    return f"""You are an expert at optimizing Databricks Genie Space configurations to improve answer accuracy.

## Task
Analyze the Genie Space configuration and labeling feedback to generate specific, field-level optimization suggestions that will help Genie answer questions more accurately.

## Genie Space Configuration
```json
{json.dumps(space_data, indent=2)}
```

## Labeling Feedback
The user labeled {len(labeling_feedback)} benchmark questions:
- {len(correct_questions)} answered correctly by Genie
- {len(incorrect_questions)} answered incorrectly by Genie

{feedback_text}

## Best Practices Checklist
{checklist_content}

## Genie Space Schema
CRITICAL: Your suggested values MUST conform to this schema. Many fields require arrays of strings, not plain strings.
{schema_content}

## Instructions

Generate optimization suggestions that will improve Genie's accuracy, especially for the INCORRECT questions.

**Constraints:**
1. Only suggest modifications to EXISTING fields - do not suggest adding new tables or new array items
2. Use exact JSON paths with NUMERIC indices only (e.g., "instructions.text_instructions[0].content", "data_sources.tables[0].column_configs[2].description"). Do NOT invent query syntax like [find(...)] - only use [0], [1], [2], etc.
3. Prioritize suggestions that directly address incorrect benchmark questions
4. Limit to 10-15 most impactful suggestions
5. CRITICAL: Suggested values MUST match the schema types. Fields like `description`, `content`, `question`, `sql`, `instruction`, `synonyms` must be arrays of strings, e.g., ["value"] not "value"
6. Reference the actual array indices from the provided configuration - count the position (0-indexed) of the element you want to modify

**API Constraints (do not violate):**
- At most 1 text_instruction is allowed per space - do not add more
- SQL fields in filters, expressions, measures must not be empty
- All IDs must be unique within their collection
- Do not suggest adding new items to arrays - only modify existing items

**Valid categories:**
- instruction: Text instruction modifications
- sql_example: Example question-SQL pair modifications
- filter: SQL snippet filter modifications
- expression: SQL snippet expression modifications
- measure: SQL snippet measure modifications
- synonym: Column synonym additions
- join_spec: Join specification modifications
- description: Column/table description modifications

**Priority levels:**
- high: Directly addresses an incorrect benchmark question
- medium: Improves general accuracy based on patterns
- low: Minor enhancement for clarity

Output your suggestions as JSON with this exact structure:
{{
  "suggestions": [
    {{
      "field_path": "exact.json.path[index].field",
      "current_value": <current value from config or null if adding>,
      "suggested_value": <new suggested value>,
      "rationale": "Explanation of why this change helps and which questions it addresses",
      "checklist_reference": "related-checklist-item-id or null",
      "priority": "high" | "medium" | "low",
      "category": "instruction" | "sql_example" | "filter" | "expression" | "measure" | "synonym" | "join_spec" | "description"
    }}
  ],
  "summary": "Brief overall summary of the optimization strategy"
}}

Focus on actionable changes that will measurably improve Genie's ability to answer the types of questions that were marked incorrect."""


def get_synthesis_prompt(
    section_analyses: list[dict],
    is_full_analysis: bool,
) -> str:
    """Build the prompt for cross-sectional synthesis after all sections are analyzed.

    Args:
        section_analyses: List of section analysis results (section_name, checklist, findings, score, summary)
        is_full_analysis: Whether all 10 sections were analyzed

    Returns:
        The formatted prompt string
    """
    # Format section summaries
    section_summaries = []
    for analysis in section_analyses:
        passed = sum(1 for c in analysis.get("checklist", []) if c.get("passed"))
        total = len(analysis.get("checklist", []))
        section_summaries.append(
            f"- **{analysis['section_name']}**: {passed}/{total} passed. {analysis.get('summary', '')}"
        )
    sections_text = "\n".join(section_summaries)

    return f"""You are synthesizing a cross-sectional analysis of a Databricks Genie Space configuration.

## Section Analysis Results
{sections_text}

## Instructions

Based on the section analyses, provide a holistic assessment that:

1. **Identifies compensating strengths**: Where one section's strength makes up for another's weakness.
   - For example, rich metric views can compensate for missing table descriptions
   - Strong example SQLs can compensate for missing join specifications
   - Rich text instructions can compensate for missing snippets

2. **Celebrates what's working well**: Highlight 2-4 strengths worth preserving.

3. **Identifies quick wins**: List 3-5 specific, actionable improvements that would have high impact.

4. **Determines overall assessment**:
   - "good_to_go": The space is well-configured, minor improvements only
   - "quick_wins": The space works but has clear opportunities for improvement
   - "foundation_needed": The space needs fundamental improvements to be effective

Be encouraging but honest. Focus on improvement opportunities rather than failures.

{"Note: This is a partial analysis (not all sections were analyzed). Be tentative in the overall assessment." if not is_full_analysis else ""}

Output your synthesis as JSON with this exact structure:
{{
  "assessment": "good_to_go" | "quick_wins" | "foundation_needed",
  "assessment_rationale": "Brief explanation of the assessment",
  "compensating_strengths": [
    {{
      "covering_section": "section that provides the strength",
      "covered_section": "section being compensated for",
      "explanation": "How the strength compensates"
    }}
  ],
  "celebration_points": [
    "What's working well (2-4 items)"
  ],
  "top_quick_wins": [
    "Specific actionable improvement (3-5 items)"
  ]
}}"""


def get_fix_agent_prompt(
    space_id: str,
    findings: list[str],
    space_config: dict,
) -> str:
    """Build the prompt for the AI fix agent.

    Args:
        space_id: The Genie Space ID
        findings: List of IQ scan findings to fix
        space_config: The current space configuration dict

    Returns:
        Formatted prompt string
    """
    findings_text = "\n".join(f"- {f}" for f in findings) if findings else "No specific findings"
    config_json = json.dumps(space_config, indent=2)

    return f"""You are a Databricks Genie Space configuration repair agent. Your job is to analyze configuration issues and generate specific, targeted fixes.

## Space ID: {space_id}

## Issues Found (from IQ scan):
{findings_text}

## Current Configuration:
```json
{config_json}
```

## Your Task:
Generate a JSON fix plan with specific field-level patches to address the findings above.

For each fix:
1. Identify the exact field path using dot notation (e.g., "instructions.text_instructions[0].content")
2. Specify the new value that resolves the issue
3. Explain why this fix helps

Rules:
- Only fix actual issues from the findings list
- Be conservative - improve existing values rather than replacing entirely
- For missing sections, add minimal but useful content
- Text instructions should explain business context in plain English
- SQL examples should be realistic for the configured tables

Output JSON with this exact structure:
{{
  "patches": [
    {{
      "field_path": "path.to.field[0].subfield",
      "new_value": "the new value to set",
      "rationale": "Why this fixes the issue"
    }}
  ],
  "summary": "Brief summary of all fixes applied"
}}

Generate only the patches needed to address the specific findings. Do not over-engineer."""
