"""Optimizer module for generating Genie Space optimization suggestions."""

import logging
from pathlib import Path

import mlflow
from mlflow.entities import SpanType

from backend.services.llm_utils import call_serving_endpoint, get_llm_model, parse_json_from_llm_response
from backend.models import (
    ConfigMergeResponse,
    LabelingFeedbackItem,
    OptimizationResponse,
    OptimizationSuggestion,
)
from backend.prompts import get_optimization_prompt

logger = logging.getLogger(__name__)


class GenieSpaceOptimizer:
    """Generates optimization suggestions for Genie Space configurations."""

    def __init__(self):
        self.model = get_llm_model()
        self._checklist_content: str | None = None
        self._schema_content: str | None = None

    def _get_checklist_content(self) -> str:
        """Load the checklist markdown content."""
        if self._checklist_content is None:
            checklist_path = Path(__file__).parent.parent.parent / "docs" / "checklist-by-schema.md"
            self._checklist_content = checklist_path.read_text()
        return self._checklist_content

    def _get_schema_content(self) -> str:
        """Load the Genie Space schema documentation."""
        if self._schema_content is None:
            schema_path = Path(__file__).parent.parent.parent / "docs" / "genie-space-schema.md"
            self._schema_content = schema_path.read_text()
        return self._schema_content

    @mlflow.trace(span_type=SpanType.LLM)
    def generate_optimizations(
        self,
        space_data: dict,
        labeling_feedback: list[LabelingFeedbackItem],
    ) -> OptimizationResponse:
        """Generate optimization suggestions based on labeling feedback.

        Args:
            space_data: The full Genie Space configuration
            labeling_feedback: List of labeling feedback items from the benchmark session

        Returns:
            OptimizationResponse with suggestions and summary
        """
        # Convert feedback items to dicts for the prompt
        feedback_dicts = [
            {
                "question_text": item.question_text,
                "is_correct": item.is_correct,
                "feedback_text": item.feedback_text,
            }
            for item in labeling_feedback
        ]

        # Get checklist and schema content
        checklist_content = self._get_checklist_content()
        schema_content = self._get_schema_content()

        # Build the prompt
        prompt = get_optimization_prompt(
            space_data=space_data,
            labeling_feedback=feedback_dicts,
            checklist_content=checklist_content,
            schema_content=schema_content,
        )

        # Call the LLM
        content = call_serving_endpoint(
            messages=[{"role": "user", "content": prompt}],
            model=self.model,
            max_tokens=8192,  # Ensure enough tokens for response
        )

        logger.info(f"Raw LLM response length: {len(content)}")

        result = parse_json_from_llm_response(content)

        # Convert to response model
        suggestions = [
            OptimizationSuggestion(**s) for s in result.get("suggestions", [])
        ]
        summary = result.get("summary", "")

        # Get trace ID if available
        trace_id = ""
        if mlflow.get_current_active_span() is not None:
            try:
                trace_id = mlflow.get_current_active_span().request_id or ""
            except Exception:
                pass

        return OptimizationResponse(
            suggestions=suggestions,
            summary=summary,
            trace_id=trace_id,
        )

    def merge_config(
        self,
        space_data: dict,
        suggestions: list[OptimizationSuggestion],
    ) -> ConfigMergeResponse:
        """Merge optimization suggestions into the config programmatically.

        Args:
            space_data: The original Genie Space configuration
            suggestions: List of optimization suggestions to apply

        Returns:
            ConfigMergeResponse with merged config and summary
        """
        import copy
        import re

        # Deep copy to avoid modifying original
        merged = copy.deepcopy(space_data)

        applied_count = 0
        failed_paths = []

        for suggestion in suggestions:
            try:
                self._apply_suggestion(merged, suggestion.field_path, suggestion.suggested_value)
                applied_count += 1
            except Exception as e:
                logger.warning(f"Failed to apply suggestion at {suggestion.field_path}: {e}")
                failed_paths.append(suggestion.field_path)

        # Build summary
        if failed_paths:
            summary = f"Applied {applied_count} of {len(suggestions)} suggestions. Failed paths: {', '.join(failed_paths[:3])}{'...' if len(failed_paths) > 3 else ''}"
        else:
            summary = f"Successfully applied all {applied_count} suggestions to the configuration."

        return ConfigMergeResponse(
            merged_config=merged,
            summary=summary,
            trace_id="",
        )

    def _apply_suggestion(self, config: dict, field_path: str, value: any) -> None:
        """Apply a single suggestion to the config at the given path.

        Supports paths like:
        - "instructions.text_instructions[0].content"
        - "data_sources.tables[2].columns[0].synonyms"
        """
        import re

        # Parse path into segments
        # Split on dots, but handle array indices
        segments = []
        for part in field_path.split("."):
            # Check for array index like "text_instructions[0]"
            match = re.match(r"^(.+?)\[(\d+)\]$", part)
            if match:
                segments.append(match.group(1))  # key name
                segments.append(int(match.group(2)))  # array index
            else:
                segments.append(part)

        # Navigate to parent and set value
        current = config
        for i, segment in enumerate(segments[:-1]):
            if isinstance(segment, int):
                # Array index
                if not isinstance(current, list) or segment >= len(current):
                    raise ValueError(f"Invalid array index {segment} at path position {i}")
                current = current[segment]
            else:
                # Object key
                if not isinstance(current, dict):
                    raise ValueError(f"Expected dict at {segment}, got {type(current)}")
                if segment not in current:
                    # Create missing intermediate objects
                    next_segment = segments[i + 1] if i + 1 < len(segments) else None
                    current[segment] = [] if isinstance(next_segment, int) else {}
                current = current[segment]

        # Set the final value
        final_key = segments[-1]
        if isinstance(final_key, int):
            if not isinstance(current, list):
                raise ValueError(f"Expected list for index {final_key}")
            # Extend list if needed
            while len(current) <= final_key:
                current.append(None)
            current[final_key] = value
        else:
            if not isinstance(current, dict):
                raise ValueError(f"Expected dict for key {final_key}")
            current[final_key] = value


# Lazy initialization
_optimizer: GenieSpaceOptimizer | None = None


def get_optimizer() -> GenieSpaceOptimizer:
    """Get or create the optimizer instance."""
    global _optimizer
    if _optimizer is None:
        _optimizer = GenieSpaceOptimizer()
    return _optimizer
