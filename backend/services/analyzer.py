import logging
import os
import re
import uuid
from collections.abc import Generator
from pathlib import Path

import mlflow
from mlflow.entities import SpanType
from mlflow.genai.agent_server import invoke as invoke_decorator

from backend.checklist_parser import get_checklist_items_for_section, SECTIONS
from backend.services.genie_client import get_serialized_space
from backend.services.llm_utils import call_serving_endpoint, get_llm_model, parse_json_from_llm_response
from backend.models import (
    AgentInput,
    AgentOutput,
    ChecklistItem,
    Finding,
    SectionAnalysis,
    SynthesisResult,
)
from backend.prompts import get_checklist_evaluation_prompt
from backend.synthesizer import synthesize_analysis

logger = logging.getLogger(__name__)

# Check if MLflow experiment is configured before enabling tracing
_mlflow_experiment_id = os.environ.get("MLFLOW_EXPERIMENT_ID", "").strip()
_mlflow_tracing_enabled = False

if _mlflow_experiment_id:
    try:
        mlflow.tracing.enable()
        _mlflow_tracing_enabled = True
        logger.info(f"MLflow tracing enabled with experiment ID: {_mlflow_experiment_id}")
    except Exception as e:
        logger.warning(f"Failed to enable MLflow tracing: {e}. Tracing will be disabled.")
        mlflow.tracing.disable()
else:
    logger.warning(
        "MLFLOW_EXPERIMENT_ID is not set. MLflow tracing is disabled. "
        "Set MLFLOW_EXPERIMENT_ID in app.yaml to enable trace logging."
    )
    mlflow.tracing.disable()


class GenieSpaceAnalyzer:
    """Analyzes Genie Space configurations against best practices."""

    def __init__(self):
        self.model = get_llm_model()
        self._session_id: str | None = None

    def start_session(self) -> str:
        """Start a new analysis session and return the session ID."""
        self._session_id = str(uuid.uuid4())
        return self._session_id

    def end_session(self):
        """Clear the current session."""
        self._session_id = None

    def _get_section_data(self, space: dict, section_path: str) -> dict | list | None:
        """Extract a section from the Genie space by dot-notation path."""
        parts = section_path.split(".")
        current = space
        for part in parts:
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current

    def _create_missing_section_analysis(
        self, section_name: str, full_space: dict | None = None
    ) -> SectionAnalysis:
        """Create analysis for a missing/unconfigured section.

        Args:
            section_name: Name of the missing section
            full_space: The full space data for cross-section checks

        Returns:
            SectionAnalysis with a low score and finding for the missing section
        """
        # Human-readable descriptions for each section
        section_descriptions = {
            "data_sources.tables": "Tables are required for the space to query data",
            "data_sources.metric_views": "Metric views provide pre-computed aggregations for better accuracy",
            "instructions.text_instructions": "Text instructions help Genie understand business context and terminology",
            "instructions.example_question_sqls": "Example SQL queries teach Genie complex query patterns",
            "instructions.sql_functions": "SQL functions register custom UDFs for specialized calculations",
            "instructions.join_specs": "Join specifications help Genie correctly join tables",
            "instructions.sql_snippets.filters": "Filter snippets define common filtering patterns (e.g., 'last quarter')",
            "instructions.sql_snippets.expressions": "Expression snippets define common categorizations or derivations",
            "instructions.sql_snippets.measures": "Measure snippets define standard business metrics",
            "benchmarks.questions": "Benchmark questions help evaluate and improve Genie accuracy",
        }

        description = section_descriptions.get(
            section_name, f"The {section_name} section is not configured"
        )

        # Create checklist items for missing section (all fail)
        checklist_items = get_checklist_items_for_section(section_name)
        checklist = [
            ChecklistItem(
                id=item["id"],
                description=item["description"],
                passed=False,
                details="Section not configured",
            )
            for item in checklist_items
        ]

        # Calculate score based on checklist
        passed_count = sum(1 for item in checklist if item.passed)
        total_count = len(checklist)
        score = round(passed_count / total_count * 10) if total_count > 0 else 0

        finding = Finding(
            category="suggestion",
            severity="medium",
            description=f"Section '{section_name}' is not configured",
            recommendation=f"Consider adding {section_name}. {description}.",
            reference=f"See checklist documentation for {section_name}",
        )

        return SectionAnalysis(
            section_name=section_name,
            checklist=checklist,
            findings=[finding],
            score=score,
            summary=f"This section is not configured. {description}.",
        )

    @mlflow.trace(span_type=SpanType.LLM)
    def analyze_section(
        self,
        section_name: str,
        section_data: dict | list | None,
        full_space: dict | None = None,
    ) -> SectionAnalysis:
        """Analyze a single section using hybrid checklist approach.

        Args:
            section_name: Name of the section to analyze
            section_data: The section data, or None if not configured
            full_space: The full space data for cross-section checks

        Returns:
            SectionAnalysis with checklist items, findings and score
        """
        # Handle missing sections
        if section_data is None:
            return self._create_missing_section_analysis(section_name, full_space)

        # Tag trace with session ID if there's an active trace
        if self._session_id and mlflow.get_current_active_span() is not None:
            try:
                mlflow.update_current_trace(
                    metadata={
                        "mlflow.trace.session": self._session_id,
                    }
                )
            except Exception:
                pass  # Ignore if trace update fails

        # Get all checklist items for this section
        checklist_items = get_checklist_items_for_section(section_name)
        checklist = []
        findings = []

        if checklist_items:
            # Call LLM to evaluate all items
            prompt = get_checklist_evaluation_prompt(
                section_name, section_data, checklist_items
            )

            content = call_serving_endpoint(
                messages=[{"role": "user", "content": prompt}],
                model=self.model,
            )

            result = parse_json_from_llm_response(content)

            # Process LLM evaluations into ChecklistItems
            evaluations = {e["id"]: e for e in result.get("evaluations", [])}
            for item in checklist_items:
                eval_result = evaluations.get(item["id"], {})
                checklist.append(
                    ChecklistItem(
                        id=item["id"],
                        description=item["description"],
                        passed=eval_result.get("passed", False),
                        details=eval_result.get("details"),
                    )
                )

            # Get findings from LLM response
            findings = [Finding(**f) for f in result.get("findings", [])]
            summary = result.get("summary", "")
        else:
            summary = "No checklist items defined for this section."

        # Calculate score based on checklist items
        passed_count = sum(1 for item in checklist if item.passed)
        total_count = len(checklist)
        score = round(passed_count / total_count * 10) if total_count > 0 else 0

        return SectionAnalysis(
            section_name=section_name,
            checklist=checklist,
            findings=findings,
            score=score,
            summary=summary,
        )

    @mlflow.trace(name="predict", span_type=SpanType.AGENT)
    def predict(self, inputs: list[AgentInput]) -> list[AgentOutput]:
        """Main entry point for the agent."""
        self.start_session()
        try:
            outputs = []

            for agent_input in inputs:
                genie_space_id = agent_input.genie_space_id

                # Fetch the Genie space
                with mlflow.start_span(name="fetch_genie_space") as span:
                    span.set_inputs({"genie_space_id": genie_space_id})
                    space = get_serialized_space(genie_space_id)
                    span.set_outputs({"keys": list(space.keys())})

                # Analyze each section sequentially
                analyses = []
                total_score = 0
                section_count = 0

                for section_name in SECTIONS:
                    section_data = self._get_section_data(space, section_name)

                    # Analyze ALL sections (including missing ones)
                    with mlflow.start_span(name=f"analyze_{section_name}") as span:
                        span.set_inputs({"section_name": section_name})
                        analysis = self.analyze_section(
                            section_name, section_data, full_space=space
                        )
                        analyses.append(analysis)
                        total_score += analysis.score
                        section_count += 1
                        span.set_outputs(
                            {
                                "score": analysis.score,
                                "findings_count": len(analysis.findings),
                                "checklist_passed": sum(
                                    1 for c in analysis.checklist if c.passed
                                ),
                                "checklist_total": len(analysis.checklist),
                            }
                        )

                # Run cross-sectional synthesis (full analysis = all 10 sections)
                is_full_analysis = section_count == len(SECTIONS)
                synthesis = None
                if is_full_analysis:
                    with mlflow.start_span(name="synthesize") as span:
                        synthesis = synthesize_analysis(analyses, is_full_analysis)
                        span.set_outputs({
                            "assessment": synthesis.assessment.value,
                            "compensating_strengths_count": len(synthesis.compensating_strengths),
                        })

                overall_score = total_score // section_count if section_count > 0 else 0
                trace_id = (
                    mlflow.get_current_active_span().request_id
                    if mlflow.get_current_active_span()
                    else ""
                )

                outputs.append(
                    AgentOutput(
                        genie_space_id=genie_space_id,
                        analyses=analyses,
                        synthesis=synthesis,
                        overall_score=overall_score,
                        trace_id=trace_id,
                    )
                )

            return outputs
        finally:
            self.end_session()

    def get_sections_with_data(self, space: dict) -> list[tuple[str, dict | list]]:
        """Get list of sections that have data in the space.

        Args:
            space: The serialized Genie space

        Returns:
            List of (section_name, section_data) tuples for sections with data only
        """
        sections_with_data = []
        for section_name in SECTIONS:
            section_data = self._get_section_data(space, section_name)
            if section_data is not None:
                sections_with_data.append((section_name, section_data))
        return sections_with_data

    def get_all_sections(self, space: dict) -> list[tuple[str, dict | list | None]]:
        """Get all sections with their data (or None if missing).

        Args:
            space: The serialized Genie space

        Returns:
            List of (section_name, section_data) tuples for ALL sections.
            section_data is None if the section is not configured.
        """
        return [
            (section_name, self._get_section_data(space, section_name))
            for section_name in SECTIONS
        ]

    def predict_streaming(
        self, agent_input: AgentInput, selected_sections: list[str] | None = None
    ) -> Generator[dict, None, AgentOutput]:
        """Streaming version of predict that yields progress updates.

        Yields dicts with progress info, then returns the final AgentOutput.
        Note: Session should be started before calling this method (e.g., via start_session()).

        Args:
            agent_input: The input containing the genie_space_id
            selected_sections: Optional list of section names to analyze. If None, all sections are analyzed.
        """
        genie_space_id = agent_input.genie_space_id

        # Start session if not already active
        if not self._session_id:
            self.start_session()

        # Create a trace for the entire streaming operation
        with mlflow.start_span(
            name="predict_streaming", span_type=SpanType.AGENT
        ) as root_span:
            root_span.set_inputs({"genie_space_id": genie_space_id})

            try:
                # Fetch the Genie space
                yield {"status": "fetching", "message": "Fetching Genie space..."}
                with mlflow.start_span(name="fetch_genie_space") as span:
                    span.set_inputs({"genie_space_id": genie_space_id})
                    space = get_serialized_space(genie_space_id)
                    span.set_outputs({"keys": list(space.keys())})

                # Determine which sections to analyze
                sections_to_analyze = selected_sections if selected_sections else SECTIONS
                is_full_analysis = set(sections_to_analyze) == set(SECTIONS)

                # Analyze each section sequentially (including missing sections)
                analyses = []
                total_score = 0
                section_count = 0
                total_sections = len(sections_to_analyze)

                # Prepare sections with their data (None for missing sections)
                all_sections = [
                    (name, self._get_section_data(space, name)) for name in sections_to_analyze
                ]

                for i, (section_name, section_data) in enumerate(all_sections, 1):
                    yield {
                        "status": "analyzing",
                        "section": section_name,
                        "current": i,
                        "total": total_sections,
                        "message": f"Analyzing {section_name}...",
                    }

                    with mlflow.start_span(name=f"analyze_{section_name}") as span:
                        span.set_inputs({"section_name": section_name})
                        analysis = self.analyze_section(
                            section_name, section_data, full_space=space
                        )
                        analyses.append(analysis)
                        total_score += analysis.score
                        section_count += 1
                        span.set_outputs(
                            {
                                "score": analysis.score,
                                "findings_count": len(analysis.findings),
                                "checklist_passed": sum(
                                    1 for c in analysis.checklist if c.passed
                                ),
                                "checklist_total": len(analysis.checklist),
                            }
                        )

                # Run cross-sectional synthesis (only for full analysis)
                synthesis = None
                if is_full_analysis:
                    yield {"status": "synthesizing", "message": "Synthesizing cross-sectional insights..."}
                    with mlflow.start_span(name="synthesize") as span:
                        synthesis = synthesize_analysis(analyses, is_full_analysis)
                        span.set_outputs({
                            "assessment": synthesis.assessment.value,
                            "compensating_strengths_count": len(synthesis.compensating_strengths),
                        })

                overall_score = total_score // section_count if section_count > 0 else 0
                trace_id = root_span.request_id if root_span else None

                yield {"status": "complete", "message": "Analysis complete!"}

                # Set outputs on root span
                root_span.set_outputs(
                    {
                        "overall_score": overall_score,
                        "sections_analyzed": section_count,
                        "is_full_analysis": is_full_analysis,
                    }
                )

                return AgentOutput(
                    genie_space_id=genie_space_id,
                    analyses=analyses,
                    synthesis=synthesis,
                    overall_score=overall_score,
                    trace_id=trace_id or "",
                )
            finally:
                self.end_session()


OUTPUT_DIR = Path(__file__).parent.parent / "output"


def _sanitize_filename(name: str) -> str:
    """Sanitize a string for use in filenames.

    Allows only alphanumeric characters, hyphens, underscores, and dots.
    Replaces any other characters with underscores.
    """
    return re.sub(r"[^a-zA-Z0-9\-_.]", "_", name)


def _format_section_name(section_name: str) -> str:
    """Format section name for display in markdown."""
    return section_name.replace("_", " ").replace(".", " → ").title()


def format_analysis_as_markdown(output: AgentOutput) -> str:
    """Convert analysis output to a formatted markdown document.

    Args:
        output: The analysis output to format

    Returns:
        Formatted markdown string
    """
    lines = []

    # Title
    lines.append("# Genie Space Analysis Report")
    lines.append("")
    lines.append(f"**Space ID:** `{output.genie_space_id}`")
    lines.append("")

    # Overall Score
    lines.append("---")
    lines.append("")
    lines.append(f"## Overall Score: {output.overall_score}/10")
    lines.append("")

    # Summary Statistics
    total_findings = sum(len(a.findings) for a in output.analyses)
    high_count = sum(
        1 for a in output.analyses for f in a.findings if f.severity == "high"
    )
    medium_count = sum(
        1 for a in output.analyses for f in a.findings if f.severity == "medium"
    )
    low_count = sum(
        1 for a in output.analyses for f in a.findings if f.severity == "low"
    )

    # Checklist statistics
    total_checklist = sum(len(a.checklist) for a in output.analyses)
    passed_checklist = sum(
        1 for a in output.analyses for c in a.checklist if c.passed
    )

    lines.append("### Summary Statistics")
    lines.append("")
    lines.append(f"- **Checklist Items Passed:** {passed_checklist}/{total_checklist}")
    lines.append(f"- **Total Findings:** {total_findings}")
    lines.append(f"- **High Severity:** {high_count}")
    lines.append(f"- **Medium Severity:** {medium_count}")
    lines.append(f"- **Low Severity:** {low_count}")
    lines.append(f"- **Sections Analyzed:** {len(output.analyses)}")
    lines.append("")

    # Section-by-section breakdown
    lines.append("---")
    lines.append("")
    lines.append("## Section Analysis")
    lines.append("")

    for analysis in output.analyses:
        display_name = _format_section_name(analysis.section_name)

        # Section header with score
        lines.append(f"### {display_name}")
        lines.append("")

        # Checklist progress
        passed = sum(1 for c in analysis.checklist if c.passed)
        total = len(analysis.checklist)
        lines.append(f"**Checklist:** {passed}/{total} items passed")
        lines.append(f"**Score:** {analysis.score}/10")
        lines.append("")

        # Summary
        if analysis.summary:
            lines.append(f"**Summary:** {analysis.summary}")
            lines.append("")

        # Checklist items
        if analysis.checklist:
            lines.append("#### Checklist")
            lines.append("")
            for item in analysis.checklist:
                status = "✓" if item.passed else "✗"
                details = f" - {item.details}" if item.details else ""
                lines.append(f"- {status} {item.description}{details}")
            lines.append("")

        # Findings grouped by severity
        if analysis.findings:
            lines.append("#### Findings")
            lines.append("")

            # Group findings by severity
            findings_by_severity = {"high": [], "medium": [], "low": []}
            for finding in analysis.findings:
                if finding.severity in findings_by_severity:
                    findings_by_severity[finding.severity].append(finding)

            severity_labels = {
                "high": "🔴 High Severity",
                "medium": "🟠 Medium Severity",
                "low": "🔵 Low Severity",
            }

            for severity in ["high", "medium", "low"]:
                severity_findings = findings_by_severity[severity]
                if severity_findings:
                    lines.append(f"**{severity_labels[severity]}**")
                    lines.append("")
                    for finding in severity_findings:
                        lines.append(f"- **{finding.description}**")
                        lines.append(
                            f"  - 💡 *Recommendation:* {finding.recommendation}"
                        )
                        if finding.reference:
                            lines.append(f"  - 📚 *Reference:* {finding.reference}")
                    lines.append("")
        else:
            lines.append("✅ No issues found in this section.")
            lines.append("")

        lines.append("---")
        lines.append("")

    return "\n".join(lines)


def save_analysis_output(output: AgentOutput) -> Path:
    """Save analysis output as a formatted markdown file.

    Args:
        output: The analysis output to save

    Returns:
        Path to the saved markdown file
    """
    OUTPUT_DIR.mkdir(exist_ok=True)

    # Sanitize genie_space_id to prevent path traversal
    safe_id = _sanitize_filename(output.genie_space_id)
    filepath = OUTPUT_DIR / f"analysis_{safe_id}.md"

    # Defense in depth: verify path is within OUTPUT_DIR
    if not filepath.resolve().is_relative_to(OUTPUT_DIR.resolve()):
        raise ValueError("Invalid genie_space_id: path traversal detected")

    markdown_content = format_analysis_as_markdown(output)
    filepath.write_text(markdown_content)
    return filepath


# Lazy initialization of the analyzer
_analyzer: GenieSpaceAnalyzer | None = None


def get_analyzer() -> GenieSpaceAnalyzer:
    """Get or create the analyzer instance."""
    global _analyzer
    if _analyzer is None:
        _analyzer = GenieSpaceAnalyzer()
    return _analyzer


@invoke_decorator()
async def invoke(data: dict) -> dict:
    """Invoke function for the non-conversational agent."""
    analyzer = get_analyzer()
    input_obj = AgentInput(**data)
    result = analyzer.predict([input_obj])[0]
    save_analysis_output(result)
    return result.model_dump()
