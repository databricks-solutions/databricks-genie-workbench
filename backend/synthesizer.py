"""Cross-sectional synthesis for Genie Space analysis.

Performs LLM-based synthesis after all sections are analyzed to identify
compensating strengths, celebration points, and quick wins.
"""

import logging

import mlflow
from mlflow.entities import SpanType

from backend.services.llm_utils import call_serving_endpoint, get_llm_model, parse_json_from_llm_response
from backend.models import (
    AssessmentCategory,
    CompensatingStrength,
    SectionAnalysis,
    SynthesisResult,
)
from backend.prompts import get_synthesis_prompt

logger = logging.getLogger(__name__)


def synthesize_analysis(
    analyses: list[SectionAnalysis],
    is_full_analysis: bool,
) -> SynthesisResult:
    """Synthesize cross-sectional analysis results.

    Args:
        analyses: List of section analysis results
        is_full_analysis: Whether all 10 sections were analyzed

    Returns:
        SynthesisResult with assessment, compensating strengths, etc.
    """
    # Convert analyses to dict format for prompt
    section_analyses_dict = [
        {
            "section_name": a.section_name,
            "checklist": [c.model_dump() for c in a.checklist],
            "findings": [f.model_dump() for f in a.findings],
            "score": a.score,
            "summary": a.summary,
        }
        for a in analyses
    ]

    prompt = get_synthesis_prompt(
        section_analyses=section_analyses_dict,
        is_full_analysis=is_full_analysis,
    )

    with mlflow.start_span(name="synthesize_analysis", span_type=SpanType.LLM) as span:
        span.set_inputs({
            "sections_analyzed": len(analyses),
            "is_full_analysis": is_full_analysis,
        })

        content = call_serving_endpoint(
            messages=[{"role": "user", "content": prompt}],
            model=get_llm_model(),
        )

        result = parse_json_from_llm_response(content)

        span.set_outputs({
            "assessment": result.get("assessment"),
            "compensating_strengths_count": len(result.get("compensating_strengths", [])),
            "celebration_points_count": len(result.get("celebration_points", [])),
            "quick_wins_count": len(result.get("top_quick_wins", [])),
        })

    # Parse assessment category
    assessment_str = result.get("assessment", "quick_wins")
    try:
        assessment = AssessmentCategory(assessment_str)
    except ValueError:
        logger.warning(f"Invalid assessment category: {assessment_str}, defaulting to quick_wins")
        assessment = AssessmentCategory.QUICK_WINS

    # Parse compensating strengths
    compensating_strengths = [
        CompensatingStrength(
            covering_section=cs.get("covering_section", ""),
            covered_section=cs.get("covered_section", ""),
            explanation=cs.get("explanation", ""),
        )
        for cs in result.get("compensating_strengths", [])
    ]

    return SynthesisResult(
        assessment=assessment,
        assessment_rationale=result.get("assessment_rationale", ""),
        compensating_strengths=compensating_strengths,
        celebration_points=result.get("celebration_points", []),
        top_quick_wins=result.get("top_quick_wins", []),
    )
