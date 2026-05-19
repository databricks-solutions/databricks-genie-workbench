"""Plan 4 Task 11 — register_reasoning_prompts helper."""
from __future__ import annotations

from unittest.mock import MagicMock, patch


def _make_register_prompt_stub() -> MagicMock:
    stub = MagicMock()
    stub.return_value = MagicMock(version=1, name="probe")
    return stub


def test_helper_calls_register_prompt_for_every_reasoning_skill() -> None:
    """Three reasoning skills at this point; _reference_smoke_test
    has no prompt_registry_name so it's skipped."""
    from genie_space_optimizer.optimization import llm_prompt_registry

    register_prompt_stub = _make_register_prompt_stub()
    with patch.object(
        llm_prompt_registry, "_mlflow_register_prompt",
        register_prompt_stub,
    ):
        result = llm_prompt_registry.register_reasoning_prompts()

    skills_registered = {
        c.kwargs.get("name") for c in register_prompt_stub.call_args_list
    }
    assert "gso_reasoning_failure_clustering" in skills_registered
    assert "gso_reasoning_rca_evidence_extraction" in skills_registered
    assert result["registered"] == sorted(skills_registered)
    assert result["skipped_no_registry_name"] >= 0
    assert result["mlflow_unavailable"] is False


def test_helper_passes_skill_body_as_template() -> None:
    from genie_space_optimizer.optimization import llm_prompt_registry
    from genie_space_optimizer.skills._loader import _SKILL_LOADER

    expected_body = _SKILL_LOADER.load_prompt(
        "failure_clustering",
        expected_constant_name="FAILURE_CLUSTERING_PROMPT",
    )

    register_prompt_stub = _make_register_prompt_stub()
    with patch.object(
        llm_prompt_registry, "_mlflow_register_prompt",
        register_prompt_stub,
    ):
        llm_prompt_registry.register_reasoning_prompts()

    failure_clustering_call = next(
        c for c in register_prompt_stub.call_args_list
        if c.kwargs.get("name") == "gso_reasoning_failure_clustering"
    )
    assert failure_clustering_call.kwargs["template"] == expected_body
    assert "Plan 4" in failure_clustering_call.kwargs["commit_message"]
    assert failure_clustering_call.kwargs["tags"] == {
        "skill_id": "failure-clustering",
        "type": "reasoning_skill_prompt",
        "plan": "gso_plan_4",
    }


def test_helper_skips_skills_without_prompt_registry_name_field() -> None:
    """Plan 2's smoke-test skill does not carry prompt_registry_name."""
    from genie_space_optimizer.optimization import llm_prompt_registry

    register_prompt_stub = _make_register_prompt_stub()
    with patch.object(
        llm_prompt_registry, "_mlflow_register_prompt",
        register_prompt_stub,
    ):
        result = llm_prompt_registry.register_reasoning_prompts()

    registered_names = {
        c.kwargs.get("name") for c in register_prompt_stub.call_args_list
    }
    assert all(
        "smoke_test" not in (name or "") and "smoke-test" not in (name or "")
        for name in registered_names
    )
    assert result["skipped_no_registry_name"] >= 1


def test_helper_returns_mlflow_unavailable_when_import_fails() -> None:
    """When mlflow.genai.register_prompt is missing, the helper
    returns mlflow_unavailable=True — never raises."""
    from genie_space_optimizer.optimization import llm_prompt_registry

    with patch.object(
        llm_prompt_registry, "_mlflow_register_prompt", None,
    ):
        result = llm_prompt_registry.register_reasoning_prompts()
    assert result["mlflow_unavailable"] is True
    assert result["registered"] == []


def test_helper_tolerates_per_skill_registration_failure() -> None:
    """One skill's register_prompt call raises → helper logs and
    continues with the next skill; never raises."""
    from genie_space_optimizer.optimization import llm_prompt_registry

    def _selective_raise(*, name, **_kwargs):
        if name == "gso_reasoning_failure_clustering":
            raise RuntimeError("PERMISSION_DENIED")
        return MagicMock(version=1)

    register_prompt_stub = MagicMock(side_effect=_selective_raise)
    with patch.object(
        llm_prompt_registry, "_mlflow_register_prompt",
        register_prompt_stub,
    ):
        result = llm_prompt_registry.register_reasoning_prompts()

    assert result["failed_skills"]
    failed_names = {f["name"] for f in result["failed_skills"]}
    assert "gso_reasoning_failure_clustering" in failed_names
    assert "gso_reasoning_rca_evidence_extraction" in result["registered"]


def test_helper_is_idempotent_across_repeated_invocations() -> None:
    """Calling the helper twice issues the calls twice — by design."""
    from genie_space_optimizer.optimization import llm_prompt_registry

    register_prompt_stub = _make_register_prompt_stub()
    with patch.object(
        llm_prompt_registry, "_mlflow_register_prompt",
        register_prompt_stub,
    ):
        result1 = llm_prompt_registry.register_reasoning_prompts()
        result2 = llm_prompt_registry.register_reasoning_prompts()

    assert result1["registered"] == result2["registered"]
    assert register_prompt_stub.call_count == 2 * len(result1["registered"])
