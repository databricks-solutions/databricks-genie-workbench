"""Plan 4 — register reasoning-skill prompts to MLflow Prompt
Registry.

One walk per job startup (idempotent — MLflow mints a new version
only when the template changes). Modeled after the existing pattern
in ``optimization/synthesis.py`` (``register_synthesis_prompt``):

  * Try-import ``mlflow.genai.register_prompt``; if unavailable,
    no-op.
  * For each reasoning skill with a ``prompt_registry_name``
    frontmatter field, call
    ``register_prompt(name=, template=, commit_message=, tags=)``
    with the SKILL.md body as the template.
  * Per-skill failure logs and continues — never raises.

Returns a result dict so callers (harness startup, tests) can
surface the outcome without parsing logs.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Callable

from genie_space_optimizer.skills._loader import _SKILL_LOADER

logger = logging.getLogger(__name__)


def _resolve_mlflow_register_prompt() -> Callable | None:
    """Return ``mlflow.genai.register_prompt`` or ``None`` if mlflow
    is unavailable / the GenAI namespace lacks ``register_prompt``."""
    try:
        import mlflow  # noqa: PLC0415
    except Exception:  # noqa: BLE001
        return None
    register = getattr(
        getattr(mlflow, "genai", None), "register_prompt", None,
    )
    if not callable(register):
        return None
    return register


# Module-level binding so tests can patch ``_mlflow_register_prompt``
# to inject a stub without touching the mlflow package itself.
_mlflow_register_prompt: Callable | None = _resolve_mlflow_register_prompt()


def _discover_reasoning_skill_ids() -> list[str]:
    """Walk the skills directory for folders that contain a SKILL.md
    with ``llm_call_kind: reasoning``."""
    root = Path(_SKILL_LOADER._root)
    skill_ids: list[str] = []
    for entry in sorted(root.iterdir()):
        if not entry.is_dir():
            continue
        if not (entry / "SKILL.md").exists():
            continue
        try:
            rsm = _SKILL_LOADER.load_reasoning_metadata(entry.name)
        except Exception:  # noqa: BLE001
            continue
        if rsm is not None:
            skill_ids.append(entry.name)
    return skill_ids


def register_reasoning_prompts() -> dict[str, Any]:
    """Register every reasoning skill's SKILL.md body to MLflow
    Prompt Registry.

    Idempotent (MLflow versions on template change). Tolerant of
    every failure mode (missing mlflow, per-skill registration
    errors). Returns a result dict suitable for logging and
    assertion in tests:

      {
        "registered": ["gso_reasoning_rca_evidence_extraction", ...],
        "skipped_no_registry_name": <int>,
        "failed_skills": [{"name": "...", "error": "..."}, ...],
        "mlflow_unavailable": True|False,
      }
    """
    if _mlflow_register_prompt is None:
        logger.info(
            "register_reasoning_prompts: mlflow.genai.register_prompt "
            "unavailable; skipping registration (runtime still loads "
            "bodies via _SKILL_LOADER)"
        )
        return {
            "registered": [],
            "skipped_no_registry_name": 0,
            "failed_skills": [],
            "mlflow_unavailable": True,
        }

    registered: list[str] = []
    failed: list[dict[str, str]] = []
    skipped = 0

    for skill_id in _discover_reasoning_skill_ids():
        rsm = _SKILL_LOADER.load_reasoning_metadata(skill_id)
        if rsm is None or not rsm.prompt_registry_name:
            skipped += 1
            continue
        meta = _SKILL_LOADER.load_metadata(skill_id)
        prompt_constant = str(meta.get("prompt_constant_name") or "")
        try:
            template = _SKILL_LOADER.load_prompt(
                skill_id, expected_constant_name=prompt_constant,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning(
                "register_reasoning_prompts.load_failed "
                "skill_id=%s err=%s",
                skill_id, exc,
            )
            failed.append(
                {"name": rsm.prompt_registry_name, "error": str(exc)[:300]}
            )
            continue

        try:
            _mlflow_register_prompt(
                name=rsm.prompt_registry_name,
                template=template,
                commit_message=(
                    f"Plan 4 registered prompt body for "
                    f"skill_id={skill_id!r}; idempotent — MLflow "
                    "mints a new version only when the template "
                    "changes."
                ),
                tags={
                    "skill_id": str(meta.get("skill_id") or skill_id),
                    "type": "reasoning_skill_prompt",
                    "plan": "gso_plan_4",
                },
            )
            registered.append(rsm.prompt_registry_name)
        except Exception as exc:  # noqa: BLE001 — never raise
            logger.warning(
                "register_reasoning_prompts.register_failed "
                "name=%s err=%s",
                rsm.prompt_registry_name, exc,
            )
            failed.append({
                "name": rsm.prompt_registry_name,
                "error": str(exc)[:300],
            })

    return {
        "registered": sorted(registered),
        "skipped_no_registry_name": skipped,
        "failed_skills": failed,
        "mlflow_unavailable": False,
    }
