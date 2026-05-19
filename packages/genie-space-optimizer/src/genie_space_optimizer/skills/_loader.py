"""Plan 4 — file-based loader for ``skills/<skill_id>/SKILL.md``.

YAML frontmatter is parsed by a minimal hand-rolled parser (no
PyYAML dependency added) — only ``key: value`` lines, with values
parsed as: ``true``/``false`` -> bool; bare integer -> int; anything
else -> str (stripped, unquoted).
"""
from __future__ import annotations

from pathlib import Path
from typing import Any


_DEFAULT_ROOT = Path(__file__).parent


def _parse_frontmatter(text: str) -> tuple[dict[str, Any], str]:
    """Split a SKILL.md into ``(metadata_dict, body_text)``.

    Frontmatter is bounded by ``---`` lines. Returns ``({}, full_text)``
    when no frontmatter is present (caller should reject when
    frontmatter is required).
    """
    lines = text.splitlines(keepends=True)
    if not lines or lines[0].rstrip("\n") != "---":
        return {}, text
    end_idx: int | None = None
    for i in range(1, len(lines)):
        if lines[i].rstrip("\n") == "---":
            end_idx = i
            break
    if end_idx is None:
        return {}, text
    meta: dict[str, Any] = {}
    for raw in lines[1:end_idx]:
        s = raw.rstrip("\n")
        if not s.strip() or s.startswith("#"):
            continue
        if ":" not in s:
            continue
        k, v = s.split(":", 1)
        k = k.strip()
        v = v.strip()
        # Strip wrapping quotes if present:
        if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
            v = v[1:-1]
        if v.lower() == "true":
            meta[k] = True
        elif v.lower() == "false":
            meta[k] = False
        else:
            try:
                meta[k] = int(v)
            except ValueError:
                meta[k] = v
    body = "".join(lines[end_idx + 1:])
    # Strip the single leading newline that always follows the closing
    # frontmatter marker, so the body's first character is the first
    # character of the prompt:
    if body.startswith("\n"):
        body = body[1:]
    return meta, body


class SkillLoader:
    """Resolves ``skill_id`` to ``(metadata, body)`` from
    ``<root>/<skill_id>/SKILL.md``.

    Caches per-skill_id reads in-process for fast repeated lookups;
    the cache is fine to never invalidate because skill files are
    static at runtime.
    """

    def __init__(self, root: Path | None = None) -> None:
        self._root = Path(root) if root is not None else _DEFAULT_ROOT
        self._cache: dict[str, tuple[dict, str]] = {}

    def _read(self, skill_id: str) -> tuple[dict, str]:
        if skill_id in self._cache:
            return self._cache[skill_id]
        path = self._root / skill_id / "SKILL.md"
        if not path.is_file():
            raise FileNotFoundError(
                f"SKILL.md not found for skill_id={skill_id!r} "
                f"(expected at {path})"
            )
        text = path.read_text(encoding="utf-8")
        meta, body = _parse_frontmatter(text)
        if not meta:
            raise ValueError(
                f"SKILL.md for {skill_id!r} is missing required YAML "
                f"frontmatter (must be bounded by --- lines)"
            )
        self._cache[skill_id] = (meta, body)
        return self._cache[skill_id]

    def load_prompt(
        self, skill_id: str, expected_constant_name: str,
    ) -> str:
        """Return the body text of ``<skill_id>/SKILL.md``.

        Raises ``ValueError`` when the file's
        ``prompt_constant_name`` frontmatter does not match the
        ``expected_constant_name`` argument — this protects against
        accidentally loading the wrong skill into a constant.
        """
        meta, body = self._read(skill_id)
        actual = meta.get("prompt_constant_name")
        if actual != expected_constant_name:
            raise ValueError(
                f"constant name mismatch for skill_id={skill_id!r}: "
                f"frontmatter says {actual!r}, caller expected "
                f"{expected_constant_name!r}"
            )
        return body

    def load_metadata(self, skill_id: str) -> dict:
        meta, _ = self._read(skill_id)
        return dict(meta)


_SKILL_LOADER = SkillLoader()


# ── Plan 2 — reasoning-skill metadata accessors (additive). ───────────
# Below: a typed metadata record, three accessor functions, and three
# attribute assignments that bolt the accessors onto SkillLoader. We
# use the bolt-on pattern (rather than editing the class definition
# above) so this change is purely additive — diff-only inspection of
# the existing class block shows zero modifications. Plan 8 cleanup
# may inline these into the class once all Plan-3/4/5/6/7 skills are
# stable.

import importlib as _plan2_importlib
import json as _plan2_json
from dataclasses import dataclass as _plan2_dataclass


@_plan2_dataclass(frozen=True, slots=True)
class ReasoningSkillMetadata:
    """Typed view of the Plan-2-introduced frontmatter fields.

    All fields are surfaced explicitly so the runner can validate
    them without raw-dict probing. ``examples_dir`` and ``eval_dir``
    are relative paths (joined against the skill's directory at
    lookup time).
    """

    llm_call_kind: str
    output_schema_class: str
    max_tokens: int
    abstain_supported: bool
    examples_dir: str | None
    eval_dir: str | None
    model_override: str | None
    # ── Plan 4 — MLflow Prompt Registry registration name. ──
    # When non-None, register_reasoning_prompts() registers this
    # skill's SKILL.md body to MLflow Prompt Registry under this
    # name (idempotent; MLflow mints a new version only when the
    # template changes). None for template / non-production skills
    # (the Plan 2 smoke-test).
    prompt_registry_name: str | None = None


_PLAN2_MAX_EXAMPLES = 4  # Anthropic: "curate canonical, not laundry list"


def _plan2_coerce_bool(value: object, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return default


def _load_reasoning_metadata(
    self: "SkillLoader", skill_id: str
) -> "ReasoningSkillMetadata | None":
    """Return reasoning-skill metadata, or None for legacy skills.

    A skill is "reasoning" iff its frontmatter has
    ``llm_call_kind: reasoning``. When set, ``output_schema_class``
    and ``max_tokens`` are required; missing required fields raise
    ``ValueError``.
    """
    meta = self.load_metadata(skill_id)
    if meta.get("llm_call_kind") != "reasoning":
        return None
    output_schema_class = meta.get("output_schema_class")
    if not output_schema_class:
        raise ValueError(
            f"skill {skill_id!r} has llm_call_kind=reasoning but is "
            "missing required frontmatter field 'output_schema_class'"
        )
    if "max_tokens" not in meta:
        raise ValueError(
            f"skill {skill_id!r} has llm_call_kind=reasoning but is "
            "missing required frontmatter field 'max_tokens'"
        )
    max_tokens_raw = meta["max_tokens"]
    try:
        max_tokens = int(max_tokens_raw)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"skill {skill_id!r} has non-integer "
            f"max_tokens={max_tokens_raw!r}"
        ) from exc
    return ReasoningSkillMetadata(
        llm_call_kind="reasoning",
        output_schema_class=str(output_schema_class),
        max_tokens=max_tokens,
        abstain_supported=_plan2_coerce_bool(
            meta.get("abstain_supported"), default=False
        ),
        examples_dir=meta.get("examples_dir") or None,
        eval_dir=meta.get("eval_dir") or None,
        model_override=meta.get("model_override") or None,
        # Plan 4: None when absent; non-None when SKILL.md sets it.
        prompt_registry_name=(
            str(meta["prompt_registry_name"])
            if meta.get("prompt_registry_name") else None
        ),
    )


def _load_output_schema_class(self: "SkillLoader", skill_id: str) -> type:
    """Resolve the frontmatter ``output_schema_class`` to the actual
    class.

    Format: ``"package.module:ClassName"``. Raises ``ValueError`` on
    malformed paths and propagates ``ImportError`` /
    ``AttributeError`` when resolution fails.
    """
    rsm = _load_reasoning_metadata(self, skill_id)
    if rsm is None:
        raise ValueError(
            f"skill {skill_id!r} is not a reasoning skill"
        )
    path = rsm.output_schema_class
    if ":" not in path:
        raise ValueError(
            f"output_schema_class for {skill_id!r} must be in form "
            f"'package.module:ClassName'; got {path!r}"
        )
    module_path, class_name = path.split(":", 1)
    module = _plan2_importlib.import_module(module_path)
    return getattr(module, class_name)


def _iter_examples(self: "SkillLoader", skill_id: str):
    """Yield (filename, parsed_json_dict) for every example JSON file.

    The skill's examples_dir frontmatter is interpreted relative to
    the skill folder. When the field is absent or the directory does
    not exist, yields nothing. Enforces ≤4 examples (Anthropic
    context-engineering guidance).
    """
    rsm = _load_reasoning_metadata(self, skill_id)
    examples_dir_rel = (rsm.examples_dir if rsm else None) or "./examples"
    skill_root = self._root / skill_id
    ex_dir = (skill_root / examples_dir_rel).resolve()
    if not ex_dir.is_dir():
        return
    files = sorted(p for p in ex_dir.iterdir() if p.suffix == ".json")
    if len(files) > _PLAN2_MAX_EXAMPLES:
        raise ValueError(
            f"skill {skill_id!r} has {len(files)} example files in "
            f"{examples_dir_rel}/; framework enforces ≤4 canonical "
            "examples per Anthropic context engineering guidance"
        )
    for path in files:
        yield path.name, _plan2_json.loads(
            path.read_text(encoding="utf-8")
        )


SkillLoader.load_reasoning_metadata = _load_reasoning_metadata  # type: ignore[assignment]
SkillLoader.load_output_schema_class = _load_output_schema_class  # type: ignore[assignment]
SkillLoader.iter_examples = _iter_examples  # type: ignore[assignment]
