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
