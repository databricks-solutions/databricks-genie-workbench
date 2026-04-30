"""Render the app.yaml template used by Databricks Apps deployments."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Mapping

from .workspace_source import write_workspace_file


PLACEHOLDER_RE = re.compile(r"__[A-Z0-9_]+__")


def render_text(template: str, replacements: Mapping[str, str | None]) -> str:
    text = template
    for key, value in replacements.items():
        text = text.replace(f"__{key}__", value or "")

    unresolved = sorted(set(PLACEHOLDER_RE.findall(text)))
    if unresolved:
        raise ValueError(f"Unresolved app.yaml placeholders: {', '.join(unresolved)}")
    return text


def render_app_yaml(
    *,
    template_path: str | Path,
    output_workspace_path: str,
    replacements: Mapping[str, str | None],
    workspace_client,
) -> str:
    rendered = render_text(Path(template_path).read_text(encoding="utf-8"), replacements)
    write_workspace_file(workspace_client, output_workspace_path, rendered.encode("utf-8"))
    return rendered

