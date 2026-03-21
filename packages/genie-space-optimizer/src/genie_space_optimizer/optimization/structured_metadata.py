"""Structured metadata schema for Genie Space descriptions.

Provides parse/render/update utilities so that table, column, metric view,
and function descriptions follow a predictable plain-text format with
ALL-CAPS section headers (e.g. ``PURPOSE:\\nvalue``).  Legacy Markdown
``**Section:** value`` descriptions are still parsed for backward
compatibility and re-rendered in the new format on the next write.

Each lever owns specific sections, preventing collateral damage when one
lever updates metadata that another lever depends on.
"""

from __future__ import annotations

import logging
import re
from typing import Literal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Section Templates per Entity Type
# ---------------------------------------------------------------------------

TABLE_DESCRIPTION_SECTIONS: list[str] = [
    "purpose", "best_for", "grain", "scd", "relationships",
]

COLUMN_DIMENSION_SECTIONS: list[str] = [
    "definition", "values", "synonyms",
]

COLUMN_MEASURE_SECTIONS: list[str] = [
    "definition", "aggregation", "grain_note", "synonyms",
]

COLUMN_KEY_SECTIONS: list[str] = [
    "definition", "join", "synonyms",
]

FUNCTION_SECTIONS: list[str] = [
    "purpose", "best_for", "use_instead_of", "parameters", "example",
]

MV_TABLE_SECTIONS: list[str] = [
    "purpose", "best_for", "grain", "important_filters",
]

ENTITY_TYPE_TEMPLATES: dict[str, list[str]] = {
    "table": TABLE_DESCRIPTION_SECTIONS,
    "column_dim": COLUMN_DIMENSION_SECTIONS,
    "column_measure": COLUMN_MEASURE_SECTIONS,
    "column_key": COLUMN_KEY_SECTIONS,
    "function": FUNCTION_SECTIONS,
    "mv_table": MV_TABLE_SECTIONS,
}

SECTION_LABELS: dict[str, str] = {
    "purpose": "Purpose",
    "best_for": "Best for",
    "grain": "Grain",
    "scd": "SCD",
    "relationships": "Relationships",
    "definition": "Definition",
    "values": "Values",
    "aggregation": "Aggregation",
    "grain_note": "Grain note",
    "join": "Join",
    "synonyms": "Synonyms",
    "use_instead_of": "Use instead of",
    "parameters": "Parameters",
    "example": "Example",
    "important_filters": "Important filters",
}

_LABEL_TO_KEY: dict[str, str] = {v.lower(): k for k, v in SECTION_LABELS.items()}
_UPPER_LABEL_TO_KEY: dict[str, str] = {v.upper(): k for k, v in SECTION_LABELS.items()}

# ---------------------------------------------------------------------------
# Lever Section Ownership
# ---------------------------------------------------------------------------

LEVER_SECTION_OWNERSHIP: dict[int, set[str]] = {
    0: {
        "purpose", "best_for", "grain", "scd", "relationships",
        "definition", "values", "synonyms", "aggregation", "grain_note",
        "join", "important_filters", "use_instead_of", "parameters", "example",
    },
    1: {"purpose", "best_for", "grain", "scd", "definition", "values", "synonyms"},
    2: {"definition", "values", "aggregation", "grain_note", "important_filters", "synonyms"},
    3: {"purpose", "best_for", "use_instead_of", "parameters", "example"},
    4: {"relationships", "join"},
    5: set(),
}

# Regex that matches legacy ``**Label:** rest-of-line`` at start of a line
_SECTION_RE = re.compile(
    r"^\*\*(?P<label>[^*:]+?):\*\*\s*(?P<value>.*)$",
    re.MULTILINE,
)

# Regex that matches plain-text ALL-CAPS headers.  Supports both
# ``LABEL:\n`` (value on subsequent lines) and ``LABEL: value`` (inline).
_PLAINTEXT_SECTION_RE = re.compile(
    r"^(?P<label>[A-Z][A-Z ]+?):\s*(?P<value>.*)$",
)

_SORTED_UPPER_LABELS: list[str] = sorted(
    list(_UPPER_LABEL_TO_KEY.keys()), key=lambda s: len(s), reverse=True
)
_INJECT_NL_RE = re.compile(
    r"(?<=\S) +(?=(?:"
    + "|".join(re.escape(k) for k in _SORTED_UPPER_LABELS)
    + r"):)",
)

_EMBEDDED_HEADER_PATTERN = re.compile(
    r"(?:^|\s)(?:"
    + "|".join(re.escape(k) for k in _SORTED_UPPER_LABELS)
    + r"):",
)


def _contains_embedded_headers(section_key: str, value: str) -> list[str]:
    """Return list of OTHER section headers found embedded in *value*.

    Only flags headers that differ from the section being updated so that
    e.g. ``"definition": "DEFINITION: ..."`` is not a false positive.
    """
    own_label = SECTION_LABELS.get(section_key, "").upper()
    found: list[str] = []
    for m in _EMBEDDED_HEADER_PATTERN.finditer(value):
        header = m.group().strip().rstrip(":")
        if header != own_label:
            found.append(header)
    return found


# ---------------------------------------------------------------------------
# Numeric / measure heuristics (mirrored from config.py to avoid circular)
# ---------------------------------------------------------------------------

_NUMERIC_TYPES = {
    "DOUBLE", "FLOAT", "DECIMAL", "INT", "INTEGER", "BIGINT",
    "SMALLINT", "TINYINT", "LONG", "SHORT", "BYTE", "NUMBER",
}

_MEASURE_PREFIXES = (
    "avg_", "sum_", "count_", "total_", "pct_", "ratio_",
    "min_", "max_", "num_", "mean_", "median_", "stddev_",
)


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------

def parse_structured_description(text: str | list[str] | None) -> dict[str, str]:
    """Parse structured description sections from a description string.

    Supports three formats:

    * **Legacy Markdown**: ``**Purpose:** value`` (inline on one line)
    * **Plain-text ALL-CAPS (multi-line)**: ``PURPOSE:\\nvalue`` (header on
      own line, value on subsequent lines)
    * **Plain-text ALL-CAPS (inline)**: ``PURPOSE: value`` (header and value
      on the same line)

    Returns a dict mapping section keys (e.g. ``"purpose"``, ``"definition"``)
    to their values.  Any text that does not belong to a recognized section is
    stored under the ``"_preamble"`` key.

    Parsing an already-structured description and re-rendering yields the
    same output (idempotent).
    """
    if text is None:
        return {}
    if isinstance(text, list):
        text = "\n".join(text)
    text = text.strip()
    if not text:
        return {}

    text = _INJECT_NL_RE.sub("\n", text)

    sections: dict[str, str] = {}
    preamble_lines: list[str] = []
    current_key: str | None = None
    current_lines: list[str] = []

    def _flush() -> None:
        nonlocal current_key, current_lines
        if current_key is not None:
            sections[current_key] = "\n".join(current_lines).strip()
        elif current_lines:
            preamble_lines.extend(current_lines)
        current_key = None
        current_lines = []

    for line in text.split("\n"):
        m_md = _SECTION_RE.match(line)
        m_pt = _PLAINTEXT_SECTION_RE.match(line) if not m_md else None

        if m_md:
            _flush()
            label = m_md.group("label").strip().lower()
            current_key = _LABEL_TO_KEY.get(label)
            if current_key is None:
                preamble_lines.append(line)
                current_key = None
                current_lines = []
            else:
                current_lines = [m_md.group("value").strip()]
        elif m_pt:
            label_upper = m_pt.group("label").strip()
            resolved_key = _UPPER_LABEL_TO_KEY.get(label_upper)
            if resolved_key is not None:
                _flush()
                current_key = resolved_key
                inline_val = m_pt.group("value").strip()
                current_lines = [inline_val] if inline_val else []
            else:
                if current_key is not None:
                    current_lines.append(line)
                else:
                    preamble_lines.append(line)
        else:
            if current_key is not None:
                current_lines.append(line)
            else:
                preamble_lines.append(line)

    _flush()

    preamble = "\n".join(preamble_lines).strip()
    if preamble:
        sections["_preamble"] = preamble

    return sections


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------

EntityType = Literal[
    "table", "column_dim", "column_measure", "column_key", "function", "mv_table",
]


def render_structured_description(
    sections: dict[str, str],
    entity_type: EntityType,
) -> list[str]:
    """Render sections into the Genie API ``description`` format (``list[str]``).

    Outputs plain-text with ALL-CAPS section headers on their own line,
    value on the next line, and a blank line separator between sections::

        PURPOSE:
        SCD Type 2 property dimension...

        BEST FOR:
        Property lookups, pricing analysis.

    Preserves ``_preamble`` at the top if present.
    """
    template = ENTITY_TYPE_TEMPLATES.get(entity_type, [])
    lines: list[str] = []

    preamble = sections.get("_preamble", "").strip()
    if preamble:
        lines.append(preamble)
        lines.append("")

    for section_key in template:
        value = sections.get(section_key, "").strip()
        if not value:
            continue
        label = SECTION_LABELS[section_key]
        lines.append(f"{label.upper()}:")
        lines.append(value)
        lines.append("")

    if lines and lines[-1] == "":
        lines.pop()

    return lines if lines else [""]


def _infer_entity_type_from_sections(sections: dict[str, str]) -> EntityType:
    """Infer the entity type from which section keys are present."""
    keys = set(sections) - {"_preamble"}
    if keys & {"use_instead_of", "parameters", "example"}:
        return "function"
    if keys & {"important_filters"} and "scd" not in keys:
        return "mv_table"
    if keys & {"purpose", "best_for", "grain", "scd", "relationships"}:
        return "table"
    if keys & {"aggregation", "grain_note"}:
        return "column_measure"
    if keys & {"join"}:
        return "column_key"
    return "column_dim"


def deduplicate_structured_description(text: str | list[str] | None) -> str:
    """Parse and re-render a structured description, collapsing duplicates.

    When descriptions are updated by successive lever applications they can
    accumulate repeated section headers (e.g. two PURPOSE blocks).  Since
    :func:`parse_structured_description` stores sections in a dict, later
    occurrences of the same key overwrite earlier ones.  Re-rendering thus
    produces a clean, deduplicated description.

    Returns the original text unchanged if no structured sections are found.
    """
    if text is None:
        return ""
    if isinstance(text, list):
        text = "\n".join(text)
    text = text.strip()
    if not text:
        return ""
    sections = parse_structured_description(text)
    if not sections or not any(k != "_preamble" for k in sections):
        return text
    preamble = sections.pop("_preamble", "")
    etype = _infer_entity_type_from_sections(sections)
    rendered_lines = render_structured_description(sections, etype)
    result = "\n".join(rendered_lines)
    if not result.strip() and preamble:
        return preamble
    return result


# ---------------------------------------------------------------------------
# Updater
# ---------------------------------------------------------------------------

class LeverOwnershipError(ValueError):
    """Raised when a lever attempts to modify a section it does not own."""


def update_section(
    current_description: str | list[str] | None,
    section: str,
    new_value: str,
    lever: int,
    entity_type: EntityType,
) -> list[str]:
    """Update one section of a structured description.

    Validates that *lever* owns *section* via ``LEVER_SECTION_OWNERSHIP``,
    parses the current description, replaces just that section, and
    re-renders.

    Returns the new description as a ``list[str]`` suitable for the Genie API.

    Raises ``LeverOwnershipError`` if the lever does not own the section
    or if the value embeds other section headers inline.
    """
    allowed = LEVER_SECTION_OWNERSHIP.get(lever, set())
    if section not in allowed:
        raise LeverOwnershipError(
            f"Lever {lever} cannot modify section '{section}' "
            f"(allowed: {sorted(allowed)})"
        )

    embedded = _contains_embedded_headers(section, new_value)
    if embedded:
        raise LeverOwnershipError(
            f"Lever {lever}: section '{section}' value embeds other headers "
            f"{embedded} inline. Each section must be a separate key."
        )

    sections = parse_structured_description(current_description)
    sections[section] = new_value
    return render_structured_description(sections, entity_type)


def update_sections(
    current_description: str | list[str] | None,
    updates: dict[str, str],
    lever: int,
    entity_type: EntityType,
) -> list[str]:
    """Update multiple sections at once, validating lever ownership for each.

    *updates* maps section keys to their new values.  Sections that the lever
    does not own are silently skipped (with a warning) rather than rejecting
    the entire patch.  Only raises ``LeverOwnershipError`` when *every*
    requested section is locked.
    """
    allowed = LEVER_SECTION_OWNERSHIP.get(lever, set())
    applicable = {k: v for k, v in updates.items() if k in allowed}
    skipped = sorted(k for k in updates if k not in allowed)
    if skipped:
        logger.warning(
            "Lever %d: skipping locked sections %s (allowed: %s)",
            lever, skipped, sorted(allowed),
        )

    clean_applicable: dict[str, str] = {}
    for key, new_val in applicable.items():
        embedded = _contains_embedded_headers(key, new_val)
        if embedded:
            logger.warning(
                "Lever %d: rejecting section '%s' — value embeds headers %s inline. "
                "Each section must be a separate key.",
                lever, key, embedded,
            )
        else:
            clean_applicable[key] = new_val
    applicable = clean_applicable

    if not applicable:
        raise LeverOwnershipError(
            f"Lever {lever} cannot modify any of the requested sections "
            f"{sorted(updates)} (allowed: {sorted(allowed)})"
        )

    sections = parse_structured_description(current_description)

    _INFO_LOSS_THRESHOLD = 0.7
    _MIN_EXISTING_LEN = 20
    for key, new_val in applicable.items():
        old_val = sections.get(key, "")
        if (
            old_val
            and new_val
            and len(old_val.strip()) > _MIN_EXISTING_LEN
            and len(new_val.strip()) < len(old_val.strip()) * _INFO_LOSS_THRESHOLD
        ):
            logger.warning(
                "Section '%s' would lose content (%d -> %d chars) — merging instead of replacing",
                key, len(old_val.strip()), len(new_val.strip()),
            )
            applicable[key] = f"{old_val.rstrip('. ')}. {new_val.lstrip()}"

    sections.update(applicable)
    return render_structured_description(sections, entity_type)


# ---------------------------------------------------------------------------
# Column Classifier
# ---------------------------------------------------------------------------

def classify_column(
    col_name: str,
    data_type: str,
    *,
    is_in_metric_view: bool = False,
    enable_entity_matching: bool = False,
) -> Literal["dimension", "measure", "key"]:
    """Classify a column as dimension, measure, or key.

    Uses naming conventions, data type, and contextual flags to determine
    which structured description template to apply.
    """
    lower = col_name.lower()
    if lower.endswith("_key") or lower.endswith("_id") or lower == "id":
        return "key"

    dt_upper = (data_type or "").upper().split("(")[0].strip()
    if dt_upper in _NUMERIC_TYPES:
        if any(lower.startswith(p) for p in _MEASURE_PREFIXES):
            return "measure"
        if is_in_metric_view and not enable_entity_matching:
            return "measure"

    if enable_entity_matching:
        return "dimension"

    if dt_upper in _NUMERIC_TYPES and not enable_entity_matching:
        return "measure"

    return "dimension"


def entity_type_for_column(
    col_name: str,
    data_type: str,
    *,
    is_in_metric_view: bool = False,
    enable_entity_matching: bool = False,
) -> EntityType:
    """Return the ``EntityType`` string for a column classification."""
    kind = classify_column(
        col_name, data_type,
        is_in_metric_view=is_in_metric_view,
        enable_entity_matching=enable_entity_matching,
    )
    _map: dict[str, EntityType] = {
        "dimension": "column_dim",
        "measure": "column_measure",
        "key": "column_key",
    }
    return _map[kind]


# ---------------------------------------------------------------------------
# Synonyms Helpers
# ---------------------------------------------------------------------------

def extract_synonyms_section(sections: dict[str, str]) -> list[str]:
    """Parse the ``synonyms`` section into a list of individual terms.

    The section stores synonyms as a comma-separated string,
    e.g. ``"store id, location number"``.
    """
    raw = sections.get("synonyms", "").strip()
    if not raw:
        return []
    return [s.strip() for s in raw.split(",") if s.strip()]


def format_synonyms_section(synonyms: list[str]) -> str:
    """Format a list of synonym terms into the structured section value."""
    return ", ".join(s.strip() for s in synonyms if s.strip())


def merge_synonyms(existing: list[str], proposed: list[str]) -> list[str]:
    """Merge proposed synonyms into an existing list, avoiding duplicates."""
    seen = {s.lower() for s in existing}
    merged = list(existing)
    for s in proposed:
        if s.strip() and s.strip().lower() not in seen:
            merged.append(s.strip())
            seen.add(s.strip().lower())
    return merged


# ---------------------------------------------------------------------------
# Convenience: format a column's current description as a structured prompt
# ---------------------------------------------------------------------------

def format_column_for_prompt(
    col_name: str,
    description: str | list[str] | None,
    synonyms: list[str] | None,
    data_type: str,
    uc_comment: str = "",
    *,
    is_in_metric_view: bool = False,
    enable_entity_matching: bool = False,
) -> str:
    """Format a column's current metadata as structured sections for the LLM.

    Used to build the "current state" portion of a slot-filling prompt.
    Shows each section with its current value (or ``(empty)`` if not set).
    """
    etype = entity_type_for_column(
        col_name, data_type,
        is_in_metric_view=is_in_metric_view,
        enable_entity_matching=enable_entity_matching,
    )
    template_sections = ENTITY_TYPE_TEMPLATES[etype]

    desc_text = description
    if isinstance(desc_text, list):
        desc_text = "\n".join(desc_text)
    if not desc_text and uc_comment:
        desc_text = uc_comment

    sections = parse_structured_description(desc_text)

    if synonyms:
        existing_syn = extract_synonyms_section(sections)
        all_syns = merge_synonyms(existing_syn, synonyms)
        sections["synonyms"] = format_synonyms_section(all_syns)

    kind = classify_column(
        col_name, data_type,
        is_in_metric_view=is_in_metric_view,
        enable_entity_matching=enable_entity_matching,
    )

    lines: list[str] = [
        f"Column: `{col_name}` ({data_type or 'unknown'}) [type: {kind}]",
    ]
    for section_key in template_sections:
        label = SECTION_LABELS[section_key]
        value = sections.get(section_key, "").strip()
        lines.append(f"  **{label}:** {value if value else '(empty)'}")

    preamble = sections.get("_preamble", "").strip()
    if preamble:
        lines.append(f"  [Legacy text]: {preamble}")

    return "\n".join(lines)


def sections_for_lever(lever: int, entity_type: EntityType) -> list[str]:
    """Return the section keys that a lever is allowed to fill for an entity type."""
    owned = LEVER_SECTION_OWNERSHIP.get(lever, set())
    template = ENTITY_TYPE_TEMPLATES.get(entity_type, [])
    return [s for s in template if s in owned]
