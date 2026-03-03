"""Parse checklist items from docs/checklist-by-schema.md.

This module provides the single source of truth for all checklist items
by parsing them from the markdown documentation file.
"""

import re
from pathlib import Path
from functools import lru_cache


# Sections to analyze (in order for UI walkthrough)
# This is the single source of truth for section names
SECTIONS = [
    "data_sources.tables",
    "data_sources.metric_views",
    "instructions.text_instructions",
    "instructions.example_question_sqls",
    "instructions.sql_functions",
    "instructions.join_specs",
    "instructions.sql_snippets.filters",
    "instructions.sql_snippets.expressions",
    "instructions.sql_snippets.measures",
    "benchmarks.questions",
]

# Set version for fast lookup (derived from SECTIONS)
RECOGNIZED_SECTIONS = set(SECTIONS)

# Default path to checklist markdown file
DEFAULT_CHECKLIST_PATH = Path(__file__).parent.parent / "docs" / "checklist-by-schema.md"


def slugify(text: str) -> str:
    """Convert description text to a stable ID slug.

    Examples:
        "At least 1 table is configured" -> "at-least-1-table-is-configured"
        "Tables are focused (only necessary tables)" -> "tables-are-focused-only-necessary-tables"
    """
    # Remove backticks and special chars, lowercase, replace spaces with hyphens
    slug = text.lower()
    slug = re.sub(r"[`'\"]", "", slug)  # Remove quotes/backticks
    slug = re.sub(r"[^a-z0-9\s-]", " ", slug)  # Replace special chars with space
    slug = re.sub(r"\s+", "-", slug.strip())  # Replace spaces with hyphens
    slug = re.sub(r"-+", "-", slug)  # Collapse multiple hyphens
    return slug


def parse_checklist_markdown(markdown_path: Path | None = None) -> dict[str, list[dict]]:
    """Parse the checklist markdown file and extract items by section.

    Args:
        markdown_path: Path to the markdown file. Defaults to docs/checklist-by-schema.md

    Returns:
        Dict mapping section names to lists of checklist items.
        Each item has 'id' and 'description' keys.

    Example:
        {
            "data_sources.tables": [
                {"id": "at-least-1-table-is-configured", "description": "At least 1 table is configured"},
                ...
            ],
            ...
        }
    """
    if markdown_path is None:
        markdown_path = DEFAULT_CHECKLIST_PATH

    content = markdown_path.read_text()
    lines = content.split("\n")

    result: dict[str, list[dict]] = {section: [] for section in RECOGNIZED_SECTIONS}

    # Track current section hierarchy
    current_h2 = ""  # e.g., "data_sources", "instructions", "benchmarks"
    current_h3 = ""  # e.g., "tables", "metric_views", "sql_snippets"
    current_h4 = ""  # e.g., "filters", "expressions", "measures"

    for line in lines:
        stripped = line.strip()

        # Parse headers to track section hierarchy
        if stripped.startswith("## "):
            # Extract section name from backticks: ## `data_sources` -> data_sources
            match = re.search(r"`([^`]+)`", stripped)
            current_h2 = match.group(1) if match else ""
            current_h3 = ""
            current_h4 = ""

        elif stripped.startswith("### "):
            match = re.search(r"`([^`]+)`", stripped)
            current_h3 = match.group(1) if match else ""
            current_h4 = ""

        elif stripped.startswith("#### "):
            match = re.search(r"`([^`]+)`", stripped)
            current_h4 = match.group(1) if match else ""

        # Parse checklist items: - [ ] **[P]** Description or - [ ] **[L]** Description
        # Also support items without [P]/[L] tags: - [ ] Description
        elif stripped.startswith("- [ ]"):
            # Remove the checkbox prefix and any [P]/[L] tags
            item_text = stripped[5:].strip()  # Remove "- [ ]"
            item_text = re.sub(r"\*\*\[(P|L)\]\*\*\s*", "", item_text)  # Remove **[P]** or **[L]**
            item_text = item_text.strip()

            if not item_text:
                continue

            # Build section path
            if current_h4:
                section_path = f"{current_h2}.{current_h3}.{current_h4}"
            elif current_h3:
                section_path = f"{current_h2}.{current_h3}"
            else:
                continue  # Skip items not under a recognized section

            # Only add if it's a recognized section
            if section_path in RECOGNIZED_SECTIONS:
                result[section_path].append({
                    "id": slugify(item_text),
                    "description": item_text,
                })

    return result


@lru_cache(maxsize=1)
def get_checklist_items() -> dict[str, list[dict]]:
    """Get all checklist items, cached for performance.

    Returns:
        Dict mapping section names to lists of checklist items.
    """
    return parse_checklist_markdown()


def get_checklist_items_for_section(section_name: str) -> list[dict]:
    """Get checklist items for a specific section.

    Args:
        section_name: Section name (e.g., "data_sources.tables")

    Returns:
        List of checklist items with 'id' and 'description' keys.
        Returns empty list if section is not recognized.
    """
    items = get_checklist_items()
    return items.get(section_name, [])


def clear_cache() -> None:
    """Clear the cached checklist items. Useful for testing or hot-reloading."""
    get_checklist_items.cache_clear()
