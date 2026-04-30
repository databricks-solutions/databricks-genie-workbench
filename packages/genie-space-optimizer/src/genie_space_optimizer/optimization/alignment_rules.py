"""Config-driven deterministic question/SQL alignment rules.

The LLM alignment check (``validate_question_sql_alignment`` in
``benchmarks.py``) is the source of truth for deciding whether a
benchmark's expected SQL actually answers the question. It is correct
but expensive: every benchmark eats an LLM round-trip.

Some misalignment classes are cheap to detect via string match — e.g.
"the SQL filters on column X but the question never mentions any of the
synonyms for X". For Genie Spaces that have well-known business flags
(default same-store filter, default region filter, etc.), a deterministic
shortcut is worth the saved LLM cost AND yields more deterministic
behavior across runs.

This module provides the *mechanism* for those shortcuts. By default the
rule list is empty so the optimizer is identical for any space. A
deployment can register rules either:

1. Programmatically (e.g. a host application imports
   ``DETERMINISTIC_EXTRA_FILTER_RULES`` and assigns its own tuple), or
2. Via the ``GSO_EXTRA_FILTER_RULES_PATH`` environment variable pointing
   at a JSON file shaped as a list of rule dicts.

Rule schema (JSON)::

    [
      {
        "name": "active_only_implicit",
        "column_substring": "is_active_flag",
        "question_terms": ["active", "currently active"],
        "issue_template": "EXTRA_FILTER: SQL filters on {column} but the question does not ask for active-only results."
      }
    ]

``{column}`` in ``issue_template`` is interpolated with the
``column_substring`` for readability.
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExtraFilterRule:
    """A deterministic alignment rule.

    Attributes:
        name: Stable identifier (used in logs / debugging).
        column_substring: Case-insensitive substring searched for in the
            benchmark's ``expected_sql``. When present, the rule looks
            for any of ``question_terms`` in the question text; if none
            match, the issue is emitted.
        question_terms: Case-insensitive substrings whose presence in
            the question indicates the filter is intentional. If the
            question mentions any of these, the rule does NOT fire.
        issue_template: ``str.format``-compatible template with optional
            ``{column}`` placeholder. The emitted string is appended to
            the alignment issues list.
    """

    name: str
    column_substring: str
    question_terms: tuple[str, ...]
    issue_template: str

    def evaluate(self, *, question_lower: str, sql_lower: str) -> str | None:
        """Return the issue string if the rule fires, else ``None``."""
        if self.column_substring.lower() not in sql_lower:
            return None
        if any(term.lower() in question_lower for term in self.question_terms):
            return None
        return self.issue_template.format(column=self.column_substring)


def _load_rules_from_path(path: str) -> tuple[ExtraFilterRule, ...]:
    """Load rules from a JSON file. Soft-fails on any error."""
    try:
        raw = json.loads(Path(path).read_text())
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning(
            "alignment_rules: failed to load %s (%s); falling back to empty rule list.",
            path,
            exc,
        )
        return ()
    if not isinstance(raw, list):
        logger.warning(
            "alignment_rules: expected a JSON list in %s, got %s; ignoring.",
            path,
            type(raw).__name__,
        )
        return ()

    rules: list[ExtraFilterRule] = []
    for i, entry in enumerate(raw):
        if not isinstance(entry, dict):
            logger.warning(
                "alignment_rules: entry %d in %s is not an object; skipping.", i, path
            )
            continue
        try:
            rule = ExtraFilterRule(
                name=str(entry["name"]),
                column_substring=str(entry["column_substring"]),
                question_terms=tuple(str(t) for t in entry.get("question_terms", ())),
                issue_template=str(entry["issue_template"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning(
                "alignment_rules: entry %d in %s is malformed (%s); skipping.",
                i,
                path,
                exc,
            )
            continue
        rules.append(rule)
    return tuple(rules)


def _initial_rules() -> tuple[ExtraFilterRule, ...]:
    path = os.environ.get("GSO_EXTRA_FILTER_RULES_PATH", "").strip()
    if not path:
        return ()
    return _load_rules_from_path(path)


# Module-level rule list. Mutable from host applications via reassignment;
# defaults to empty (or env-loaded) so the optimizer ships customer-agnostic.
DETERMINISTIC_EXTRA_FILTER_RULES: tuple[ExtraFilterRule, ...] = _initial_rules()


def evaluate_extra_filter_rules(
    *,
    question: str,
    expected_sql: str,
    rules: tuple[ExtraFilterRule, ...] | None = None,
) -> list[str]:
    """Return any alignment issues fired by ``rules``.

    When ``rules`` is ``None``, uses the module-level
    ``DETERMINISTIC_EXTRA_FILTER_RULES`` (looked up at call time so host
    applications can swap the rule list at runtime).
    """
    active = DETERMINISTIC_EXTRA_FILTER_RULES if rules is None else rules
    if not active:
        return []
    question_lower = (question or "").lower()
    sql_lower = (expected_sql or "").lower()
    issues: list[str] = []
    for rule in active:
        issue = rule.evaluate(question_lower=question_lower, sql_lower=sql_lower)
        if issue is not None:
            issues.append(issue)
    return issues
