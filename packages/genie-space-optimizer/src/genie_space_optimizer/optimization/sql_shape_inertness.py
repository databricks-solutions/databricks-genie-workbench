"""Track B / B2 — post-apply SQL-shape inertness detector.

The d139 / e943 postmortems showed a lone ``add_instruction`` patch for
a SQL-shape RCA (``top_n_cardinality_collapse``) being "applied +
accepted" while the LLM's generated SQL was structurally unchanged —
a phantom accept that ``live-llm-only`` (stubbed post-apply eval) hides.

This module is the *pure* inertness detector. After a patch is applied
it compares the generated SQL *shape* before and after: if the patch is
a lone behaviorally-inert mechanism for the RCA (a natural-language
instruction for an instruction-insufficient RCA) and the shape did not
change, the patch is marked ``applied_but_inert``. That flag rides on
the per-iteration :class:`CandidateLifecycle` so the reconciler and the
selector can refuse a phantom accept.

The shape signature deliberately ignores literals and whitespace so a
``LIMIT 5`` → ``LIMIT 10`` edit reads as the SAME shape (still a flat
top-N collapse), while a window-function rewrite reads as DIFFERENT.
"""
from __future__ import annotations

import re
from collections.abc import Iterable

from genie_space_optimizer.optimization.patch_mechanism import PatchMechanism
from genie_space_optimizer.optimization.rca_mechanism_routing import (
    _structural_fix_mechanisms,
    instruction_text_is_insufficient_for,
)


# String / numeric literals are normalised to a single placeholder so a
# literal-only edit does not register as a shape change.
_STRING_LITERAL = re.compile(r"'(?:[^']|'')*'")
_NUMERIC_LITERAL = re.compile(r"\b\d+(?:\.\d+)?\b")
_WHITESPACE = re.compile(r"\s+")
_PUNCT_SPACE = re.compile(r"\s*([(),])\s*")


def sql_shape_signature(sql: str | None) -> str:
    """Return a normalised structural signature of a SQL string.

    Normalisation: lowercase, collapse whitespace, strip string and
    numeric literals to a placeholder, and tighten spacing around
    parentheses/commas. Two queries with the same clause skeleton but
    different literals share a signature; a query whose grain or clause
    structure changed does not.
    """
    text = str(sql or "").strip().lower()
    if not text:
        return ""
    text = _STRING_LITERAL.sub("'?'", text)
    text = _NUMERIC_LITERAL.sub("?", text)
    text = _WHITESPACE.sub(" ", text)
    text = _PUNCT_SPACE.sub(r"\1", text)
    return text.strip()


def sql_shape_unchanged(sql_before: str | None, sql_after: str | None) -> bool:
    """True when both SQLs reduce to the same shape signature."""
    return sql_shape_signature(sql_before) == sql_shape_signature(sql_after)


def detect_applied_but_inert(
    *,
    rca_kind: str | None,
    mechanisms: Iterable[PatchMechanism],
    sql_before: str | None,
    sql_after: str | None,
) -> bool:
    """True when an applied lone instruction left the SQL shape unchanged.

    Fires only when ALL hold:

      1. ``rca_kind`` is a SQL-shape RCA where a lone ``INSTRUCTION_TEXT``
         is insufficient (:func:`instruction_text_is_insufficient_for`),
         AND
      2. the applied mechanisms are a lone ``INSTRUCTION_TEXT`` with no
         *structural* (non-prose) shaping companion present — a paired
         structural mechanism (snippet / description / routing) makes an
         unchanged shape a *coverage* concern, not the lone-instruction
         phantom, AND
      3. the generated SQL shape is unchanged before vs after.

    Returns ``False`` otherwise so the detector never over-fires on a
    legitimately-shaping patch or an RCA outside the SQL-shape contract.
    """
    if not instruction_text_is_insufficient_for(rca_kind):
        return False
    observed = {m for m in mechanisms if m is not None}
    if PatchMechanism.INSTRUCTION_TEXT not in observed:
        return False
    if observed & _structural_fix_mechanisms(rca_kind):
        return False
    # A lone instruction (no structural companion) — inert iff the
    # generated SQL shape did not move.
    return sql_shape_unchanged(sql_before, sql_after)


__all__ = [
    "sql_shape_signature",
    "sql_shape_unchanged",
    "detect_applied_but_inert",
]
