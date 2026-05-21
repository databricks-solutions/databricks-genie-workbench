"""Plan 12 — evidence_kind → eligible_lever_families policy.

Closes the routing bug observed in postmortem
``dc89d1a9-...`` where ``gs_004`` (wrong-aggregation evidence) was
routed to Lever 1 (non-generating ``add_column_description``) despite
the evidence demanding structural example-SQL or metric-view
generation. The non-generating lane could not produce a patch that
addresses the failure mode; the iteration burned budget without
any chance of fixing the QID.

Lever families:

  * ``"1"``  — ``add_column_description`` / metadata edits (NON-GENERATING)
  * ``"5b"`` — ``add_example_sql`` (generating, forgiving)
  * ``"6"``  — ``add_sql_snippet_*`` (generating, structural)
  * ``"2"``  — refine metric view (generating, MV)

The first element in the returned tuple is the preferred family; the
AG router falls back to subsequent elements if the first one declines
(e.g. the LLM didn't propose anything in that lane).
"""
from __future__ import annotations


# Evidence kinds where the SQL itself has a structural problem the
# optimizer must teach Genie to handle (top-N truncation, missing
# filter, wrong measure, etc.). Lever 1 cannot fix any of these — it
# only edits descriptions and aliases — so it is refused entirely.
_GENERATING_REQUIRED_KINDS: frozenset[str] = frozenset({
    "wrong_aggregation",
    "missing_filter",
    "column_disambiguation",
    "top_n_collapse",
    "plural_top_n_collapse",
    "wrong_measure",
    "wrong_join",
    "missing_join",
    "wrong_grain",
})

# Evidence kinds where Lever 1 IS appropriate — the failure is
# metadata-level (e.g. Genie picked the wrong column because two
# columns had similar names and no descriptions to disambiguate).
_METADATA_ONLY_KINDS: frozenset[str] = frozenset({
    "ambiguous_column_description",
    "ambiguous_table_description",
})


def _normalize(evidence_kind: object) -> str:
    """Strip + lowercase for resilient lookup; empty / None pass through
    as the empty string (caller's "unknown" bucket)."""
    if evidence_kind is None:
        return ""
    return str(evidence_kind).strip().lower()


def eligible_lever_families(
    evidence_kind: str,
) -> tuple[object, ...]:
    """Return the ordered tuple of lever families eligible for the
    given ``evidence_kind``. The first element is the preferred family;
    the AG router falls back to subsequent elements if the first one
    declines.

    Three branches:

      * Metadata-only evidence → ``("5b", "6", 1)``. Lever 1 IS in the
        tuple because the failure is genuinely metadata-level; the
        preferred family is still a generating lane because most fixes
        benefit from showing Genie a concrete example.
      * Generating-required evidence → ``("5b", "6", 2)``. Lever 1 is
        absent; the AG router will NEVER route here.
      * Unknown evidence → ``("5b", "6")``. Safest default. NEVER
        defaults to Lever 1 (the postmortem-observed regression).
    """
    kind = _normalize(evidence_kind)
    if kind in _METADATA_ONLY_KINDS:
        return ("5b", "6", 1)
    if kind in _GENERATING_REQUIRED_KINDS:
        return ("5b", "6", 2)
    return ("5b", "6")


def refuses_non_generating_lane(evidence_kind: str) -> bool:
    """Returns ``True`` if Lever 1 (non-generating) is forbidden for
    this ``evidence_kind``. The AG router uses this as a pre-flight
    check before assigning ``target_lever=1`` — if it returns True,
    the router must instead pick from
    :func:`eligible_lever_families`.
    """
    return 1 not in eligible_lever_families(evidence_kind)
