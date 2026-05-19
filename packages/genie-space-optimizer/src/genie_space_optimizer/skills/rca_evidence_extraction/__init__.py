"""Plan 3 — rca-evidence-extraction reasoning skill.

LLM-driven replacement for ``rca._asi_finding_from_metadata``. Emits
a typed ``PerQidRcaEvidence`` per failing qid; falls back to the
existing deterministic vocab-puller when the LLM declines.

Folder is named with underscores so Python can import nested modules
(``output_schema``); the SKILL.md ``skill_id`` keeps the hyphenated
form ``rca-evidence-extraction`` for postmortem / log readability.
"""
