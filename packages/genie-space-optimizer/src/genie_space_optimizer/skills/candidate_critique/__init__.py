"""Plan 6 — candidate-critique reasoning skill.

LLM-driven typed CritiqueVerdict producer that runs between
``stages.proposals`` and ``stages.gates``. Scores each candidate
proposal against its failure evidence BEFORE the expensive safety
gate + applier + post-patch evaluation cycle. Advisory by default;
``GSO_CRITIQUE_GATE_ENFORCING=true`` flips it to gating (``discard``
verdicts filter the proposal out of the slate).

Folder underscored for Python import; ``skill_id`` frontmatter keeps
the hyphenated form ``candidate-critique`` for postmortem readability
(matches Plans 3/4/5).
"""
