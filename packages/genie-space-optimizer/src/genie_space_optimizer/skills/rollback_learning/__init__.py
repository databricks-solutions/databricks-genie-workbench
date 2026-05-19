"""Plan 7 — rollback-learning reasoning skill.

LLM-driven typed NextAttemptHypothesis producer that runs between
``stages.acceptance`` (when it returns ``rolled_back``) and
``stages.learning``. One hypothesis per rolled-back cluster. The
hypothesis is NEVER auto-applied — it is stamped onto
``metadata_snapshot["_last_attempt_hypothesis_by_cluster"][cluster_id]``
and read by Plan 5's repair-intent synthesizer on the NEXT iteration
as additional grounding context. Behind ``GSO_PLAN7_ROLLBACK_LEARNING``
flag (default false) for the first deploy.

Folder underscored for Python import; ``skill_id`` frontmatter keeps
the hyphenated form ``rollback-learning`` for postmortem readability
(matches Plans 3/4/5/6).
"""
