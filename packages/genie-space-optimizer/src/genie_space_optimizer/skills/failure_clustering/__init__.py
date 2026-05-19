"""Plan 4 — failure-clustering reasoning skill.

LLM-driven semantic clustering of per-qid RCA evidence into typed
``LlmCluster`` groups. Consumes Plan 3's ``PerQidRcaEvidence``;
emits groups with a deterministic-stamped ``cluster_id`` and a typed
``suggested_repair_shape: RepairShape`` for Plan 5 intent synthesis.

Folder underscored for Python import; ``skill_id`` frontmatter keeps
the hyphenated form ``failure-clustering`` for postmortem
readability.
"""
