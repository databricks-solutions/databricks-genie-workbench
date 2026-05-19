"""Plan 5 — repair-intent-synthesis reasoning skill.

LLM-driven typed RepairProposal producer that replaces the brittle
deterministic ``pick_archetype`` + ``_derive_asset_slice_from_afs``
dispatch glue inside ``_dispatch_lever_5b_for_cluster``. Emits a
free-form intent name + description with a closed RepairShape +
PatchType (PatchType may cross lever boundaries → cross-lever
router).

Folder underscored for Python import; ``skill_id`` frontmatter
keeps the hyphenated form ``repair-intent-synthesis`` for postmortem
readability (matches Plans 3/4).
"""
