"""Quarantined legacy modules.

Modules under this package are no longer imported by production code.
They are preserved so we can:

1. Read them while reviewing the SM cutover.
2. Restore individual modules if a regression surfaces.
3. Use them as the source for ``GSO_USE_LEGACY_LEVER_LOOP=true`` rollback.

Do NOT add new imports of these modules from active source. The
CI grep at ``scripts/verify_legacy_isolation.py`` enforces this.

Physical deletion of this directory is the follow-up PR after Phase 7
trial confirms the SM is the sole production path.
"""
