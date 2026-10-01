"""Authorization guards for the GenieWatch surface.

GenieWatch introduces SP-privileged, cross-workspace operations (manual rollup
refresh, system-table reads) that don't fit the workbench model where every
authenticated user can do everything. As a minimal first step, the admin-only
write endpoints are gated to workspace admins. A broader operator-vs-manager
role model is tracked as a follow-up.

The guard lives in ``backend/services/admin_gate.py`` (shared with the Ontology
routers, MV-D109); it is re-exported here so existing imports keep working.
"""

from __future__ import annotations

from backend.services.admin_gate import is_admin_request, require_admin

__all__ = ["is_admin_request", "require_admin"]
