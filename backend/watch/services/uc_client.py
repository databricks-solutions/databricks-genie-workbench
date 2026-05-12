"""Unity Catalog table metadata lookup for the GenieWatch resources tab.

Returns kind/owner/comment for a single UC table. Distinct from the workbench
`backend.services.uc_client` (which has broader catalog/schema discovery).
"""

import logging
from typing import Optional

from backend.services.auth import get_workspace_client

logger = logging.getLogger(__name__)


def _enum_value(v) -> str:
    raw = getattr(v, "value", v)
    return str(raw or "")


def get_table(full_name: str) -> Optional[dict]:
    if not full_name or full_name.count(".") != 2:
        return None
    try:
        client = get_workspace_client()
        t = client.tables.get(full_name=full_name)
    except Exception as e:
        logger.debug("UC tables.get(%s) failed: %s", full_name, e)
        return None
    return {
        "full_name": full_name,
        "kind": _enum_value(getattr(t, "table_type", None)),
        "owner": getattr(t, "owner", None),
        "comment": getattr(t, "comment", None),
    }
