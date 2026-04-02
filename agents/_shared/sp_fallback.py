"""Service Principal fallback for Genie API scope errors.

When OBO tokens lack the 'genie' scope (before the user consent flow is
triggered), the Genie API returns scope errors. This module extracts the
retry-with-SP pattern from the monolith into a reusable decorator and
convenience function.

Source pattern: backend/services/genie_client.py:22-68
"""

from __future__ import annotations

import functools
import logging
from typing import Callable, TypeVar

from agents._shared.auth_bridge import (
    get_workspace_client,
    get_service_principal_client,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


def _is_scope_error(e: Exception) -> bool:
    """Check if exception is a missing OAuth scope error.

    Matches the same check in backend/services/genie_client.py:22-25.
    """
    msg = str(e).lower()
    return "scope" in msg or "insufficient_scope" in msg


def with_sp_fallback(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator: retry with SP client if OBO token lacks Genie scope.

    The decorated function must accept a ``client`` keyword argument
    (a WorkspaceClient). On scope error, the function is retried with
    the service principal client.

    Usage::

        @with_sp_fallback
        def get_genie_space(space_id: str, *, client=None):
            client = client or get_workspace_client()
            return client.api_client.do(
                "GET", f"/api/2.0/genie/spaces/{space_id}"
            )
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if _is_scope_error(e):
                logger.info(
                    "%s: OBO scope error, retrying with service principal",
                    func.__name__,
                )
                kwargs["client"] = get_service_principal_client()
                return func(*args, **kwargs)
            raise

    return wrapper


def genie_api_call(method: str, path: str, **kwargs):
    """Make a Genie API call with automatic SP fallback.

    Convenience function for simple one-off API calls that don't need
    the full decorator pattern.

    Args:
        method: HTTP method (GET, POST, etc.)
        path: API path (e.g., "/api/2.0/genie/spaces/{id}")
        **kwargs: Forwarded to ``client.api_client.do()``.

    Returns:
        API response dict.
    """
    client = get_workspace_client()
    try:
        return client.api_client.do(method=method, path=path, **kwargs)
    except Exception as e:
        if _is_scope_error(e):
            logger.info("Genie API %s: scope error, retrying with SP", path)
            sp = get_service_principal_client()
            if sp is not client:
                return sp.api_client.do(method=method, path=path, **kwargs)
        raise
