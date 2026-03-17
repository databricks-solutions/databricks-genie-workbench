"""Public integration API for embedding GSO in other applications.

This module provides standalone functions that can be called without
FastAPI dependency injection or pydantic-settings env-prefix conventions.
All configuration is passed explicitly via :class:`IntegrationConfig`.

Usage::

    from genie_space_optimizer.integration import (
        IntegrationConfig,
        trigger_optimization,
        apply_optimization,
        discard_optimization,
        get_lever_info,
        get_default_lever_order,
    )

    config = IntegrationConfig(
        catalog="main",
        schema_name="genie_optimization",
        warehouse_id="abc123",
    )
    result = trigger_optimization(space_id, ws, sp_ws, config)
"""

from .apply import apply_optimization
from .config import IntegrationConfig
from .discard import discard_optimization
from .levers import get_default_lever_order, get_lever_info
from .trigger import trigger_optimization
from .types import ActionResult, TriggerResult

__all__ = [
    "IntegrationConfig",
    "TriggerResult",
    "ActionResult",
    "trigger_optimization",
    "apply_optimization",
    "discard_optimization",
    "get_lever_info",
    "get_default_lever_order",
]
