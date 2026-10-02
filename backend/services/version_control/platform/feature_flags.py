"""Explicit feature switches, independent of environment variables or credentials.

Opt-in switches are necessary, never sufficient authorization. M08's future
capability checks and M03/M06 admission still gate every enabled writer.
"""

from collections.abc import Mapping
from dataclasses import dataclass, fields
from types import MappingProxyType

WRITE_SWITCHES = frozenset({
    "vc_writes_enabled", "vc_restore_enabled", "vc_promotion_enabled",
    "vc_optimizer_apply_enabled", "vc_reconcile_enabled",
})


@dataclass(frozen=True)
class FeatureFlags:
    vc_writes_enabled: bool = False
    vc_restore_enabled: bool = False
    vc_promotion_enabled: bool = False
    vc_optimizer_apply_enabled: bool = False
    vc_reconcile_enabled: bool = False
    vc_history_enabled: bool = False

    def __post_init__(self) -> None:
        if any(type(getattr(self, definition.name)) is not bool for definition in fields(self)):
            raise TypeError("Feature switches require explicit booleans")

    @property
    def registry(self) -> Mapping[str, bool]:
        return MappingProxyType({definition.name: getattr(self, definition.name)
                                 for definition in fields(self)})

    def enabled(self, name: str) -> bool:
        requested = self.registry[name]
        return requested and (name not in WRITE_SWITCHES or self.vc_writes_enabled)

    @classmethod
    def from_config(cls, config: Mapping[str, bool]) -> "FeatureFlags":
        """Build explicit switches from a config mapping.

        Only recognized switch names are read; any missing switch defaults to
        ``False`` (fail-closed). Non-boolean values are rejected by
        ``__post_init__`` so a typo like ``"true"`` never enables a writer.
        Unknown keys are ignored so a shared platform-config dict can be passed
        directly.
        """
        names = {definition.name for definition in fields(cls)}
        return cls(**{name: config[name] for name in names if name in config})
