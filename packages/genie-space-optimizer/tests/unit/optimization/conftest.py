"""Test isolation for the four prompt-improvements capture sinks.

Plans 1-4 each install a module-singleton capture sink in
``common.config`` that, when its require-coverage env flag is set,
registers an ``atexit`` handler. Tests that exercise the coverage gate
leave the sink in a partial state — counters incremented but the
matching shadow-comparison call omitted. At pytest shutdown the atexit
handler fires with stale state and prints ``[GSO_*] ... trial
incomplete`` to stderr. The process still exits 0 (the later atexit
handlers preempt ``os._exit``), but the noise pollutes CI output.

This autouse fixture:
  1. Resets every sink before and after each test in this directory.
  2. Clears the four ``GSO_*_CAPTURE_REQUIRE_COVERAGE`` env vars after
     each test, so any atexit handler that was registered during the
     test reads the env var at shutdown, sees it unset, and returns
     early (no spurious "trial incomplete" stderr).

Once cleared, the atexit handlers cannot be re-armed by subsequent
tests without those tests re-setting the env var explicitly — which
is the existing contract: tests that exercise the coverage gate already
do that via ``_reload_config_with_env``.
"""
from __future__ import annotations

import os

import pytest


_SINK_ATTR_NAMES = (
    "_NARROWING_CAPTURE_SINK",       # Plan 1
    "_LEVER_FIVE_CAPTURE_SINK",      # Plan 2
    "_THREE_STAGE_CAPTURE_SINK",     # Plan 3
    "_RAW_EVIDENCE_CAPTURE_SINK",    # Plan 4
)

_REQUIRE_COVERAGE_ENV_KEYS = (
    "GSO_NARROWING_CAPTURE_REQUIRE_COVERAGE",
    "GSO_LEVER5_SPLIT_CAPTURE_REQUIRE_COVERAGE",
    "GSO_THREE_STAGE_CAPTURE_REQUIRE_COVERAGE",
    "GSO_RAW_EVIDENCE_CAPTURE_REQUIRE_COVERAGE",
)


def _reset_all_sinks() -> None:
    from genie_space_optimizer.common import config as cfg
    for attr in _SINK_ATTR_NAMES:
        sink = getattr(cfg, attr, None)
        if sink is None:
            continue
        reset = getattr(sink, "reset_for_test", None)
        if reset is not None:
            reset()


def _clear_require_coverage_env() -> None:
    for key in _REQUIRE_COVERAGE_ENV_KEYS:
        os.environ.pop(key, None)


@pytest.fixture(autouse=True)
def _reset_capture_sinks_around_each_test():
    _reset_all_sinks()
    yield
    _reset_all_sinks()
    _clear_require_coverage_env()
