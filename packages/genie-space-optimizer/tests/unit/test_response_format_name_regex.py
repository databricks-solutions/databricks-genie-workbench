"""PR-1A — pin the Databricks endpoint name-regex constraint on
``response_format.json_schema.name``.

Both the 98ec8950 (attempt 8) and dc89d1a9 (attempt 8) lever-loop trials
saw 100% Stage 1 ``BadRequestError`` with the provider body
``tools.0.custom.name failed ^[a-zA-Z0-9_-]{1,128}$``. The Databricks
Foundation Model API treats ``response_format.json_schema.name`` as a
tool name internally and rejects on that regex.

For Plan 11 stages we wrap the result type in ``AbstainableEnvelope[T]``;
the Python generic-alias machinery sets ``__name__`` to e.g.
``'AbstainableEnvelope[Plan11DiagnoseOutput]'`` which contains ``[`` and
``]``. ``build_response_format`` assigns ``model_cls.__name__`` straight
to ``json_schema.name`` (see prompt_io.py line ~189), so the endpoint
rejects every Plan 11 LLM call before tokens are consumed.

This module is the failing-test step of the test-first sequence in
``docs/llmdrivenarchitecture/v5/
stage1-tool-name-and-request-envelope-contract_e7b21f04.plan.md``
(PR-1A). All five parametrized cases MUST be red on the commit that
introduces this test, then turn green in PR-1B (the
``_safe_schema_name`` sanitizer in ``prompt_io.py``).
"""
from __future__ import annotations

import re

import pytest

from genie_space_optimizer.optimization.llm_reasoning_io import (
    AbstainableEnvelope,
)
from genie_space_optimizer.optimization.prompt_io import build_response_format
from genie_space_optimizer.skills.plan11_cluster.output_schema import (
    Plan11ClusterOutput,
)
from genie_space_optimizer.skills.plan11_diagnose.output_schema import (
    Plan11DiagnoseOutput,
)
from genie_space_optimizer.skills.plan11_narrow.output_schema import (
    Plan11NarrowOutput,
)
from genie_space_optimizer.skills.plan11_repair.output_schema import (
    Plan11RepairOutput,
)
from genie_space_optimizer.skills.plan11_synthesize.output_schema import (
    Plan11SynthesizeOutput,
)

# The exact regex the Databricks Foundation Model endpoint enforces on
# ``tools[*].custom.name`` (and, by extension, the internally-derived
# tool name from ``response_format.json_schema.name``). Captured
# verbatim from the dc89d1a9 / 98ec8950 trial provider bodies:
#   ``tools.0.custom.name failed ^[a-zA-Z0-9_-]{1,128}$``
DATABRICKS_TOOL_NAME_REGEX = re.compile(r"^[a-zA-Z0-9_-]{1,128}$")


_PLAN11_RESULT_CLASSES = [
    pytest.param(Plan11DiagnoseOutput, id="diagnose"),
    pytest.param(Plan11ClusterOutput, id="cluster"),
    pytest.param(Plan11SynthesizeOutput, id="synthesize"),
    pytest.param(Plan11RepairOutput, id="repair"),
    pytest.param(Plan11NarrowOutput, id="narrow"),
]


@pytest.mark.parametrize("result_cls", _PLAN11_RESULT_CLASSES)
def test_envelope_schema_name_matches_databricks_regex(result_cls):
    """Every Plan 11 ``AbstainableEnvelope[T]`` response_format must
    carry a ``json_schema.name`` that the Databricks endpoint accepts.

    Failing this test means a future Plan 11 LLM call will get
    ``BadRequestError`` with ``tokens_input=0`` before reaching
    inference. The marker emitted under PR-A will carry
    ``error_kind="endpoint_decline"`` (pre-PR-1C) or
    ``error_kind="request_envelope_invalid"`` (post-PR-1C), but the
    request never executes either way.
    """
    envelope_cls = AbstainableEnvelope[result_cls]
    fmt = build_response_format(envelope_cls)
    name = fmt["json_schema"]["name"]
    assert isinstance(name, str), name
    assert DATABRICKS_TOOL_NAME_REGEX.match(name), (
        f"json_schema.name {name!r} does not match "
        f"{DATABRICKS_TOOL_NAME_REGEX.pattern!r}; forbidden chars: "
        f"{sorted({c for c in name if not re.match('[a-zA-Z0-9_-]', c)})}"
    )


@pytest.mark.parametrize("result_cls", _PLAN11_RESULT_CLASSES)
def test_envelope_schema_name_is_non_empty_after_sanitize(result_cls):
    """A sanitizer that collapses every char to ``_`` and then strips
    trailing ``_`` could theoretically produce an empty string. Pin
    that the wire name is always ≥1 char and ≤128 chars so the
    Databricks regex's length bound is respected from both sides."""
    envelope_cls = AbstainableEnvelope[result_cls]
    fmt = build_response_format(envelope_cls)
    name = fmt["json_schema"]["name"]
    assert 1 <= len(name) <= 128, (len(name), name)


def test_bare_result_class_name_already_safe():
    """Sanity: the bare (non-envelope) Plan 11 output class names are
    already regex-safe. This pins that the issue is the envelope wrap,
    not the underlying schemas, so future call sites that bind a bare
    schema directly do not regress."""
    for result_cls in (
        Plan11DiagnoseOutput,
        Plan11ClusterOutput,
        Plan11SynthesizeOutput,
        Plan11RepairOutput,
    ):
        fmt = build_response_format(result_cls)
        name = fmt["json_schema"]["name"]
        assert DATABRICKS_TOOL_NAME_REGEX.match(name), (
            result_cls.__name__,
            name,
        )
