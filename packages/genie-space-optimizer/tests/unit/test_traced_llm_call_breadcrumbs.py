"""Phase 3.6 Task 2 — _traced_llm_call writes iteration/ag/cluster to its span."""
from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

from genie_space_optimizer.optimization import optimizer
from genie_space_optimizer.optimization.llm_call_recorder import (
    RecorderBinding,
    _RECORDER_BINDING,
)


@contextmanager
def _capture_span_inputs():
    """Patch mlflow.start_span to record the set_inputs call args."""
    inputs_captured: list[dict] = []

    class _SpanRecorder:
        def set_inputs(self, payload):
            inputs_captured.append(dict(payload))

        def set_outputs(self, payload):
            pass

        def add_event(self, event):
            pass

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def _fake_start_span(*, name, span_type):
        return _SpanRecorder()

    import mlflow as _mlflow
    with patch.object(_mlflow, "start_span", side_effect=_fake_start_span):
        yield inputs_captured


def _stub_openai_with(text: str):
    client = MagicMock(name="OpenAIClientStub")
    choice = MagicMock()
    choice.message.content = text
    comp = MagicMock()
    comp.choices = [choice]
    comp.usage = MagicMock(prompt_tokens=1, completion_tokens=1, total_tokens=2)
    client.chat.completions.create.return_value = comp
    return client


def test_chain_span_carries_iteration_ag_cluster_breadcrumbs():
    binding_token = _RECORDER_BINDING.set(
        RecorderBinding(iteration=3, ag_id="AG_77", cluster_id="H001"),
    )
    try:
        with _capture_span_inputs() as inputs, patch.object(
            optimizer, "_get_openai_client",
            return_value=_stub_openai_with('{"x":1}'),
        ):
            optimizer._traced_llm_call(
                w=None,
                system_msg="",
                prompt="p",
                span_name="stage_1_discovery",
                max_retries=1,
            )
    finally:
        _RECORDER_BINDING.reset(binding_token)

    merged: dict = {}
    for d in inputs:
        merged.update(d)
    assert merged.get("iteration") == 3
    assert merged.get("ag_id") == "AG_77"
    assert merged.get("cluster_id") == "H001"


def test_chain_span_breadcrumbs_default_when_unbound():
    """No binding set → -1/empty defaults; span still gets the keys
    for parser uniformity."""
    binding_token = _RECORDER_BINDING.set(
        RecorderBinding(iteration=-1, ag_id="", cluster_id=""),
    )
    try:
        with _capture_span_inputs() as inputs, patch.object(
            optimizer, "_get_openai_client",
            return_value=_stub_openai_with("ok"),
        ):
            optimizer._traced_llm_call(
                w=None,
                system_msg="",
                prompt="p",
                span_name="adaptive_strategy",
                max_retries=1,
            )
    finally:
        _RECORDER_BINDING.reset(binding_token)

    merged: dict = {}
    for d in inputs:
        merged.update(d)
    assert merged.get("iteration") == -1
    assert merged.get("ag_id") == ""
    assert merged.get("cluster_id") == ""
