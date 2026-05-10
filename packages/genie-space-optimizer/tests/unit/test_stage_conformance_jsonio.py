# tests/unit/test_stage_conformance_jsonio.py
import pytest

from genie_space_optimizer.optimization.stages import STAGES
from genie_space_optimizer.optimization.stages._json_io import JsonRoundTrip


@pytest.mark.parametrize("entry", STAGES, ids=lambda e: e.stage_key)
def test_input_class_mixes_jsonroundtrip(entry) -> None:
    assert issubclass(entry.input_class, JsonRoundTrip), (
        f"{entry.stage_key}.INPUT_CLASS={entry.input_class.__name__} "
        f"does not mix JsonRoundTrip; cannot fixture-replay this stage"
    )


@pytest.mark.parametrize("entry", STAGES, ids=lambda e: e.stage_key)
def test_output_class_mixes_jsonroundtrip(entry) -> None:
    assert issubclass(entry.output_class, JsonRoundTrip), (
        f"{entry.stage_key}.OUTPUT_CLASS={entry.output_class.__name__} "
        f"does not mix JsonRoundTrip"
    )


@pytest.mark.parametrize("entry", STAGES, ids=lambda e: e.stage_key)
def test_execute_is_callable(entry) -> None:
    assert callable(entry.execute)
    assert entry.execute is getattr(entry.module, "execute")
