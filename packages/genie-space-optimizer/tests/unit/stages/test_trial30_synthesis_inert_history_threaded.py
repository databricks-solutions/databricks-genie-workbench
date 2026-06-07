import inspect

from genie_space_optimizer.optimization.inert_mechanism_history import (
    InertMechanismHistory,
)
from genie_space_optimizer.optimization.stages import synthesize
from genie_space_optimizer.optimization.stages.synthesize import (
    render_inert_mechanism_history_section,
)


def test_render_lists_rejected_mechanisms_per_qid():
    history = (
        InertMechanismHistory(
            qid="gs_026",
            rca_kind="top_n_cardinality_collapse",
            rejected_mechanisms=("lever-5",),
        ),
    )
    section = render_inert_mechanism_history_section(history)
    assert "gs_026" in section
    assert "top_n_cardinality_collapse" in section
    assert "lever-5" in section


def test_render_empty_history_is_blank():
    assert render_inert_mechanism_history_section(()) == ""


def test_build_request_accepts_inert_history_kwarg():
    sig = inspect.signature(synthesize._build_request)
    assert "inert_mechanism_history" in sig.parameters


def test_run_synthesis_accepts_inert_history_kwarg():
    sig = inspect.signature(
        synthesize.run_plan11_synthesis_for_single_cluster
    )
    assert "inert_mechanism_history" in sig.parameters


def test_optimizer_entrypoint_accepts_inert_history_kwarg():
    from genie_space_optimizer.optimization.optimizer import (
        run_state_machine_iteration_and_persist,
    )

    sig = inspect.signature(run_state_machine_iteration_and_persist)
    assert "inert_mechanism_history" in sig.parameters
