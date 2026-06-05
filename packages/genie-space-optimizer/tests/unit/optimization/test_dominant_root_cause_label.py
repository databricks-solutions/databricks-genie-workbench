"""Trial 19 B1 — ``dominant_root_cause_label`` free-text aggregator."""
from genie_space_optimizer.optimization.rca import RcaKind
from genie_space_optimizer.optimization.rca_card_builder import (
    dominant_root_cause,
    dominant_root_cause_label,
)


def test_label_prefers_rca_kind_label_string() -> None:
    """When Stage 1 emits ``rca_kind_label``, that string wins over enum."""
    asi = {
        "gs_009": {
            "failure_type": "wrong_filter_condition",
            "rca_kind_label": "top_n_cardinality_collapse",
        },
    }
    assert dominant_root_cause_label(asi) == "top_n_cardinality_collapse"


def test_label_back_compat_with_failure_type_only() -> None:
    """Pre-Trial-19 rows (no rca_kind_label) still resolve via the enum mapper."""
    asi = {
        "gs_009": {"failure_type": "plural_top_n_collapse"},
    }
    assert (
        dominant_root_cause_label(asi)
        == RcaKind.TOP_N_CARDINALITY_COLLAPSE.value
    )


def test_label_never_collapses_typed_to_unknown_in_mixed_cluster() -> None:
    """Trial 19 B1 core invariant.

    Mixed cluster with one typed label and N unknowns must keep the
    typed label dominant — this is the Trial 18 postmortem failure
    that B1 directly addresses.
    """
    asi = {
        "gs_009": {"rca_kind_label": "top_n_cardinality_collapse"},
        "gs_011": {"rca_kind_label": "unknown"},
        "gs_012": {"rca_kind_label": "unknown"},
        "gs_013": {"rca_kind_label": "unknown"},
    }
    assert dominant_root_cause_label(asi) == "top_n_cardinality_collapse"


def test_label_empty_input_returns_unknown() -> None:
    assert dominant_root_cause_label({}) == "unknown"


def test_label_all_empty_metadata_returns_unknown() -> None:
    asi = {"gs_001": {"failure_type": ""}}
    assert dominant_root_cause_label(asi) == "unknown"


def test_label_confidence_breaks_tie_when_present() -> None:
    asi = {
        "gs_009": {
            "rca_kind_label": "top_n_cardinality_collapse",
            "confidence": 0.95,
        },
        "gs_011": {
            "rca_kind_label": "filter_logic_mismatch",
            "confidence": 0.40,
        },
    }
    assert dominant_root_cause_label(asi) == "top_n_cardinality_collapse"


def test_label_lexical_tiebreak_without_confidence() -> None:
    asi = {
        "gs_001": {"rca_kind_label": "top_n_cardinality_collapse"},
        "gs_002": {"rca_kind_label": "filter_logic_mismatch"},
    }
    assert dominant_root_cause_label(asi) == "filter_logic_mismatch"


def test_dominant_root_cause_back_compat_alias_returns_enum() -> None:
    """The legacy alias must continue to return ``RcaKind``."""
    asi = {
        "gs_009": {"rca_kind_label": "top_n_cardinality_collapse"},
        "gs_011": {"rca_kind_label": "unknown"},
    }
    assert dominant_root_cause(asi) == RcaKind.TOP_N_CARDINALITY_COLLAPSE


def test_dominant_root_cause_unknown_label_round_trips_to_unknown_enum() -> None:
    asi = {"gs_001": {"rca_kind_label": "unknown"}}
    assert dominant_root_cause(asi) == RcaKind.UNKNOWN


def test_dominant_root_cause_invented_label_falls_back_to_unknown_enum() -> None:
    """Invented labels (not in the enum) round-trip via the safe parser.

    The text matchers in ``_safe_rca_kind`` may still classify the
    cluster — but with empty metadata they'll return ``UNKNOWN``. This
    documents the back-compat alias's behavior on genuinely new labels.
    """
    asi = {"gs_001": {"rca_kind_label": "newly_invented_failure_kind"}}
    assert dominant_root_cause(asi) == RcaKind.UNKNOWN
