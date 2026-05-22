"""Phase 1 applier rejects raw dict candidates from the legacy L6 path."""
import pytest

from genie_space_optimizer.optimization.applier import (
    UntypedProposalError,
    require_typed_repair_proposal,
)


def test_dict_candidate_raises():
    with pytest.raises(UntypedProposalError, match="raw dict"):
        require_typed_repair_proposal({"intent_id": "x", "patch_type": "p"})


def test_repair_proposal_instance_passes():
    from genie_space_optimizer.optimization.repair_proposal_typed import RepairProposal
    # Build a minimal valid RepairProposal; exact constructor depends on the existing module.
    rp = RepairProposal.__new__(RepairProposal)  # tolerant constructor for unit testing
    # The validator only checks ``isinstance`` — concrete fields are out of scope here.
    require_typed_repair_proposal(rp)
