"""Guard: the genie_space_versions DDL stays in lockstep with the wire contracts.

The ``valid_origin`` CHECK constraint is a hand-maintained mirror of ``Origin``. When those
drift, a legitimate write (e.g. the §21 initial-capture ``Origin.CREATE``) fails the Delta
constraint and the row is silently dropped by the fail-soft capture hook. This test fails
loudly the moment a new ``Origin`` member is added without extending the constraint.
"""

import re
from pathlib import Path

from backend.services.version_control import contracts as vc

_DDL = Path("backend/version_control_ddl/01-versions.sql").read_text()


def _valid_origin_values():
    match = re.search(r"CONSTRAINT valid_origin CHECK \(origin IN \(([^)]*)\)\)", _DDL)
    assert match, "valid_origin CHECK not found in 01-versions.sql"
    return {token.strip().strip("'") for token in match.group(1).split(",")}


def test_valid_origin_constraint_covers_every_origin_enum_value():
    assert _valid_origin_values() == {origin.value for origin in vc.Origin}


def test_valid_origin_admits_create_origin():
    # The specific member the §21 initial-capture hook writes; called out so a regression
    # points straight at the create flow rather than a generic set mismatch.
    assert vc.Origin.CREATE.value in _valid_origin_values()
