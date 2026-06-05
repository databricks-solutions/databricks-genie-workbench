"""Trial 21 postmortem-replay regression suite.

This package contains the W1 fixtures and the bright-line assertion test
module that replaces the synthetic 7/7 ACCEPTED live LLM sweep as the
merge gate for Trial 21.

Until W2-W9 land, every test in `test_trial21_postmortem_replay.py` is
EXPECTED TO BE RED. The test file uses `pytest.mark.xfail(strict=True)`
on each bright-line condition so the suite is allowed to fail without
blocking unrelated CI; the xfail markers come off as each W-item lands
and the corresponding assertion turns green.
"""
