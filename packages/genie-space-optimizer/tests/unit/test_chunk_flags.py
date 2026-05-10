# tests/unit/test_chunk_flags.py
import os
from unittest import mock

from genie_space_optimizer.common.config import (
    stage_handlers_chunk_a_enabled,
    stage_handlers_chunk_b_enabled,
    stage_handlers_chunk_c_enabled,
    stage_handlers_chunk_d_enabled,
)


def test_all_chunk_flags_default_off() -> None:
    with mock.patch.dict(os.environ, {}, clear=True):
        assert not stage_handlers_chunk_a_enabled()
        assert not stage_handlers_chunk_b_enabled()
        assert not stage_handlers_chunk_c_enabled()
        assert not stage_handlers_chunk_d_enabled()


def test_chunk_d_flag_reads_env() -> None:
    with mock.patch.dict(os.environ, {"GSO_STAGE_HANDLERS_CHUNK_D": "1"}, clear=True):
        assert stage_handlers_chunk_d_enabled()
    with mock.patch.dict(os.environ, {"GSO_STAGE_HANDLERS_CHUNK_D": "0"}, clear=True):
        assert not stage_handlers_chunk_d_enabled()
