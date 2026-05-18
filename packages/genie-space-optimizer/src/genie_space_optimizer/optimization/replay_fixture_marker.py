"""WU-5 — dual-channel replay fixture emission + extraction.

The legacy contract emits the replay fixture as one compact JSON
line wrapped in ``===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN===`` /
``===PHASE_A_REPLAY_FIXTURE_JSON_END===`` markers on stderr. The
2026-05-18 postmortems observed in-band pollution from concurrent
prompt/source prints, rendering the bracketed JSON unparseable.

This module:

1. Emits the fixture twice — once as plain JSON (legacy) and once
   as base64-encoded JSON inside a separate marker pair
   (``===PHASE_A_REPLAY_FIXTURE_BASE64_BEGIN===`` /
   ``===PHASE_A_REPLAY_FIXTURE_BASE64_END===``). Base64 is immune to
   line-level interleaving because any pollution would corrupt the
   decode and the extractor detects that.

2. Provides ``extract_replay_fixture_from_stream`` which prefers
   plain JSON, falls back to base64, and reports the source so the
   postmortem can tell which channel was used.
"""
from __future__ import annotations

import base64
import json
import sys
from dataclasses import dataclass
from typing import Any, Optional


PLAIN_BEGIN = "===PHASE_A_REPLAY_FIXTURE_JSON_BEGIN==="
PLAIN_END = "===PHASE_A_REPLAY_FIXTURE_JSON_END==="
B64_BEGIN = "===PHASE_A_REPLAY_FIXTURE_BASE64_BEGIN==="
B64_END = "===PHASE_A_REPLAY_FIXTURE_BASE64_END==="


@dataclass(frozen=True, slots=True)
class ExtractedFixture:
    payload: Optional[dict[str, Any]]
    source: str  # "plain_json" | "base64_fallback" | "missing"


def emit_dual_fixture(
    *, payload: dict[str, Any], stream_name: str = "stderr"
) -> None:
    """Write the fixture to ``stream_name`` as both plain JSON and
    base64. Single-line bodies in both windows so log greppers can
    parse line-at-a-time.
    """
    stream = sys.stderr if stream_name == "stderr" else sys.stdout
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    encoded = base64.b64encode(raw.encode("utf-8")).decode("ascii")
    stream.write("\n" + PLAIN_BEGIN + "\n")
    stream.write(raw + "\n")
    stream.write(PLAIN_END + "\n")
    stream.write(B64_BEGIN + "\n")
    stream.write(encoded + "\n")
    stream.write(B64_END + "\n")
    stream.flush()


def _slice_between(text: str, begin: str, end: str) -> Optional[str]:
    i = text.find(begin)
    if i < 0:
        return None
    j = text.find(end, i + len(begin))
    if j < 0:
        return None
    return text[i + len(begin):j].strip()


def extract_replay_fixture_from_stream(stream_text: str) -> ExtractedFixture:
    """Return the parsed fixture payload.

    Precedence:
      1. Plain JSON marker pair, if the bracketed body parses as JSON.
      2. Base64 marker pair, if the bracketed body decodes and parses.
      3. Missing.
    """
    plain = _slice_between(stream_text, PLAIN_BEGIN, PLAIN_END)
    if plain is not None:
        try:
            return ExtractedFixture(
                payload=json.loads(plain),
                source="plain_json",
            )
        except json.JSONDecodeError:
            pass
    b64 = _slice_between(stream_text, B64_BEGIN, B64_END)
    if b64 is not None:
        try:
            decoded = base64.b64decode(b64).decode("utf-8")
            return ExtractedFixture(
                payload=json.loads(decoded),
                source="base64_fallback",
            )
        except Exception:
            pass
    return ExtractedFixture(payload=None, source="missing")
