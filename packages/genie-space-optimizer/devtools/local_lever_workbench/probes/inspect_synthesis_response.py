"""One-shot Stage 3 raw-response probe for the local workbench.

Goal: when ``synthesis_empty_reason=all_candidates_unsafe`` fires (see
Trial 13d), the canonical marker only tells us that every raw proposal's
``patch_type`` failed the closed :class:`PatchType` enum. It does NOT
record the rejected strings, so we cannot tell whether the LLM is
hallucinating new patch_type values, drifting on casing, or aliasing
known archetypes under different names.

This probe monkey-patches ``LlmReasoningCall.invoke`` for the duration
of one workbench iteration, tees every Stage 3 synthesis response into
a JSONL file, and then prints the rejected ``patch_type`` strings.

Intentionally a devtools-only diagnostic. The fix — promoting these
rejected raws into the structured synthesis marker — is a separate
production code change tracked in the iteration tracker.

Usage:
    uv run --no-sync python devtools/local_lever_workbench/probes/inspect_synthesis_response.py \\
        --input devtools/local_lever_workbench/runs/dc89_from_capture/bundle.json \\
        --output devtools/local_lever_workbench/runs/dc89_from_capture/synthesis_raw_responses.jsonl \\
        --profile fevm-prashanth \\
        --llm-model databricks-claude-sonnet-4-6
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

# devtools/local_lever_workbench/probes/inspect_synthesis_response.py →
# parents[3] = packages/genie-space-optimizer
PKG_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PKG_ROOT / "src"))
sys.path.insert(0, str(PKG_ROOT / "devtools"))
sys.path.insert(0, str(PKG_ROOT / "tests"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Workbench bundle JSON")
    parser.add_argument(
        "--output",
        required=True,
        help="JSONL path to tee Stage 3 raw responses into",
    )
    parser.add_argument("--profile", default="fevm-prashanth")
    parser.add_argument("--llm-model", default="databricks-claude-sonnet-4-6")
    parser.add_argument("--iteration", type=int, default=1)
    args = parser.parse_args()

    from local_lever_workbench.input_bundle import from_bundle_json
    from local_lever_workbench.local_runner import (
        APPLY_MODE_FAKE_RECORD,
        LLM_MODE_LIVE,
        run_workbench_iteration,
    )
    from local_lever_workbench.models import WorkbenchRunConfig

    bundle = from_bundle_json(Path(args.input))

    # Tee setup. Monkey-patch ``LlmReasoningCall.invoke`` so every
    # synthesis call writes a JSON line with the system+user prompts
    # (truncated) and the raw parsed_output. Other stages' calls are
    # also tee'd so we get a complete view if the bundle's cluster
    # composition changes.
    import genie_space_optimizer.optimization.llm_reasoning_call as lrc_mod

    original_invoke = lrc_mod.LlmReasoningCall.invoke
    teed: list[dict] = []

    def teeing_invoke(self, *, w, request):  # type: ignore[no-untyped-def]
        resp = original_invoke(self, w=w, request=request)
        try:
            entry = {
                "ts_ms": int(time.time() * 1000),
                "call_id": str(getattr(request, "call_id", "") or ""),
                "system_prompt_len": len(
                    str(getattr(request, "system_prompt", "") or "")
                ),
                "user_prompt_excerpt": (
                    str(getattr(request, "user_prompt", "") or "")[:1500]
                ),
                "succeeded": bool(getattr(resp, "succeeded", False)),
                "tokens_input": int(getattr(resp, "tokens_input", 0) or 0),
                "tokens_output": int(getattr(resp, "tokens_output", 0) or 0),
                "parsed_output": getattr(resp, "parsed_output", None),
                "declined": (
                    {
                        "reason": str(
                            getattr(resp.declined, "reason", "") or ""
                        ),
                        "explanation": str(
                            getattr(resp.declined, "explanation", "") or ""
                        ),
                    }
                    if getattr(resp, "declined", None) is not None
                    else None
                ),
            }
            teed.append(entry)
        except Exception as exc:  # noqa: BLE001 — diagnostic must not crash run
            teed.append({"tee_error": repr(exc)})
        return resp

    lrc_mod.LlmReasoningCall.invoke = teeing_invoke  # type: ignore[method-assign]
    try:
        config = WorkbenchRunConfig(
            bundle_path=Path(args.input),
            output_dir=Path(args.output).parent,
            llm_mode=LLM_MODE_LIVE,
            apply_mode=APPLY_MODE_FAKE_RECORD,
            profile=args.profile,
            llm_model=args.llm_model,
            iteration=args.iteration,
            tape_path=None,
        )
        artifacts = run_workbench_iteration(bundle, config)
    finally:
        lrc_mod.LlmReasoningCall.invoke = original_invoke  # type: ignore[method-assign]

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for entry in teed:
            f.write(json.dumps(entry, default=str) + "\n")

    # Surface the rejected patch_types right here so the operator does
    # not need a second tool to read the JSONL.
    print(f"wrote {len(teed)} tee'd LLM responses to {out_path}")
    print(f"elapsed_seconds: {artifacts.elapsed_seconds:.1f}")
    # PatchRecorder exposes the collected records as the ``patches``
    # list attribute. ``record(...)`` is the *add* method (with kwargs),
    # not the accessor — earlier probe revisions confused them.
    recorder = artifacts.recorder
    records = list(getattr(recorder, "patches", []) or [])
    print(f"recorded_patches: {len(records)}")
    print("--- rejected patch_types (from synthesis responses) ---")
    seen: dict[str, int] = {}
    for entry in teed:
        po = entry.get("parsed_output") or {}
        if not isinstance(po, dict):
            continue
        proposals = po.get("proposals") or []
        if not isinstance(proposals, list):
            continue
        for p in proposals:
            if not isinstance(p, dict):
                continue
            pt = str(p.get("patch_type", "")).strip()
            if not pt:
                continue
            seen[pt] = seen.get(pt, 0) + 1
    if not seen:
        print("(no proposals emitted by any synthesis call)")
    else:
        for pt, n in sorted(seen.items(), key=lambda kv: (-kv[1], kv[0])):
            print(f"  count={n}  patch_type={pt!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
