"""Probe for tests that assert a blocking call was moved off the event loop."""

import asyncio


def off_event_loop() -> bool:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return True
    return False
