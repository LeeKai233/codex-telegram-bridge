from __future__ import annotations

import time

import pytest


@pytest.fixture(autouse=True)
def _reset_timezone_state() -> None:
    """Re-sync libc timezone state after tests that mutate TZ.

    monkeypatch restores the TZ environment variable but cannot undo
    time.tzset(); without a follow-up tzset the mutated zone leaks into
    later tests on hosts whose baseline zone differs (CI is UTC).
    """
    yield
    time.tzset()
