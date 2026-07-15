from __future__ import annotations

import time
from pathlib import Path

import pyotp

from codex_telegram_bridge.security import SecurityManager
from codex_telegram_bridge.store import Store


def enrolled_manager(tmp_path: Path) -> tuple[Store, SecurityManager, str, list[str]]:
    store = Store(tmp_path / "state.sqlite3")
    manager = SecurityManager(store, tmp_path / "config" / "totp_secret", unlock_seconds=60)
    enrollment = manager.begin_enrollment("tester")
    code = pyotp.TOTP(enrollment.secret).now()
    assert manager.commit_enrollment(enrollment, code)
    store.set_meta("totp_last_timecode", -1)
    return store, manager, code, enrollment.recovery_codes


def test_totp_is_single_use_and_unlock_is_process_local(tmp_path: Path) -> None:
    store, manager, code, _ = enrolled_manager(tmp_path)
    assert manager.verify(code)
    assert manager.is_unlocked()
    assert not manager.verify(code)
    manager.lock()
    assert not manager.is_unlocked()
    restarted = SecurityManager(store, manager.secret_path, unlock_seconds=60)
    assert not restarted.is_unlocked()
    store.close()


def test_five_failures_lock_even_a_valid_totp(tmp_path: Path) -> None:
    store, manager, code, _ = enrolled_manager(tmp_path)
    for _ in range(5):
        assert not manager.verify("invalid")
    assert int(store.get_meta("totp_locked_until", 0)) > int(time.time())
    assert not manager.verify(code)
    store.close()


def test_recovery_code_is_one_time_and_reset_removes_enrollment(tmp_path: Path) -> None:
    store, manager, _, recovery_codes = enrolled_manager(tmp_path)
    recovery = recovery_codes[0]
    assert manager.verify(recovery.lower())
    manager.lock()
    assert not manager.verify(recovery)
    manager.reset_enrollment()
    assert not manager.configured
    assert store.unused_recovery_codes() == []
    store.close()


def test_secret_symlink_is_not_accepted(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    target = tmp_path / "target-secret"
    target.write_text(pyotp.random_base32(), encoding="ascii")
    target.chmod(0o600)
    secret = tmp_path / "totp_secret"
    secret.symlink_to(target)
    manager = SecurityManager(store, secret)
    assert not manager.configured
    assert not manager.verify("123456")
    store.close()


def test_pair_code_is_one_time(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    manager = SecurityManager(store, tmp_path / "totp_secret")
    code = manager.create_pair_code(60)
    assert manager.consume_pair_code(code)
    assert not manager.consume_pair_code(code)
    store.close()


def test_space_leases_are_isolated_process_local_and_epoch_revocable(tmp_path: Path) -> None:
    store, manager, code, recovery_codes = enrolled_manager(tmp_path)
    try:
        assert manager.verify_for_space("space-a", code)
        assert manager.is_space_unlocked("space-a")
        assert manager.space_unlock_remaining("space-a") > 0
        assert store.get_meta("totp_unlocked_until") == 0
        assert not manager.is_space_unlocked("space-b")
        assert not manager.is_unlocked()

        restarted = SecurityManager(store, manager.secret_path, unlock_seconds=60)
        assert not restarted.is_space_unlocked("space-a")
        assert restarted.verify_for_space("space-b", recovery_codes[0])
        assert restarted.is_space_unlocked("space-b")
        assert not manager.is_space_unlocked("space-b")

        epoch = restarted.auth_epoch
        manager.lock_all()
        assert restarted.auth_epoch == epoch + 1
        assert not restarted.is_space_unlocked("space-b")
    finally:
        store.close()


def test_bind_code_is_one_time_and_separate_from_pair_code(tmp_path: Path) -> None:
    store = Store(tmp_path / "state.sqlite3")
    manager = SecurityManager(store, tmp_path / "totp_secret")
    try:
        bind_code = manager.create_bind_code(60)
        assert manager.consume_bind_code(bind_code)
        assert not manager.consume_bind_code(bind_code)
    finally:
        store.close()
