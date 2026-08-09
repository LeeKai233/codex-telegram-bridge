from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "bridge-service.sh"
RUST = "codex-telegram-rust-full.service"
PYTHON = "codex-telegram-bridge.service"


def run_switch(tmp_path: Path, *arguments: str, active: str = "", enabled: str = "") -> subprocess.CompletedProcess[str]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "systemctl.log"
    active_state = tmp_path / "active-units"
    enabled_state = tmp_path / "enabled-units"
    active_state.write_text("\n".join(filter(None, active.split(","))) + "\n", encoding="utf-8")
    enabled_state.write_text("\n".join(filter(None, enabled.split(","))) + "\n", encoding="utf-8")
    shim = bin_dir / "systemctl"
    shim.write_text(
        r"""#!/usr/bin/env bash
set -eu
printf '%s\n' "$*" >>"$SWITCH_LOG"
if [[ "$1 $2 $3" == "--user is-active --quiet" ]]; then
  grep -Fxq "$4" "$SWITCH_ACTIVE_STATE"
elif [[ "$1 $2 $3" == "--user is-enabled --quiet" ]]; then
  grep -Fxq "$4" "$SWITCH_ENABLED_STATE"
elif [[ "$1 $2" == "--user start" ]]; then
  [[ "${SWITCH_FAIL_START:-}" != "$3" ]] || exit 1
  grep -Fxq "$3" "$SWITCH_ACTIVE_STATE" || printf '%s\n' "$3" >>"$SWITCH_ACTIVE_STATE"
elif [[ "$1 $2" == "--user stop" ]]; then
  shift 2
  for unit in "$@"; do sed -i "\|^${unit}$|d" "$SWITCH_ACTIVE_STATE"; done
elif [[ "$1 $2" == "--user enable" ]]; then
  grep -Fxq "$3" "$SWITCH_ENABLED_STATE" || printf '%s\n' "$3" >>"$SWITCH_ENABLED_STATE"
elif [[ "$1 $2" == "--user disable" ]]; then
  shift 2
  [[ "${1:-}" != "--now" ]] || shift
  for unit in "$@"; do
    sed -i "\|^${unit}$|d" "$SWITCH_ENABLED_STATE"
    [[ "$*" != *"--now"* ]] || sed -i "\|^${unit}$|d" "$SWITCH_ACTIVE_STATE"
  done
fi
""",
        encoding="utf-8",
    )
    shim.chmod(0o755)
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{bin_dir}:{env['PATH']}",
            "SWITCH_LOG": str(log),
            "SWITCH_ACTIVE_STATE": str(active_state),
            "SWITCH_ENABLED_STATE": str(enabled_state),
        }
    )
    result = subprocess.run(
        ["bash", str(SCRIPT), *arguments],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    result.systemctl_log = log.read_text(encoding="utf-8") if log.exists() else ""  # type: ignore[attr-defined]
    return result


def test_switch_refuses_active_opposite_without_force(tmp_path: Path) -> None:
    result = run_switch(tmp_path, "rust", active=PYTHON, enabled=PYTHON)

    assert result.returncode != 0
    assert "retry with --force" in result.stderr
    assert f"--user stop {PYTHON}" not in result.systemctl_log  # type: ignore[attr-defined]


def test_force_switch_stops_disables_and_starts_target(tmp_path: Path) -> None:
    result = run_switch(tmp_path, "rust", "--force", active=PYTHON, enabled=PYTHON)

    assert result.returncode == 0, result.stderr
    log = result.systemctl_log  # type: ignore[attr-defined]
    assert f"--user stop {PYTHON}" in log
    assert f"--user disable {PYTHON}" in log
    assert f"--user enable {RUST}" in log
    assert f"--user start {RUST}" in log


def test_force_switch_restores_previous_owner_when_target_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("SWITCH_FAIL_START", RUST)
    result = run_switch(tmp_path, "rust", "--force", active=PYTHON, enabled=PYTHON)

    assert result.returncode != 0
    log = result.systemctl_log  # type: ignore[attr-defined]
    assert f"--user start {RUST}" in log
    assert f"--user start {PYTHON}" in log
    assert "restoring" in result.stdout


def test_units_declare_symmetric_conflicts() -> None:
    rust = (ROOT / "systemd" / RUST).read_text(encoding="utf-8")
    python = (ROOT / "systemd" / PYTHON).read_text(encoding="utf-8")

    assert f"Conflicts={PYTHON}" in rust
    assert f"Conflicts={RUST}" in python
