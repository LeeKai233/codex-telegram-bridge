from __future__ import annotations

import os
import socket
import stat
import subprocess
import sys
import threading
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INSTALLER = ROOT / "install.sh"
UNIT_MARKER = "# X-CodexTelegramBridge-Installer: managed"
UNIT_VERSION = "# X-CodexTelegramBridge-Installer-Version: v0.2.5"


def run_installer_shell(
    tmp_path: Path,
    body: str,
    *,
    extra_env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    env = os.environ.copy()
    env.update(
        {
            "HOME": str(tmp_path / "home"),
            "INSTALLER_UNDER_TEST": str(INSTALLER),
            "TMPDIR": str(tmp_path / "tmp"),
            "TEST_PYTHON": sys.executable,
        }
    )
    env.update(extra_env or {})
    Path(env["HOME"]).mkdir(mode=0o700, parents=True, exist_ok=True)
    Path(env["TMPDIR"]).mkdir(mode=0o700, parents=True, exist_ok=True)
    return subprocess.run(
        ["bash", "-c", 'source "$INSTALLER_UNDER_TEST"\n' + body],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )


def managed_unit() -> str:
    return f"{UNIT_MARKER}\n{UNIT_VERSION}\n[Unit]\nDescription=test\n"


def test_failed_script_download_is_nonzero_and_cleaned_up(tmp_path: Path) -> None:
    result = run_installer_shell(
        tmp_path,
        """
trap cleanup_temp_files EXIT
curl() { return 22; }
download_script downloaded https://example.invalid/install.sh test "$(printf '0%.0s' {1..64})"
""",
    )

    assert result.returncode == 1
    assert "failed to download" in result.stderr
    assert list((tmp_path / "tmp").iterdir()) == []


def test_downloaded_script_checksum_mismatch_is_rejected(tmp_path: Path) -> None:
    result = run_installer_shell(
        tmp_path,
        """
trap cleanup_temp_files EXIT
curl() {
    local output=""
    while (($#)); do
        if [[ "$1" == -o ]]; then
            output=$2
            shift
        fi
        shift
    done
    printf '#!/bin/sh\nexit 0\n' >"$output"
}
download_script downloaded https://example.invalid/install.sh test "$(printf '0%.0s' {1..64})"
""",
    )

    assert result.returncode == 1
    assert "checksum does not match" in result.stderr
    assert list((tmp_path / "tmp").iterdir()) == []


@pytest.mark.parametrize("symlink_parent", [False, True])
def test_private_config_preparation_rejects_symlink_components(
    tmp_path: Path, symlink_parent: bool
) -> None:
    home = tmp_path / "home"
    home.mkdir(mode=0o700)
    target = tmp_path / "target"
    target.mkdir()
    if symlink_parent:
        (home / ".config").symlink_to(target, target_is_directory=True)
    else:
        (home / ".config").mkdir()
        (home / ".config" / "codex-telegram-bridge").symlink_to(
            target, target_is_directory=True
        )

    result = run_installer_shell(tmp_path, "initialize_paths\nprepare_private_config_dir\n")

    assert result.returncode == 1
    assert "symbolic-link path component" in result.stderr
    assert list(target.iterdir()) == []


def test_custom_installer_paths_are_rejected_before_onboarding(tmp_path: Path) -> None:
    config_dir = tmp_path / "home" / ".config" / "codex-telegram-bridge"
    config_dir.mkdir(mode=0o700, parents=True)
    config = config_dir / "config.toml"
    config.write_text('[bridge]\ncodex_home = "/tmp/custom-codex"\n', encoding="utf-8")
    config.chmod(0o600)

    result = run_installer_shell(
        tmp_path,
        """
initialize_paths
PYTHON_BIN=$TEST_PYTHON
prepare_private_config_dir
validate_existing_config_contract
""",
    )

    assert result.returncode == 1
    assert "unsupported custom installer paths" in result.stderr


@pytest.mark.parametrize(
    "collision",
    ["vendor_dropin", "symlink", "misplaced_marker", "local_dropin", "system_dropin"],
)
def test_unit_ownership_rejects_spoofed_or_overridden_units(
    tmp_path: Path, collision: str
) -> None:
    unit_dir = tmp_path / "home" / ".config" / "systemd" / "user"
    unit_dir.mkdir(mode=0o700, parents=True)
    unit_path = unit_dir / "codex-telegram-app-server.service"
    if collision == "vendor_dropin":
        dropin = unit_dir / f"{unit_path.name}.d"
        dropin.mkdir()
        (dropin / "managed.conf").write_text(UNIT_MARKER + "\n", encoding="utf-8")
    elif collision == "symlink":
        target = tmp_path / "unit-target"
        target.write_text(managed_unit(), encoding="utf-8")
        unit_path.symlink_to(target)
    elif collision == "misplaced_marker":
        unit_path.write_text(f"[Unit]\n{UNIT_MARKER}\n{UNIT_VERSION}\n", encoding="utf-8")
    else:
        unit_path.write_text(managed_unit(), encoding="utf-8")
        if collision == "local_dropin":
            dropin = unit_dir / f"{unit_path.name}.d"
            dropin.mkdir()
            (dropin / "override.conf").write_text(
                "[Service]\nExecStart=\nExecStart=/bin/false\n", encoding="utf-8"
            )

    fake_dropins = "/etc/systemd/user/test.d/override.conf" if collision == "system_dropin" else ""
    result = run_installer_shell(
        tmp_path,
        """
initialize_paths
systemctl() {
    if [[ "$*" == *"DropInPaths"* ]]; then
        printf '%s\n' "${FAKE_DROPINS:-}"
    fi
}
if unit_is_installer_owned codex-telegram-app-server.service; then
    exit 99
fi
""",
        extra_env={"FAKE_DROPINS": fake_dropins},
    )

    assert result.returncode == 0, result.stderr


def test_exact_local_managed_unit_is_owned_and_reinstall_is_idempotent(tmp_path: Path) -> None:
    result = run_installer_shell(
        tmp_path,
        """
initialize_paths
prepare_private_config_dir
prepare_private_config_dir
install -d -m 0700 "$USER_UNIT_DIR"
systemctl() { return 0; }
write_app_server_unit
first=$(sha256sum "$USER_UNIT_DIR/codex-telegram-app-server.service")
unit_is_installer_owned codex-telegram-app-server.service
grep -Fq 'ExecStart=/usr/bin/env CODEX_HOME=%h/.codex' \
    "$USER_UNIT_DIR/codex-telegram-app-server.service"
write_app_server_unit
second=$(sha256sum "$USER_UNIT_DIR/codex-telegram-app-server.service")
[[ "$first" == "$second" ]]
""",
    )

    assert result.returncode == 0, result.stderr


def test_previous_installer_version_unit_is_owned_and_upgradable(tmp_path: Path) -> None:
    unit_dir = tmp_path / "home" / ".config" / "systemd" / "user"
    unit_dir.mkdir(mode=0o700, parents=True)
    unit = unit_dir / "codex-telegram-app-server.service"
    unit.write_text(
        f"{UNIT_MARKER}\n"
        "# X-CodexTelegramBridge-Installer-Version: v0.1.0\n"
        "[Unit]\nDescription=old\n",
        encoding="utf-8",
    )
    unit.chmod(0o600)
    result = run_installer_shell(
        tmp_path,
        """
initialize_paths
systemctl() { return 0; }
unit_is_installer_owned codex-telegram-app-server.service
write_app_server_unit
grep -Fxq "$UNIT_VERSION" "$USER_UNIT_DIR/codex-telegram-app-server.service"
""",
    )

    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("future_version", ["0.2.1", "1.0.0"])
def test_future_installer_version_unit_is_not_owned(
    tmp_path: Path, future_version: str
) -> None:
    unit_dir = tmp_path / "home" / ".config" / "systemd" / "user"
    unit_dir.mkdir(mode=0o700, parents=True)
    unit = unit_dir / "codex-telegram-app-server.service"
    unit.write_text(
        f"{UNIT_MARKER}\n"
        f"# X-CodexTelegramBridge-Installer-Version: v{future_version}\n"
        "[Unit]\nDescription=future\n",
        encoding="utf-8",
    )
    result = run_installer_shell(
        tmp_path,
        """
initialize_paths
systemctl() { return 0; }
if unit_is_installer_owned codex-telegram-app-server.service; then
    exit 99
fi
""",
    )

    assert result.returncode == 0, result.stderr


def test_low_default_swap_disk_sets_preflight_failure(tmp_path: Path) -> None:
    result = run_installer_shell(
        tmp_path,
        """
windows_user_home() { return 1; }
recommend_swap_drive() { :; }
windows_drive_info() {
    [[ "$1" == C ]] || return 1
    printf '/mnt/c\t%s\n' "$((5 * GIB))"
}
PREFLIGHT_FAILED=0
check_default_swap_disk
[[ "$PREFLIGHT_FAILED" == 1 ]]
""",
    )

    assert result.returncode == 0
    assert "C: has only 5 GiB free" in result.stderr


@pytest.mark.parametrize("failure", ["invalid_path", "unmounted", "low_space"])
def test_custom_swap_drive_failure_sets_preflight_failure(
    tmp_path: Path, failure: str
) -> None:
    win_home = tmp_path / "windows-home"
    win_home.mkdir()
    swap_file = "relative-swap.vhdx" if failure == "invalid_path" else "G:\\\\wsl-swap.vhdx"
    (win_home / ".wslconfig").write_text(
        f"[wsl2]\nswap=2GB\nswapFile={swap_file}\n", encoding="utf-8"
    )
    result = run_installer_shell(
        tmp_path,
        """
windows_user_home() { printf '%s\n' "$TEST_WIN_HOME"; }
windows_drive_info() {
    [[ "$TEST_FAILURE" != unmounted ]] || return 1
    printf '/mnt/g\t%s\n' "$((5 * GIB))"
}
recommend_swap_drive() { :; }
PREFLIGHT_FAILED=0
check_default_swap_disk
[[ "$PREFLIGHT_FAILED" == 1 ]]
""",
        extra_env={"TEST_FAILURE": failure, "TEST_WIN_HOME": str(win_home)},
    )

    assert result.returncode == 0, result.stderr
    if failure == "invalid_path":
        assert "not an absolute local Windows drive path" in result.stderr
    elif failure == "unmounted":
        assert "G: is not a verifiably mounted local Windows drive" in result.stderr
    else:
        assert "G: has only 5 GiB free" in result.stderr


def test_custom_swap_drive_passes_with_warning_below_twenty_gib(tmp_path: Path) -> None:
    win_home = tmp_path / "windows-home"
    win_home.mkdir()
    (win_home / ".wslconfig").write_text(
        "[wsl2]\nswap=2GB\nswapFile=G:\\\\wsl-swap.vhdx\n", encoding="utf-8"
    )
    result = run_installer_shell(
        tmp_path,
        """
windows_user_home() { printf '%s\n' "$TEST_WIN_HOME"; }
windows_drive_info() {
    [[ "$1" == G ]] || return 1
    printf '/mnt/g\t%s\n' "$((15 * GIB))"
}
recommend_swap_drive() { :; }
PREFLIGHT_FAILED=0
check_default_swap_disk
[[ "$PREFLIGHT_FAILED" == 0 ]]
""",
        extra_env={"TEST_WIN_HOME": str(win_home)},
    )

    assert result.returncode == 0, result.stderr
    assert "G: has only 15 GiB free" in result.stderr


def test_existing_uv_version_mismatch_installs_and_verifies_pinned_uv(
    tmp_path: Path,
) -> None:
    old_bin = tmp_path / "old-bin"
    old_bin.mkdir()
    old_uv = old_bin / "uv"
    old_uv.write_text("#!/bin/sh\nprintf 'uv 0.10.0\\n'\n", encoding="utf-8")
    old_uv.chmod(0o700)
    result = run_installer_shell(
        tmp_path,
        """
initialize_paths
PATH="$OLD_BIN:$PATH"
download_script() {
    local result_name=$1
    local fake_installer="$TMPDIR/fake-uv-installer.sh"
    cat >"$fake_installer" <<'INSTALLER'
#!/bin/sh
mkdir -p "$UV_UNMANAGED_INSTALL"
cat >"$UV_UNMANAGED_INSTALL/uv" <<'UV'
#!/bin/sh
if [ "$1" = --version ]; then
    printf 'uv 0.11.28\n'
elif [ "$1" = python ] && [ "$2" = install ]; then
    exit 0
elif [ "$1" = python ] && [ "$2" = find ]; then
    printf '%s\n' "$TEST_PYTHON"
else
    exit 2
fi
UV
chmod 0700 "$UV_UNMANAGED_INSTALL/uv"
INSTALLER
    chmod 0700 "$fake_installer"
    printf -v "$result_name" '%s' "$fake_installer"
}
install_uv_and_python
[[ "$UV_BIN" == "$BIN_DIR/uv" ]]
[[ "$(uv_version "$UV_BIN")" == "$UV_VERSION" ]]
""",
        extra_env={"OLD_BIN": str(old_bin)},
    )

    assert result.returncode == 0, result.stderr
    assert "is not the required version 0.11.28" in result.stderr


def test_generated_bridge_unit_matches_static_unit_and_forces_codex_home(
    tmp_path: Path,
) -> None:
    result = run_installer_shell(
        tmp_path,
        """
initialize_paths
install -d -m 0700 "$USER_UNIT_DIR"
write_bridge_unit
cmp -s "$STATIC_UNIT" "$USER_UNIT_DIR/codex-telegram-bridge.service"
grep -Fxq \
    'ExecStart=/usr/bin/env CODEX_HOME=%h/.codex %h/.local/bin/codex-telegram-bridge' \
    "$USER_UNIT_DIR/codex-telegram-bridge.service"
""",
        extra_env={"STATIC_UNIT": str(ROOT / "systemd/codex-telegram-bridge.service")},
    )

    assert result.returncode == 0, result.stderr


def test_non_wsl_host_contract_fails_without_mutation(tmp_path: Path) -> None:
    result = run_installer_shell(
        tmp_path,
        """
kernel_release() { printf '6.8.0-generic\n'; }
pid_one_command() { printf 'systemd\n'; }
systemctl() { return 0; }
apt-get() { :; }
sudo() { :; }
systemd-analyze() { :; }
loginctl() { :; }
findmnt() { :; }
check_swap_io_errors() { :; }
check_default_swap_disk() { :; }
check_network() { :; }
CHECK_ONLY=1
PREFLIGHT_FAILED=0
if run_preflight; then
    exit 99
fi
""",
    )

    assert result.returncode == 0
    assert "only WSL2 is supported" in result.stderr


def test_partial_credentials_are_rejected_without_invoking_cli(tmp_path: Path) -> None:
    result = run_installer_shell(
        tmp_path,
        """
initialize_paths
prepare_private_config_dir
printf 'placeholder\n' >"$CONFIG_DIR/telegram_bot_token"
chmod 0600 "$CONFIG_DIR/telegram_bot_token"
CODEX_TG_BIN=/bin/false
configure_tokens
""",
    )

    assert result.returncode == 1
    assert "only one Telegram credential file exists" in result.stderr


def test_external_private_socket_can_be_reused_explicitly(tmp_path: Path) -> None:
    socket_path = tmp_path / "codex.sock"
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(str(socket_path))
    socket_path.chmod(0o600)
    server.listen(1)

    def accept_once() -> None:
        try:
            connection, _address = server.accept()
            connection.close()
        except OSError:
            pass

    thread = threading.Thread(target=accept_once, daemon=True)
    thread.start()
    try:
        result = run_installer_shell(
            tmp_path,
            """
initialize_paths
CODEX_SOCKET=$TEST_SOCKET
PYTHON_BIN=$TEST_PYTHON
REUSE_EXISTING_APP_SERVER=1
unit_exists() { [[ "$1" == codex-telegram-app-server.service ]]; }
unit_is_installer_owned() { return 1; }
select_app_server_strategy
[[ "$EXTERNAL_APP_SERVER" == 1 ]]
""",
            extra_env={"TEST_SOCKET": str(socket_path)},
        )
    finally:
        server.close()
        thread.join(timeout=2)

    assert result.returncode == 0, result.stderr


def test_token_command_argv_environment_and_output_do_not_contain_secrets(tmp_path: Path) -> None:
    fake_cli = tmp_path / "fake-codex-tg"
    capture = tmp_path / "capture"
    fake_cli.write_text(
        "#!/bin/sh\n"
        "printf 'argv=%s\\n' \"$*\" >\"$CAPTURE\"\n"
        "env | sort >>\"$CAPTURE\"\n"
        "printf '{\"control\":\"control_bot\",\"forum\":\"forum_bot\"}\\n'\n",
        encoding="utf-8",
    )
    fake_cli.chmod(0o700)
    control_secret = "123456789:CONTROL_SECRET_VALUE"
    forum_secret = "987654321:FORUM_SECRET_VALUE"
    result = run_installer_shell(
        tmp_path,
        """
initialize_paths
prepare_private_config_dir
PYTHON_BIN=$TEST_PYTHON
CODEX_TG_BIN=$FAKE_CLI
configure_tokens
""",
        extra_env={
            "CAPTURE": str(capture),
            "FAKE_CLI": str(fake_cli),
            "TELEGRAM_GPT_BOT_TOKEN": control_secret,
            "TELEGRAM_426_BOT_TOKEN": forum_secret,
        },
    )

    captured = capture.read_text(encoding="utf-8")
    combined = result.stdout + result.stderr + captured
    assert result.returncode == 0, result.stderr
    assert control_secret not in combined
    assert forum_secret not in combined
    assert "configure-tokens --prompt --json" in captured
    assert stat.S_IMODE(fake_cli.stat().st_mode) == 0o700
