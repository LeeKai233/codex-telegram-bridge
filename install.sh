#!/usr/bin/env bash
set -Eeuo pipefail
IFS=$'\n\t'

readonly INSTALLER_VERSION="0.1.0"
readonly UV_VERSION="0.11.28"
readonly PROJECT="codex-telegram-bridge"
readonly REPOSITORY="LeeKai233/codex-telegram-bridge"
readonly BRIDGE_SOURCE="git+https://github.com/${REPOSITORY}@v${INSTALLER_VERSION}"
readonly CODEX_INSTALL_URL="https://chatgpt.com/codex/install.sh"
readonly CODEX_INSTALL_SHA256="1154e9daf713aacd1534efca8042bfd6665ad24bc1d1dfd86b8f439fe60a7a5d"
readonly UV_INSTALL_URL="https://astral.sh/uv/${UV_VERSION}/install.sh"
readonly UV_INSTALL_SHA256="b7b3fe80cad1142a2a5794050b7db7b3291d1bac1423b0732571dd9366e8ca8b"
readonly UNIT_MARKER="# X-CodexTelegramBridge-Installer: managed"
readonly UNIT_VERSION="# X-CodexTelegramBridge-Installer-Version: v${INSTALLER_VERSION}"
readonly GIB=$((1024 * 1024 * 1024))

SKIP_ONBOARDING=0
REUSE_EXISTING_APP_SERVER=0
CHECK_ONLY=0
PREFLIGHT_FAILED=0
EXTERNAL_APP_SERVER=0
TEMP_FILES=()

BIN_DIR=""
CONFIG_DIR=""
CONFIG_FILE=""
STATE_DIR=""
USER_UNIT_DIR=""
CODEX_HOME_DIR=""
CODEX_SOCKET=""
BRIDGE_BIN=""
CODEX_TG_BIN=""
CODEX_BIN=""
UV_BIN=""
PYTHON_BIN=""
CONTROL_IDENTITY="Control Bot"
DISCUSSION_IDENTITY="Discussion Bot"

info() {
    printf '[codex-tg] %s\n' "$*"
}

warn() {
    printf '[codex-tg] warning: %s\n' "$*" >&2
}

die() {
    printf '[codex-tg] error: %s\n' "$*" >&2
    exit 1
}

preflight_error() {
    printf '[codex-tg] preflight error: %s\n' "$*" >&2
    PREFLIGHT_FAILED=1
}

usage() {
    cat <<'EOF'
Usage: install.sh [OPTIONS]

Install Codex Telegram Bridge v0.1.0 for the current WSL2 user.

Options:
  --version                    Print the installer version and exit
  --check-only                 Run host, systemd, swap, disk, and network checks only
  --skip-onboarding            Install and start services without interactive Telegram onboarding
  --reuse-existing-app-server  Reuse an existing private Codex Unix socket instead of replacing it
  -h, --help                   Show this help
EOF
}

parse_args() {
    while (($#)); do
        case "$1" in
            --version)
                printf '%s installer %s\n' "$PROJECT" "$INSTALLER_VERSION"
                return 2
                ;;
            --check-only)
                CHECK_ONLY=1
                ;;
            --skip-onboarding)
                SKIP_ONBOARDING=1
                ;;
            --reuse-existing-app-server)
                REUSE_EXISTING_APP_SERVER=1
                ;;
            -h|--help)
                usage
                return 2
                ;;
            *)
                usage >&2
                die "unknown option: $1"
                ;;
        esac
        shift
    done
}

initialize_paths() {
    BIN_DIR="$HOME/.local/bin"
    CONFIG_DIR="$HOME/.config/codex-telegram-bridge"
    CONFIG_FILE="$CONFIG_DIR/config.toml"
    STATE_DIR="$HOME/.local/state/codex-telegram-bridge"
    USER_UNIT_DIR="$HOME/.config/systemd/user"
    CODEX_HOME_DIR="$HOME/.codex"
    CODEX_SOCKET="$CODEX_HOME_DIR/app-server-control/app-server-control.sock"
    BRIDGE_BIN="$BIN_DIR/codex-telegram-bridge"
    CODEX_TG_BIN="$BIN_DIR/codex-tg"
    CODEX_BIN="$BIN_DIR/codex"
}

cleanup_temp_files() {
    local path
    for path in "${TEMP_FILES[@]}"; do
        [[ -n "$path" ]] && rm -f -- "$path"
    done
}

download_script() {
    local result_name=$1
    local url=$2
    local label=$3
    local expected_sha256=$4
    local temporary actual_sha256
    temporary="$(mktemp "${TMPDIR:-/tmp}/codex-tg-download.XXXXXX")"
    TEMP_FILES+=("$temporary")
    if ! curl --proto '=https' --tlsv1.2 -fsSL \
        --connect-timeout 20 --max-time 180 --retry 3 --retry-all-errors \
        -o "$temporary" "$url"; then
        die "failed to download the official $label installer"
    fi
    [[ -s "$temporary" ]] || die "the downloaded $label installer is empty"
    actual_sha256="$(sha256sum "$temporary" | awk '{print $1}')"
    [[ "$actual_sha256" == "$expected_sha256" ]] || \
        die "the downloaded $label installer checksum does not match this release"
    sh -n "$temporary" || die "the downloaded $label installer is not valid shell syntax"
    printf -v "$result_name" '%s' "$temporary"
}

windows_user_home() {
    local windows_path
    if ! command -v cmd.exe >/dev/null 2>&1 || ! command -v wslpath >/dev/null 2>&1; then
        return 1
    fi
    if ! windows_path="$(cmd.exe /C 'echo %USERPROFILE%' 2>/dev/null | tr -d '\r' | tail -n 1)"; then
        return 1
    fi
    [[ -n "$windows_path" ]] || return 1
    wslpath -u "$windows_path" 2>/dev/null
}

ini_value() {
    local file=$1
    local wanted=$2
    awk -v wanted="$wanted" '
        function trim(value) {
            sub(/^[[:space:]]+/, "", value)
            sub(/[[:space:]]+$/, "", value)
            return value
        }
        /^[[:space:]]*\[/ {
            section = tolower(trim($0))
            active = (section == "[wsl2]")
            next
        }
        active {
            line = $0
            sub(/[[:space:]]*[#;].*$/, "", line)
            split_at = index(line, "=")
            if (!split_at) next
            key = tolower(trim(substr(line, 1, split_at - 1)))
            if (key != tolower(wanted)) next
            value = trim(substr(line, split_at + 1))
            if (value ~ /^".*"$/) value = substr(value, 2, length(value) - 2)
            result = value
        }
        END {
            if (result != "") print result
        }
    ' "$file"
}

windows_drive_info() {
    local requested_letter=$1
    local letter mount available target source fstype expected_source

    [[ "$requested_letter" =~ ^[A-Za-z]$ ]] || return 1
    letter=${requested_letter,,}
    mount="/mnt/$letter"
    [[ -d "$mount" ]] || return 1
    target="$(findmnt -rn -T "$mount" -o TARGET 2>/dev/null || true)"
    source="$(findmnt -rn -T "$mount" -o SOURCE 2>/dev/null || true)"
    fstype="$(findmnt -rn -T "$mount" -o FSTYPE 2>/dev/null || true)"
    expected_source="${letter^^}:\\x5c"
    [[ "$target" == "$mount" ]] || return 1
    [[ "$fstype" == 9p || "$fstype" == drvfs ]] || return 1
    [[ "$source" == "$expected_source" ]] || return 1
    available="$(df -P -B1 "$mount" 2>/dev/null | awk 'NR == 2 {print $4}')"
    [[ "$available" =~ ^[0-9]+$ ]] || return 1
    printf '%s\t%s\n' "$mount" "$available"
}

largest_windows_drive() {
    local mount available letter drive_info
    local best_mount="" best_available=0
    for mount in /mnt/[a-z]; do
        [[ -d "$mount" ]] || continue
        letter=${mount##*/}
        drive_info="$(windows_drive_info "$letter")" || continue
        IFS=$'\t' read -r mount available <<<"$drive_info"
        if ((available > best_available)); then
            best_available=$available
            best_mount=$mount
        fi
    done
    [[ -n "$best_mount" ]] || return 1
    printf '%s\t%s\n' "$best_mount" "$best_available"
}

recommend_swap_drive() {
    local recommendation drive available letter
    if ! recommendation="$(largest_windows_drive)"; then
        return
    fi
    IFS=$'\t' read -r drive available <<<"$recommendation"
    letter=${drive##*/}
    info "Largest mounted Windows drive: ${letter^^}: ($((available / GIB)) GiB free)."
    info "Suggested .wslconfig value: swapFile=${letter^^}:\\\\wsl-swap.vhdx"
}

check_default_swap_disk() {
    local c_mount=${1:-/mnt/c}
    local win_home="" wslconfig="" swap_size="" swap_file="" drive_info=""
    local swap_drive="" swap_mount="" available=""
    if win_home="$(windows_user_home)"; then
        wslconfig="$win_home/.wslconfig"
    fi

    if [[ -f "$wslconfig" ]]; then
        swap_size="$(ini_value "$wslconfig" swap || true)"
        swap_file="$(ini_value "$wslconfig" swapFile || true)"
    fi

    if [[ "${swap_size,,}" =~ ^0([[:space:]]*(b|kb|mb|gb|tb))?$ ]]; then
        warn "WSL swap is disabled in ${wslconfig:-.wslconfig}; memory pressure can terminate shells."
        if [[ -z "$swap_file" ]]; then
            recommend_swap_drive
            return
        fi
    fi

    if [[ -n "$swap_file" ]]; then
        if [[ ! "$swap_file" =~ ^([A-Za-z]):[\\/].+ ]]; then
            preflight_error "configured WSL swapFile is not an absolute local Windows drive path: $swap_file"
            recommend_swap_drive
            return
        fi
        swap_drive=${BASH_REMATCH[1]^^}
        if ! drive_info="$(windows_drive_info "$swap_drive")"; then
            preflight_error "configured WSL swapFile drive ${swap_drive}: is not a verifiably mounted local Windows drive"
            recommend_swap_drive
            return
        fi
        IFS=$'\t' read -r swap_mount available <<<"$drive_info"
    else
        swap_drive=C
        swap_mount=$c_mount
        if [[ ! -d "$swap_mount" ]]; then
            preflight_error "cannot inspect $swap_mount for the default WSL swap location"
            return
        fi
        available="$(df -P -B1 "$swap_mount" 2>/dev/null | awk 'NR == 2 {print $4}')"
        if [[ ! "$available" =~ ^[0-9]+$ ]]; then
            preflight_error "cannot determine free space on $swap_mount"
            return
        fi
    fi

    if ((available < 10 * GIB)); then
        preflight_error "${swap_drive}: has only $((available / GIB)) GiB free while WSL swap uses that drive; at least 10 GiB is required"
        recommend_swap_drive
    elif ((available < 20 * GIB)); then
        warn "${swap_drive}: has only $((available / GIB)) GiB free while WSL swap uses that drive; 20 GiB or more is recommended."
        recommend_swap_drive
    else
        info "WSL swap disk check passed: ${swap_drive}: has $((available / GIB)) GiB free."
    fi
}

kernel_log_current_boot() {
    if command -v journalctl >/dev/null 2>&1; then
        journalctl -k -b --no-pager 2>/dev/null && return 0
    fi
    dmesg 2>/dev/null || true
}

check_swap_io_errors() {
    local devices="swap" device pattern log
    while read -r device; do
        [[ -n "$device" ]] || continue
        device=${device##*/}
        [[ "$device" =~ ^[A-Za-z0-9._-]+$ ]] || continue
        devices="${devices}|${device}"
    done < <(awk 'NR > 1 {print $1}' /proc/swaps 2>/dev/null || true)
    pattern="((${devices}).*(I/O error|Input/output error|Buffer I/O error|blk_update_request))|((I/O error|Input/output error|Buffer I/O error|blk_update_request).*(${devices}))"
    log="$(kernel_log_current_boot)"
    if grep -Eiq "$pattern" <<<"$log"; then
        preflight_error "the current boot contains swap-device I/O errors; repair or relocate WSL swap and run 'wsl --shutdown' before installing"
    else
        info "No current-boot swap I/O errors detected."
    fi
}

check_network() {
    local url
    if ! command -v curl >/dev/null 2>&1; then
        warn "curl is missing; the installer will add it before downloads."
        return
    fi
    for url in "$UV_INSTALL_URL" "$CODEX_INSTALL_URL" "https://github.com"; do
        if ! curl -fsSL --max-time 20 --retry 1 -o /dev/null "$url"; then
            preflight_error "cannot reach $url"
        fi
    done
}

kernel_release() {
    tr '[:upper:]' '[:lower:]' </proc/sys/kernel/osrelease
}

pid_one_command() {
    ps -p 1 -o comm= 2>/dev/null | tr -d '[:space:]'
}

run_preflight() {
    local kernel pid_one distro_id distro_like command_name

    info "Running WSL and host preflight checks..."
    if ((EUID == 0)); then
        preflight_error "run this installer as a normal WSL user, not root"
    fi

    kernel="$(kernel_release)"
    if [[ "$kernel" != *microsoft-standard-wsl2* ]]; then
        preflight_error "only WSL2 is supported (kernel: $kernel)"
    fi

    pid_one="$(pid_one_command)"
    if [[ "$pid_one" != systemd ]]; then
        preflight_error "WSL systemd is required (PID 1 is ${pid_one:-unknown})"
    elif ! systemctl --user show-environment >/dev/null 2>&1; then
        preflight_error "the systemd user manager is unavailable in this shell"
    fi

    # shellcheck disable=SC1091
    . /etc/os-release
    distro_id=${ID:-}
    distro_like=${ID_LIKE:-}
    if [[ " ${distro_id,,} ${distro_like,,} " != *" ubuntu "* && " ${distro_id,,} ${distro_like,,} " != *" debian "* ]]; then
        preflight_error "only Ubuntu and Debian WSL distributions are supported (found ${distro_id:-unknown})"
    fi

    for command_name in apt-get sudo systemctl systemd-analyze loginctl findmnt stat sha256sum; do
        command -v "$command_name" >/dev/null 2>&1 || preflight_error "required command is missing: $command_name"
    done

    if ((CHECK_ONLY == 0)) && { [[ ! -t 0 ]] || [[ ! -t 1 ]]; }; then
        preflight_error "installation is interactive; run it from a WSL terminal"
    fi

    check_swap_io_errors
    check_default_swap_disk
    check_network

    if ((PREFLIGHT_FAILED)); then
        return 1
    fi
    info "Preflight checks passed."
}

validate_no_symlink_components() {
    local path=$1
    local relative component current
    [[ "$HOME" == /* && -d "$HOME" && ! -L "$HOME" ]] || die "HOME must be a real absolute directory"
    [[ "$path" == "$HOME"/* ]] || die "installer-managed path is outside HOME: $path"
    relative=${path#"$HOME"/}
    current=$HOME
    while IFS= read -r component; do
        [[ -n "$component" ]] || continue
        current="$current/$component"
        [[ ! -L "$current" ]] || die "refusing symbolic-link path component: $current"
        [[ ! -e "$current" || -d "$current" ]] || die "path component is not a directory: $current"
    done < <(tr '/' '\n' <<<"$relative")
}

prepare_private_config_dir() {
    local owner mode
    validate_no_symlink_components "$CONFIG_DIR"
    install -d -m 0700 "$CONFIG_DIR"
    validate_no_symlink_components "$CONFIG_DIR"
    owner="$(stat -c '%u' "$CONFIG_DIR")"
    mode="$(stat -c '%a' "$CONFIG_DIR")"
    [[ "$owner" == "$EUID" ]] || die "configuration directory is not owned by the current user"
    [[ "$mode" == 700 ]] || die "configuration directory permissions could not be secured to 0700"

    if [[ -e "$CONFIG_FILE" || -L "$CONFIG_FILE" ]]; then
        [[ -f "$CONFIG_FILE" && ! -L "$CONFIG_FILE" ]] || die "existing config.toml is not a regular file"
        owner="$(stat -c '%u' "$CONFIG_FILE")"
        mode="$(stat -c '%a' "$CONFIG_FILE")"
        [[ "$owner" == "$EUID" ]] || die "existing config.toml is not owned by the current user"
        (( (8#$mode & 022) == 0 )) || die "existing config.toml is writable by group or others"
    fi
}

prepare_user_unit_dir() {
    local owner mode
    validate_no_symlink_components "$USER_UNIT_DIR"
    install -d -m 0700 "$USER_UNIT_DIR"
    validate_no_symlink_components "$USER_UNIT_DIR"
    owner="$(stat -c '%u' "$USER_UNIT_DIR")"
    mode="$(stat -c '%a' "$USER_UNIT_DIR")"
    [[ "$owner" == "$EUID" ]] || die "systemd user unit directory is not owned by the current user"
    [[ "$mode" == 700 ]] || die "systemd user unit directory permissions could not be secured"
}

validate_existing_config_contract() {
    [[ -e "$CONFIG_FILE" ]] || return 0
    if ! CONFIG_FILE="$CONFIG_FILE" \
        EXPECTED_CONFIG_DIR="$CONFIG_DIR" \
        EXPECTED_STATE_DIR="$STATE_DIR" \
        EXPECTED_CODEX_HOME="$CODEX_HOME_DIR" \
        EXPECTED_CODEX_SOCKET="$CODEX_SOCKET" \
        EXPECTED_CODEX_BINARY="$CODEX_BIN" \
        "$PYTHON_BIN" <<'PY'
import os
import tomllib
from pathlib import Path


def lexical_absolute(value: object) -> Path:
    if not isinstance(value, (str, os.PathLike)):
        raise SystemExit("installer-managed path values must be strings")
    return Path(os.path.abspath(os.fspath(Path(value).expanduser())))


config_file = Path(os.environ["CONFIG_FILE"])
with config_file.open("rb") as handle:
    document = tomllib.load(handle)
source = document.get("bridge", document)
if not isinstance(source, dict):
    raise SystemExit("invalid [bridge] configuration")

expected = {
    "config_dir": Path(os.environ["EXPECTED_CONFIG_DIR"]),
    "state_dir": Path(os.environ["EXPECTED_STATE_DIR"]),
    "codex_home": Path(os.environ["EXPECTED_CODEX_HOME"]),
    "codex_socket": Path(os.environ["EXPECTED_CODEX_SOCKET"]),
    "codex_binary": Path(os.environ["EXPECTED_CODEX_BINARY"]),
}
for key, wanted in expected.items():
    actual = lexical_absolute(source.get(key, wanted))
    if actual != wanted:
        raise SystemExit(f"unsupported installer path override: {key}={actual}")
PY
    then
        die "existing config.toml uses unsupported custom installer paths"
    fi
    info "Existing config.toml uses the supported installer-managed paths."
}

install_os_dependencies() {
    local package missing=()
    for package in ca-certificates curl git tmux; do
        if ! dpkg-query -W -f='${Status}' "$package" 2>/dev/null | grep -q 'install ok installed'; then
            missing+=("$package")
        fi
    done
    if ((${#missing[@]} == 0)); then
        info "Ubuntu/Debian dependencies are already installed."
        return
    fi
    info "Installing OS dependencies: ${missing[*]}"
    sudo apt-get update
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${missing[@]}"
}

uv_version() {
    local executable=$1
    local output
    output="$("$executable" --version 2>/dev/null)" || return 1
    [[ "$output" =~ ^uv[[:space:]]+([0-9]+\.[0-9]+\.[0-9]+)([[:space:]]|$) ]] || return 1
    printf '%s\n' "${BASH_REMATCH[1]}"
}

install_uv_and_python() {
    local candidate="" installer installed_version=""
    mkdir -p "$BIN_DIR"
    if [[ -x "$BIN_DIR/uv" ]]; then
        candidate="$BIN_DIR/uv"
    elif candidate="$(command -v uv 2>/dev/null)" && [[ -n "$candidate" ]]; then
        :
    fi

    if [[ -n "$candidate" ]] && installed_version="$(uv_version "$candidate")" && \
        [[ "$installed_version" == "$UV_VERSION" ]]; then
        UV_BIN="$candidate"
        info "Reusing uv $UV_VERSION from $UV_BIN."
    else
        if [[ -n "$candidate" ]]; then
            warn "existing uv at $candidate is not the required version $UV_VERSION; installing the pinned version into $BIN_DIR"
        fi
        info "Installing uv without modifying shell profiles..."
        download_script installer "$UV_INSTALL_URL" "uv" "$UV_INSTALL_SHA256"
        env UV_NO_MODIFY_PATH=1 UV_UNMANAGED_INSTALL="$BIN_DIR" sh "$installer"
        UV_BIN="$BIN_DIR/uv"
    fi
    [[ -x "$UV_BIN" ]] || die "uv installation did not create an executable"
    installed_version="$(uv_version "$UV_BIN")" || die "cannot parse the installed uv version"
    [[ "$installed_version" == "$UV_VERSION" ]] || \
        die "uv version verification failed: expected $UV_VERSION, found $installed_version"

    info "Installing uv-managed Python 3.14..."
    "$UV_BIN" python install 3.14
    PYTHON_BIN="$("$UV_BIN" python find 3.14)"
    [[ -x "$PYTHON_BIN" ]] || die "uv could not locate Python 3.14 after installation"
}

install_bridge() {
    info "Installing ${PROJECT} from the pinned v${INSTALLER_VERSION} source reference..."
    UV_TOOL_BIN_DIR="$BIN_DIR" "$UV_BIN" tool install --force --python 3.14 "$BRIDGE_SOURCE"
    [[ -x "$BRIDGE_BIN" && -x "$CODEX_TG_BIN" ]] || die "bridge CLI executables were not installed in $BIN_DIR"
}

install_codex() {
    local installer
    info "Installing the latest official standalone Codex release..."
    download_script installer "$CODEX_INSTALL_URL" "Codex" "$CODEX_INSTALL_SHA256"
    env CODEX_HOME="$CODEX_HOME_DIR" CODEX_NON_INTERACTIVE=1 CODEX_INSTALL_DIR="$BIN_DIR" \
        sh "$installer"
    [[ -x "$CODEX_BIN" ]] || die "the official installer did not create $CODEX_BIN"

    local app_help codex_help
    app_help="$("$CODEX_BIN" app-server --help 2>&1)"
    codex_help="$("$CODEX_BIN" --help 2>&1)"
    grep -Fq -- '--listen' <<<"$app_help" || die "installed Codex lacks app-server --listen support"
    grep -Fq -- 'unix://' <<<"$app_help" || die "installed Codex lacks app-server Unix socket support"
    grep -Fq -- '--remote' <<<"$codex_help" || die "installed Codex lacks --remote support"
    grep -Fq -- 'unix://' <<<"$codex_help" || die "installed Codex lacks --remote Unix socket support"
    info "Codex capability checks passed ($("$CODEX_BIN" --version))."
}

pause_for_codex_configuration() {
    printf '\nCodex is installed at %s.\n' "$CODEX_BIN"
    printf 'In another WSL terminal, run it once and finish your preferred configuration/login:\n\n'
    printf '  CODEX_HOME=%q %q\n\n' "$CODEX_HOME_DIR" "$CODEX_BIN"
    read -r -p "Press Enter here after Codex is configured... " _
    if ! env CODEX_HOME="$CODEX_HOME_DIR" "$CODEX_BIN" login status >/dev/null 2>&1; then
        warn "Codex did not report a persisted login. Continuing because custom authentication may be configured."
    fi
}

json_identity() {
    local payload=$1
    local key=$2
    PAYLOAD="$payload" "$PYTHON_BIN" - "$key" <<'PY'
import json
import os
import sys

value = json.loads(os.environ["PAYLOAD"])[sys.argv[1]]
if not isinstance(value, str) or not value:
    raise SystemExit("invalid identity JSON")
print(value)
PY
}

display_identity() {
    local identity=$1
    if [[ "$identity" =~ ^[0-9]+$ ]]; then
        printf '%s\n' "$identity"
    else
        printf '@%s\n' "${identity#@}"
    fi
}

configure_tokens() {
    local control_path="$CONFIG_DIR/telegram_bot_token"
    local discussion_path="$CONFIG_DIR/telegram_426_bot_token"
    local identity_json

    CONTROL_IDENTITY="Control Bot"
    DISCUSSION_IDENTITY="Discussion Bot"
    if [[ -L "$control_path" || -L "$discussion_path" ]]; then
        die "refusing symbolic-link Telegram credential files"
    fi
    if [[ -e "$control_path" && -e "$discussion_path" ]]; then
        validate_existing_credential "$control_path"
        validate_existing_credential "$discussion_path"
        info "Existing Telegram credential files found; preserving them."
        return
    fi
    if [[ -e "$control_path" || -L "$control_path" || -e "$discussion_path" || -L "$discussion_path" ]]; then
        die "only one Telegram credential file exists; resolve the partial state manually before rerunning"
    fi

    printf '\nEnter the two different Telegram BotFather tokens. Input is hidden.\n'
    identity_json="$(env -u TELEGRAM_GPT_BOT_TOKEN -u TELEGRAM_426_BOT_TOKEN \
        CODEX_HOME="$CODEX_HOME_DIR" "$CODEX_TG_BIN" configure-tokens --prompt --json)"
    CONTROL_IDENTITY="$(display_identity "$(json_identity "$identity_json" control)")"
    DISCUSSION_IDENTITY="$(display_identity "$(json_identity "$identity_json" forum)")"
    info "Telegram tokens validated for $CONTROL_IDENTITY and $DISCUSSION_IDENTITY."
}

validate_existing_credential() {
    local path=$1
    local owner mode size
    [[ -f "$path" && ! -L "$path" ]] || die "Telegram credential is not a regular file: $path"
    owner="$(stat -c '%u' "$path")"
    mode="$(stat -c '%a' "$path")"
    size="$(stat -c '%s' "$path")"
    [[ "$owner" == "$EUID" ]] || die "Telegram credential is not owned by the current user: $path"
    [[ "$mode" == 600 ]] || die "Telegram credential permissions must be 0600: $path"
    ((size > 0 && size <= 4096)) || die "Telegram credential size is invalid: $path"
}

normalize_label() {
    local value=$1
    VALUE="$value" "$PYTHON_BIN" <<'PY'
import os
import unicodedata

value = os.environ["VALUE"].strip()
if not value:
    raise SystemExit("label cannot be empty")
if len(value) > 40:
    raise SystemExit("label must be 40 characters or fewer")
if any(
    unicodedata.category(character) == "Cc" or character in "\u2028\u2029"
    for character in value
):
    raise SystemExit("label contains a control character")
print(value)
PY
}

prompt_label() {
    local prompt=$1
    local default_value=$2
    local answer normalized
    while true; do
        read -r -p "$prompt [$default_value]: " answer
        answer=${answer:-$default_value}
        if normalized="$(normalize_label "$answer" 2>/dev/null)"; then
            printf '%s\n' "$normalized"
            return
        fi
        warn "label must be non-empty, at most 40 characters, and contain no control characters"
    done
}

toml_quote() {
    "$PYTHON_BIN" - "$1" <<'PY'
import json
import sys

print(json.dumps(sys.argv[1], ensure_ascii=False))
PY
}

write_initial_config() {
    local control_label discussion_label temporary
    prepare_private_config_dir
    if [[ -e "$CONFIG_FILE" || -L "$CONFIG_FILE" ]]; then
        [[ -f "$CONFIG_FILE" && ! -L "$CONFIG_FILE" ]] || die "existing config.toml is not a regular file"
        info "Existing config.toml found; preserving it without changes."
        return
    fi

    printf '\nChoose user-facing names for the two Bots.\n'
    control_label="$(prompt_label 'Control Bot name' "$CONTROL_IDENTITY")"
    discussion_label="$(prompt_label 'Discussion Bot name' "$DISCUSSION_IDENTITY")"
    temporary="$(mktemp "$CONFIG_DIR/.config.toml.XXXXXX")"
    chmod 0600 "$temporary"
    {
        printf '[bridge]\n'
        printf 'config_dir = %s\n' "$(toml_quote "$CONFIG_DIR")"
        printf 'state_dir = %s\n' "$(toml_quote "$STATE_DIR")"
        printf 'codex_home = %s\n' "$(toml_quote "$CODEX_HOME_DIR")"
        printf 'codex_socket = %s\n' "$(toml_quote "$CODEX_SOCKET")"
        printf 'codex_binary = %s\n' "$(toml_quote "$CODEX_BIN")"
        printf 'allowed_root = %s\n' "$(toml_quote "$HOME")"
        printf 'tmux_session = "CodexBot"\n'
        printf 'control_bot_label = %s\n' "$(toml_quote "$control_label")"
        printf 'discussion_bot_label = %s\n' "$(toml_quote "$discussion_label")"
    } >"$temporary"
    mv "$temporary" "$CONFIG_FILE"
    info "Created private configuration at $CONFIG_FILE."
}

semver_is_at_most() {
    local candidate=$1
    local baseline=$2
    local index candidate_component baseline_component
    local -a candidate_parts baseline_parts

    [[ "$candidate" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || return 1
    [[ "$baseline" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || return 1
    IFS=. read -r -a candidate_parts <<<"$candidate"
    IFS=. read -r -a baseline_parts <<<"$baseline"
    for index in 0 1 2; do
        candidate_component=${candidate_parts[$index]#"${candidate_parts[$index]%%[!0]*}"}
        baseline_component=${baseline_parts[$index]#"${baseline_parts[$index]%%[!0]*}"}
        candidate_component=${candidate_component:-0}
        baseline_component=${baseline_component:-0}
        if ((${#candidate_component} < ${#baseline_component})); then
            return 0
        elif ((${#candidate_component} > ${#baseline_component})); then
            return 1
        elif [[ "$candidate_component" < "$baseline_component" ]]; then
            return 0
        elif [[ "$candidate_component" > "$baseline_component" ]]; then
            return 1
        fi
    done
    return 0
}

unit_is_installer_owned() {
    local name=$1
    local local_path="$USER_UNIT_DIR/$name"
    local dropin_dir="$USER_UNIT_DIR/${name}.d"
    local first_line="" second_line="" declared_version="" owner mode dropins
    [[ -f "$local_path" && ! -L "$local_path" ]] || return 1
    owner="$(stat -c '%u' "$local_path" 2>/dev/null)" || return 1
    mode="$(stat -c '%a' "$local_path" 2>/dev/null)" || return 1
    [[ "$owner" == "$EUID" ]] || return 1
    (( (8#$mode & 022) == 0 )) || return 1
    {
        IFS= read -r first_line || true
        IFS= read -r second_line || true
    } <"$local_path"
    [[ "$first_line" == "$UNIT_MARKER" ]] || return 1
    [[ "$second_line" =~ ^#[[:space:]]X-CodexTelegramBridge-Installer-Version:[[:space:]]v([0-9]+\.[0-9]+\.[0-9]+)$ ]] || return 1
    declared_version=${BASH_REMATCH[1]}
    semver_is_at_most "$declared_version" "$INSTALLER_VERSION" || return 1
    [[ ! -L "$dropin_dir" ]] || return 1
    if [[ -d "$dropin_dir" ]] && find "$dropin_dir" -mindepth 1 -maxdepth 1 -print -quit | grep -q .; then
        return 1
    fi
    dropins="$(systemctl --user show "$name" --property=DropInPaths --value 2>/dev/null || true)"
    [[ -z "$dropins" ]]
}

unit_exists() {
    local name=$1
    [[ -e "$USER_UNIT_DIR/$name" || -L "$USER_UNIT_DIR/$name" ]] || systemctl --user cat "$name" >/dev/null 2>&1
}

validate_private_socket() {
    SOCKET_TO_CHECK="$CODEX_SOCKET" "$PYTHON_BIN" <<'PY'
import os
import socket
import stat

path = os.environ["SOCKET_TO_CHECK"]
metadata = os.stat(path, follow_symlinks=False)
if not stat.S_ISSOCK(metadata.st_mode):
    raise SystemExit(f"not a Unix socket: {path}")
if metadata.st_uid != os.getuid():
    raise SystemExit(f"socket is not owned by uid {os.getuid()}: {path}")
if stat.S_IMODE(metadata.st_mode) & 0o077:
    raise SystemExit(f"socket is accessible by group or others: {path}")
client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
client.settimeout(3)
try:
    client.connect(path)
finally:
    client.close()
PY
}

select_app_server_strategy() {
    local app_unit="codex-telegram-app-server.service"
    if unit_exists "$app_unit"; then
        if unit_is_installer_owned "$app_unit"; then
            EXTERNAL_APP_SERVER=0
        elif ((REUSE_EXISTING_APP_SERVER)); then
            [[ -S "$CODEX_SOCKET" ]] || die "unowned $app_unit exists but $CODEX_SOCKET is not active"
            validate_private_socket || die "existing Codex socket is not private and reachable"
            EXTERNAL_APP_SERVER=1
        else
            die "unowned $app_unit already exists; rerun with --reuse-existing-app-server only if its private socket is intentional"
        fi
    elif [[ -e "$CODEX_SOCKET" || -L "$CODEX_SOCKET" ]]; then
        if ((REUSE_EXISTING_APP_SERVER)); then
            validate_private_socket || die "existing Codex socket is not private and reachable"
            EXTERNAL_APP_SERVER=1
        else
            die "$CODEX_SOCKET already exists outside an installer-owned unit; rerun with --reuse-existing-app-server to keep it"
        fi
    fi

    if unit_exists codex-telegram-bridge.service && ! unit_is_installer_owned codex-telegram-bridge.service; then
        die "unowned codex-telegram-bridge.service already exists; refusing to overwrite it"
    fi
}

write_app_server_unit() {
    local destination="$USER_UNIT_DIR/codex-telegram-app-server.service"
    local temporary
    temporary="$(mktemp "$USER_UNIT_DIR/.codex-app-server.XXXXXX")"
    cat >"$temporary" <<EOF
$UNIT_MARKER
$UNIT_VERSION
[Unit]
Description=Codex App Server for Telegram Bridge
Documentation=https://github.com/$REPOSITORY
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/env CODEX_HOME=%h/.codex %h/.local/bin/codex app-server --listen unix://
Restart=on-failure
RestartSec=5s
TimeoutStopSec=45s
EnvironmentFile=-%h/.config/codex-telegram-bridge/proxy.env
UMask=0077

[Install]
WantedBy=default.target
EOF
    chmod 0644 "$temporary"
    mv "$temporary" "$destination"
}

write_bridge_unit() {
    local destination="$USER_UNIT_DIR/codex-telegram-bridge.service"
    local after="network-online.target"
    local temporary
    if ((EXTERNAL_APP_SERVER == 0)); then
        after="network-online.target codex-telegram-app-server.service"
    fi
    temporary="$(mktemp "$USER_UNIT_DIR/.codex-telegram-bridge.XXXXXX")"
    cat >"$temporary" <<EOF
$UNIT_MARKER
$UNIT_VERSION
[Unit]
Description=Codex Telegram Bridge
Documentation=https://github.com/$REPOSITORY
Wants=network-online.target
After=$after

[Service]
Type=simple
WorkingDirectory=%h
ExecStart=/usr/bin/env CODEX_HOME=%h/.codex %h/.local/bin/codex-telegram-bridge
Restart=on-failure
RestartSec=5s
TimeoutStopSec=45s
KillSignal=SIGTERM
KillMode=process
Environment=PYTHONUNBUFFERED=1
Environment=CODEX_HOME=%h/.codex
EnvironmentFile=-%h/.config/codex-telegram-bridge/proxy.env
UMask=0077

PrivateTmp=false
NoNewPrivileges=true
ProtectSystem=full
ProtectControlGroups=true
ProtectKernelModules=true
ProtectKernelTunables=true
ProtectKernelLogs=true
ProtectClock=true
LockPersonality=true
RestrictSUIDSGID=true
RestrictRealtime=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6

[Install]
WantedBy=default.target
EOF
    chmod 0644 "$temporary"
    mv "$temporary" "$destination"
}

wait_for_unit() {
    local name=$1
    local attempt
    for ((attempt = 0; attempt < 30; attempt++)); do
        if systemctl --user is-active --quiet "$name"; then
            return 0
        fi
        sleep 1
    done
    systemctl --user status --no-pager "$name" >&2 || true
    return 1
}

install_and_start_units() {
    local install_user
    install_user="$(id -un)"
    prepare_user_unit_dir
    select_app_server_strategy
    if ((EXTERNAL_APP_SERVER == 0)); then
        write_app_server_unit
    else
        info "Reusing the existing private Codex App Server socket."
    fi
    write_bridge_unit

    if ((EXTERNAL_APP_SERVER == 0)); then
        systemd-analyze --user verify \
            "$USER_UNIT_DIR/codex-telegram-app-server.service" \
            "$USER_UNIT_DIR/codex-telegram-bridge.service"
    else
        systemd-analyze --user verify "$USER_UNIT_DIR/codex-telegram-bridge.service"
    fi
    sudo loginctl enable-linger "$install_user"
    systemctl --user daemon-reload
    if ((EXTERNAL_APP_SERVER == 0)); then
        systemctl --user enable codex-telegram-app-server.service
        systemctl --user restart codex-telegram-app-server.service
        wait_for_unit codex-telegram-app-server.service || die "Codex App Server failed to start"
        validate_private_socket || die "managed Codex socket did not become private and reachable"
    fi
    systemctl --user enable codex-telegram-bridge.service
    systemctl --user restart codex-telegram-bridge.service
    wait_for_unit codex-telegram-bridge.service || die "Codex Telegram Bridge failed to start"
}

run_onboarding() {
    if ((SKIP_ONBOARDING)); then
        warn "Telegram onboarding was skipped. Run 'codex-tg onboard --timeout 600' when ready."
        env CODEX_HOME="$CODEX_HOME_DIR" "$CODEX_TG_BIN" doctor || \
            warn "doctor is expected to report incomplete pairing/binding until onboarding finishes"
        return
    fi
    env CODEX_HOME="$CODEX_HOME_DIR" "$CODEX_TG_BIN" onboard --timeout 600
    env CODEX_HOME="$CODEX_HOME_DIR" "$CODEX_TG_BIN" doctor
}

main() {
    local parse_status=0
    parse_args "$@" || parse_status=$?
    if ((parse_status == 2)); then
        return 0
    fi
    ((parse_status == 0)) || return "$parse_status"

    run_preflight || die "preflight failed; no installation changes were made"
    if ((CHECK_ONLY)); then
        info "Check-only mode complete; no installation changes were made."
        return 0
    fi

    initialize_paths
    trap cleanup_temp_files EXIT
    umask 0077
    prepare_private_config_dir
    prepare_user_unit_dir
    install_os_dependencies
    install_uv_and_python
    validate_existing_config_contract
    select_app_server_strategy
    install_bridge
    install_codex
    pause_for_codex_configuration
    configure_tokens
    write_initial_config
    install_and_start_units
    run_onboarding

    info "Installation complete."
    info "Services: systemctl --user status codex-telegram-app-server codex-telegram-bridge"
    info "Diagnostics: $CODEX_TG_BIN doctor"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    main "$@"
fi
