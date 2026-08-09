#!/usr/bin/env bash
set -Eeuo pipefail

readonly RUST_UNIT="codex-telegram-rust-full.service"
readonly PYTHON_UNIT="codex-telegram-bridge.service"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
info() { printf '%s\n' "$*"; }

require_user() {
  [[ "${EUID}" -ne 0 ]] || die "run as the bridge user, not root"
  [[ -n "${HOME:-}" && "${HOME}" != "/" ]] || die "HOME must identify the bridge user"
  command -v systemctl >/dev/null || die "systemctl is required"
}

is_active() { systemctl --user is-active --quiet "$1"; }
is_enabled() { systemctl --user is-enabled --quiet "$1"; }

wait_active() {
  local unit="$1" attempt
  for attempt in {1..20}; do
    is_active "${unit}" && return 0
    sleep 0.25
  done
  return 1
}

restore_owner() {
  local unit="$1" enabled="$2"
  systemctl --user stop "${RUST_UNIT}" "${PYTHON_UNIT}" 2>/dev/null || true
  if [[ "${enabled}" == true ]]; then
    systemctl --user enable "${unit}" >/dev/null || true
  else
    systemctl --user disable "${unit}" >/dev/null 2>&1 || true
  fi
  systemctl --user start "${unit}" || true
}

switch_owner() {
  local target="$1" force="$2" current_enabled=false opposite_was_active=false
  local current opposite
  case "${target}" in
    rust) current="${RUST_UNIT}"; opposite="${PYTHON_UNIT}" ;;
    python) current="${PYTHON_UNIT}"; opposite="${RUST_UNIT}" ;;
    *) die "target must be rust or python" ;;
  esac

  if is_active "${current}" && ! is_active "${opposite}"; then
    systemctl --user enable "${current}" >/dev/null
    systemctl --user disable "${opposite}" >/dev/null 2>&1 || true
    info "${current} is already the active Bridge owner"
    return 0
  fi

  if is_active "${opposite}"; then
    opposite_was_active=true
    [[ "${force}" == true ]] || die "${opposite} is active; retry with --force to switch owners"
    is_enabled "${opposite}" && current_enabled=true
    systemctl --user stop "${opposite}"
    systemctl --user disable "${opposite}" >/dev/null 2>&1 || true
  elif is_active "${RUST_UNIT}" && is_active "${PYTHON_UNIT}"; then
    die "both Bridge units are active; stop one before switching"
  fi

  systemctl --user reset-failed "${current}" >/dev/null 2>&1 || true
  systemctl --user enable "${current}" >/dev/null
  if ! systemctl --user start "${current}" || ! wait_active "${current}"; then
    systemctl --user disable --now "${current}" >/dev/null 2>&1 || true
    if [[ "${force}" == true && "${opposite_was_active}" == true ]]; then
      info "${current} failed health validation; restoring ${opposite}"
      restore_owner "${opposite}" "${current_enabled}"
    fi
    die "${current} failed to become active"
  fi
  systemctl --user disable "${opposite}" >/dev/null 2>&1 || true
  info "${current} is now the active Bridge owner; ${opposite} is disabled"
}

main() {
  require_user
  local target="${1:-}" force=false
  shift || true
  while (($#)); do
    case "$1" in
      --force) force=true ;;
      *) die "usage: $0 {rust|python} [--force]" ;;
    esac
    shift
  done
  [[ -n "${target}" ]] || die "usage: $0 {rust|python} [--force]"
  switch_owner "${target}" "${force}"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
