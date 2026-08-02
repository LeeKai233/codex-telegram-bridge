#!/usr/bin/env bash
set -Eeuo pipefail

readonly UNIT_NAME="codex-telegram-rust-full.service"
PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly PROJECT_ROOT
readonly USER_UNIT_DIR="${HOME}/.config/systemd/user"
readonly CONFIG_DIR="${HOME}/.config/codex-telegram-bridge"
readonly CONFIG_PATH="${CONFIG_DIR}/rust-vnext.toml"
readonly STATE_DIR="${HOME}/.local/state/codex-telegram-bridge/rust-vnext-full"
readonly LOCK_DIR="${STATE_DIR}/leases"
readonly BINARY_PATH="${HOME}/.local/bin/codex-telegram-cli-rust-full"
readonly UNIT_PATH="${USER_UNIT_DIR}/${UNIT_NAME}"
readonly CONFIG_TEMPLATE="${PROJECT_ROOT}/docs/rust-vnext/full-testing.config.example.toml"
readonly UNIT_TEMPLATE="${PROJECT_ROOT}/systemd/${UNIT_NAME}"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
info() { printf '%s\n' "$*"; }

require_user() {
  [[ "${EUID}" -ne 0 ]] || die "run as the bridge user, not root"
  [[ -n "${HOME:-}" && "${HOME}" != "/" ]] || die "HOME must identify the bridge user"
  command -v systemctl >/dev/null || die "systemctl is required"
  [[ -r "${CONFIG_TEMPLATE}" && -r "${UNIT_TEMPLATE}" ]] || die "run from a complete checkout"
}

build_binary() {
  info "building Rust CLI"
  cargo build --release --manifest-path "${PROJECT_ROOT}/rust/Cargo.toml" -p codex-telegram-cli
  install -D -m 755 "${PROJECT_ROOT}/rust/target/release/codex-telegram-cli" "${BINARY_PATH}"
}

render_config() {
  local temporary
  temporary="$(mktemp "${CONFIG_DIR}/.rust-vnext.XXXXXX")"
  sed -e "s|@HOME@|${HOME}|g" -e "s|@PROJECT_ROOT@|${PROJECT_ROOT}|g" "${CONFIG_TEMPLATE}" >"${temporary}"
  chmod 600 "${temporary}"
  mv -f -- "${temporary}" "${CONFIG_PATH}"
}

validate_config() {
  [[ "$(stat -c '%a' "${CONFIG_PATH}")" == 600 ]] || die "Rust config must be mode 0600"
  awk -F'"' '/^[[:space:]]*credential_key[[:space:]]*=/ { seen = 1; if ($2 !~ /^rust_[A-Za-z0-9_]+$/) bad = 1 } END { exit (seen && !bad) ? 0 : 1 }' "${CONFIG_PATH}" \
    || die "all live-test credential keys must use rust_*"
  grep -Eq '^metrics_bind[[:space:]]*=[[:space:]]*"127\.0\.0\.1:9465"' "${CONFIG_PATH}" \
    || die "Rust metrics must remain on 127.0.0.1:9465"
  grep -Eq '^poll_updates[[:space:]]*=[[:space:]]*true$' "${CONFIG_PATH}" \
    || die "Rust live test must explicitly enable polling"
  "${BINARY_PATH}" validate >/dev/null
}

save_rollback_bundle() {
  local backup
  backup="$(mktemp -d "${STATE_DIR}/rollback-XXXXXXXX")"
  if [[ -e "${CONFIG_PATH}" ]]; then cp -p -- "${CONFIG_PATH}" "${backup}/rust-vnext.toml"; else : >"${backup}/config-absent"; fi
  if [[ -e "${UNIT_PATH}" ]]; then cp -p -- "${UNIT_PATH}" "${backup}/${UNIT_NAME}"; else : >"${backup}/unit-absent"; fi
  info "saved rollback bundle under ${backup}"
}

install_full() {
  require_user
  install -d -m 700 "${CONFIG_DIR}" "${USER_UNIT_DIR}" "${STATE_DIR}" "${LOCK_DIR}" "${HOME}/.local/bin"
  save_rollback_bundle
  build_binary
  render_config
  install -m 644 "${UNIT_TEMPLATE}" "${UNIT_PATH}"
  validate_config
  systemctl --user daemon-reload
  info "installed ${UNIT_NAME}; service remains stopped until cutover"
}

start_rust() {
  require_user
  systemctl --user enable --now "${UNIT_NAME}"
  systemctl --user --no-pager --full status "${UNIT_NAME}"
}

stop_rust() {
  require_user
  systemctl --user disable --now "${UNIT_NAME}" 2>/dev/null || true
  info "stopped ${UNIT_NAME}"
}

cutover() {
  require_user
  systemctl --user is-active --quiet codex-telegram-bridge.service \
    || die "Python Bridge is not active; refusing automatic cutover"
  systemctl --user stop codex-telegram-bridge.service
  if ! start_rust; then
    info "Rust full runtime did not start; restoring Python Bridge"
    systemctl --user start codex-telegram-bridge.service || true
    die "Rust cutover failed; Python restart was attempted"
  fi
  info "Rust full runtime is now the active Telegram owner; Python remains installed but stopped"
}

rollback() {
  require_user
  stop_rust
  systemctl --user start codex-telegram-bridge.service
  systemctl --user --no-pager --full status codex-telegram-bridge.service
  info "Python Bridge restored; Rust full runtime remains stopped"
}

restore_files() {
  require_user
  local backup
  backup="$(find "${STATE_DIR}" -mindepth 1 -maxdepth 1 -type d -name 'rollback-*' -printf '%T@ %p\n' 2>/dev/null | sort -nr | awk 'NR == 1 { sub(/^[^ ]+ /, ""); print }')"
  [[ -n "${backup}" ]] || die "no rollback bundle found under ${STATE_DIR}"
  stop_rust
  if [[ -f "${backup}/${UNIT_NAME}" ]]; then install -m 644 "${backup}/${UNIT_NAME}" "${UNIT_PATH}"; elif [[ -f "${backup}/unit-absent" ]]; then rm -f -- "${UNIT_PATH}"; fi
  if [[ -f "${backup}/rust-vnext.toml" ]]; then install -m 600 "${backup}/rust-vnext.toml" "${CONFIG_PATH}"; elif [[ -f "${backup}/config-absent" ]]; then rm -f -- "${CONFIG_PATH}"; fi
  systemctl --user daemon-reload
  info "restored Rust deployment files from ${backup}; no service was started"
}

case "${1:-}" in
  install) install_full ;;
  start) start_rust ;;
  stop) stop_rust ;;
  cutover) cutover ;;
  rollback) rollback ;;
  restore-files) restore_files ;;
  *) die "usage: $0 {install|start|stop|cutover|rollback|restore-files}" ;;
esac
