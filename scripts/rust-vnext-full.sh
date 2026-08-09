#!/usr/bin/env bash
set -Eeuo pipefail

readonly UNIT_NAME="codex-telegram-rust-full.service"
PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly PROJECT_ROOT
readonly USER_UNIT_DIR="${HOME}/.config/systemd/user"
readonly CONFIG_DIR="${HOME}/.config/codex-telegram-bridge"
readonly CONFIG_PATH="${CONFIG_DIR}/rust-vnext.toml"
readonly STATE_DIR="${HOME}/.local/state/codex-telegram-bridge/rust-vnext-full"
readonly STATE_DB_PATH="${STATE_DIR}/state.sqlite3"
readonly LOCK_DIR="${STATE_DIR}/leases"
readonly BINARY_PATH="${HOME}/.local/bin/codex-telegram-cli-rust-full"
readonly UNIT_PATH="${USER_UNIT_DIR}/${UNIT_NAME}"
readonly CONFIG_TEMPLATE="${PROJECT_ROOT}/docs/rust-vnext/full-testing.config.example.toml"
readonly UNIT_TEMPLATE="${PROJECT_ROOT}/systemd/${UNIT_NAME}"
readonly SERVICE_SWITCH="${PROJECT_ROOT}/scripts/bridge-service.sh"

die() { printf 'error: %s\n' "$*" >&2; exit 1; }
info() { printf '%s\n' "$*"; }

require_user() {
  [[ "${EUID}" -ne 0 ]] || die "run as the bridge user, not root"
  [[ -n "${HOME:-}" && "${HOME}" != "/" ]] || die "HOME must identify the bridge user"
  command -v systemctl >/dev/null || die "systemctl is required"
  [[ -r "${CONFIG_TEMPLATE}" && -r "${UNIT_TEMPLATE}" && -x "${SERVICE_SWITCH}" ]] || die "run from a complete checkout"
}

build_binary() {
  info "building Rust CLI"
  cargo build --release --locked --manifest-path "${PROJECT_ROOT}/rust/Cargo.toml" -p codex-telegram-cli
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
  if [[ -e "${BINARY_PATH}" ]]; then cp -p -- "${BINARY_PATH}" "${backup}/$(basename -- "${BINARY_PATH}")"; else : >"${backup}/binary-absent"; fi
  if [[ -e "${CONFIG_PATH}" ]]; then cp -p -- "${CONFIG_PATH}" "${backup}/rust-vnext.toml"; else : >"${backup}/config-absent"; fi
  if [[ -e "${UNIT_PATH}" ]]; then cp -p -- "${UNIT_PATH}" "${backup}/${UNIT_NAME}"; else : >"${backup}/unit-absent"; fi
  if [[ -e "${STATE_DB_PATH}" ]]; then
    command -v sqlite3 >/dev/null || die "sqlite3 is required for Rust rollback snapshots"
    sqlite3 "${STATE_DB_PATH}" ".backup '${backup}/state.sqlite3'"
    chmod 600 "${backup}/state.sqlite3"
  else
    : >"${backup}/state-absent"
  fi
  info "saved rollback bundle under ${backup}"
  prune_rollback_bundles
}

# Keep only the newest rollback bundles so upgrades do not accumulate an
# unbounded series of full SQLite snapshots under the state directory.
prune_rollback_bundles() {
  local keep=3 old
  find "${STATE_DIR}" -mindepth 1 -maxdepth 1 -type d -name 'rollback-*' -printf '%T@ %p\n' 2>/dev/null \
    | sort -nr | awk -v keep="${keep}" 'NR > keep { sub(/^[^ ]+ /, ""); print }' \
    | while IFS= read -r old; do
      [[ -n "${old}" && "${old}" == "${STATE_DIR}"/rollback-* ]] || continue
      rm -rf -- "${old}"
      info "pruned old rollback bundle ${old}"
    done
}

latest_rollback_bundle() {
  find "${STATE_DIR}" -mindepth 1 -maxdepth 1 -type d -name 'rollback-*' -printf '%T@ %p\n' 2>/dev/null \
    | sort -nr | awk 'NR == 1 { sub(/^[^ ]+ /, ""); print }'
}

install_full() {
  require_user
  install -d -m 700 "${CONFIG_DIR}" "${USER_UNIT_DIR}" "${STATE_DIR}" "${LOCK_DIR}" "${HOME}/.local/bin"
  # The SQLite snapshot runs concurrently with the release build; both must
  # succeed before the new unit is enabled.
  save_rollback_bundle &
  local backup_pid=$!
  build_binary
  wait "${backup_pid}" || die "rollback snapshot failed; aborting install"
  render_config
  install -m 644 "${UNIT_TEMPLATE}" "${UNIT_PATH}"
  validate_config
  systemctl --user daemon-reload
  systemctl --user disable codex-telegram-bridge.service >/dev/null 2>&1 || true
  systemctl --user enable "${UNIT_NAME}" >/dev/null
  info "installed ${UNIT_NAME}; Rust is enabled for the next user-session boot and Python is disabled"
}

start_rust() {
  require_user
  "${SERVICE_SWITCH}" rust "${@:1}"
  systemctl --user --no-pager --full status "${UNIT_NAME}"
}

stop_rust() {
  require_user
  systemctl --user disable --now "${UNIT_NAME}" 2>/dev/null || true
  info "stopped ${UNIT_NAME}"
}

cutover() {
  require_user
  "${SERVICE_SWITCH}" rust --force
  systemctl --user --no-pager --full status "${UNIT_NAME}"
}

upgrade() {
  require_user
  systemctl --user is-active --quiet "${UNIT_NAME}" \
    || die "${UNIT_NAME} is not active; use install/start before upgrade"
  install -d -m 700 "${CONFIG_DIR}" "${USER_UNIT_DIR}" "${STATE_DIR}" "${LOCK_DIR}" "${HOME}/.local/bin"
  # The SQLite snapshot runs concurrently with the release build; the build
  # usually dominates, so the backup adds no wall time to the upgrade.
  save_rollback_bundle &
  local backup_pid=$!
  build_binary
  wait "${backup_pid}" || die "rollback snapshot failed; aborting upgrade before restart"
  if [[ -e "${CONFIG_PATH}" ]]; then validate_config; else render_config; validate_config; fi
  install -m 644 "${UNIT_TEMPLATE}" "${UNIT_PATH}"
  systemctl --user daemon-reload
  if ! systemctl --user restart "${UNIT_NAME}"; then
    info "Rust upgrade failed; restoring the previous Rust binary"
    restore_binary_from_latest || true
    systemctl --user daemon-reload
    systemctl --user start "${UNIT_NAME}" || true
    die "Rust upgrade failed; Python Bridge was not started or modified"
  fi
  # Bounded readiness wait (60s, polling every 2s) instead of a single
  # immediate check that raced the service startup.
  local waited=0
  until systemctl --user is-active --quiet "${UNIT_NAME}"; do
    if (( waited >= 60 )); then
      info "Rust upgrade did not become active; restoring the previous Rust binary"
      restore_binary_from_latest || true
      systemctl --user daemon-reload
      systemctl --user start "${UNIT_NAME}" || true
      die "Rust upgrade health check failed; Python Bridge was not started or modified"
    fi
    sleep 2
    waited=$((waited + 2))
  done
  info "Rust upgrade completed; ${UNIT_NAME} is active"
}

rollback() {
  require_user
  "${SERVICE_SWITCH}" python --force
  systemctl --user --no-pager --full status codex-telegram-bridge.service
  info "Python Bridge restored as the active owner; Rust remains disabled"
}

restore_binary_from_latest() {
  local backup binary
  backup="$(latest_rollback_bundle)"
  [[ -n "${backup}" ]] || die "no Rust rollback bundle found under ${STATE_DIR}"
  binary="${backup}/$(basename -- "${BINARY_PATH}")"
  [[ -f "${binary}" ]] || die "rollback bundle has no previous Rust binary: ${backup}"
  install -m 755 "${binary}" "${BINARY_PATH}"
  restore_state_database_from "${backup}"
  info "restored Rust binary from ${backup}"
}

restore_state_database_from() {
  local backup="$1"
  install -d -m 700 "${STATE_DIR}"
  rm -f -- "${STATE_DB_PATH}" "${STATE_DB_PATH}-wal" "${STATE_DB_PATH}-shm"
  if [[ -f "${backup}/state.sqlite3" ]]; then
    install -m 600 "${backup}/state.sqlite3" "${STATE_DB_PATH}"
  elif [[ -f "${backup}/state-absent" ]]; then
    :
  else
    die "rollback bundle has no Rust state snapshot: ${backup}"
  fi
}

rollback_upgrade() {
  require_user
  local backup
  backup="$(latest_rollback_bundle)"
  [[ -n "${backup}" ]] || die "no Rust rollback bundle found under ${STATE_DIR}"
  stop_rust
  restore_binary_from_latest
  if [[ -f "${backup}/${UNIT_NAME}" ]]; then install -m 644 "${backup}/${UNIT_NAME}" "${UNIT_PATH}"; fi
  if [[ -f "${backup}/rust-vnext.toml" ]]; then install -m 600 "${backup}/rust-vnext.toml" "${CONFIG_PATH}"; fi
  systemctl --user daemon-reload
  systemctl --user start "${UNIT_NAME}"
  systemctl --user is-active --quiet "${UNIT_NAME}" \
    || die "Rust rollback did not become active; Python Bridge was not started or modified"
  info "Rust upgrade rolled back; ${UNIT_NAME} is active"
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
  start) shift; start_rust "$@" ;;
  stop) stop_rust ;;
  cutover) cutover ;;
  rollback) rollback ;;
  upgrade) upgrade ;;
  rollback-upgrade) rollback_upgrade ;;
  restore-files) restore_files ;;
  *) die "usage: $0 {install|start|stop|upgrade|rollback-upgrade|cutover|rollback|restore-files}" ;;
esac
