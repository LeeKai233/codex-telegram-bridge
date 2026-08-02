#!/usr/bin/env bash
set -Eeuo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd -P)"
readonly PROJECT_ROOT
readonly USER_UNIT_DIR="${HOME}/.config/systemd/user"
readonly CONFIG_ROOT="${HOME}/.config/codex-telegram-bridge/monitoring"
readonly STATE_ROOT="${HOME}/.local/state/codex-telegram-bridge/monitoring"
readonly DATA_ROOT="${HOME}/.local/share/codex-telegram-bridge/monitoring"
readonly CACHE_ROOT="${HOME}/.cache/codex-telegram-bridge/monitoring"
readonly PREFIX_ROOT="${HOME}/.local/opt/codex-telegram-bridge/monitoring"
readonly UNIT_PREFIX="codex-telegram"

readonly PROMETHEUS_VERSION="3.13.2"
readonly PROMETHEUS_ARCHIVE="prometheus-${PROMETHEUS_VERSION}.linux-amd64.tar.gz"
readonly PROMETHEUS_SHA256="0e8c4d46101bd025ea8265e377d2caabc57f488fc1be1c367f37db69ea41be6f"
readonly PROMETHEUS_URL="https://github.com/prometheus/prometheus/releases/download/v${PROMETHEUS_VERSION}/${PROMETHEUS_ARCHIVE}"
readonly PROMETHEUS_DIR="${PREFIX_ROOT}/prometheus-${PROMETHEUS_VERSION}"

readonly ALERTMANAGER_VERSION="0.33.1"
readonly ALERTMANAGER_ARCHIVE="alertmanager-${ALERTMANAGER_VERSION}.linux-amd64.tar.gz"
readonly ALERTMANAGER_SHA256="93d802cba6a8d27239d747ce117df7648d326ab67394e32247540b030e9842ba"
readonly ALERTMANAGER_URL="https://github.com/prometheus/alertmanager/releases/download/v${ALERTMANAGER_VERSION}/${ALERTMANAGER_ARCHIVE}"
readonly ALERTMANAGER_DIR="${PREFIX_ROOT}/alertmanager-${ALERTMANAGER_VERSION}"

readonly GRAFANA_VERSION="13.1.1"
readonly GRAFANA_ARCHIVE="grafana-${GRAFANA_VERSION}.linux-amd64.tar.gz"
readonly GRAFANA_SHA256="0c07116968aea49768af8babd3c3f162d19012655a1a220cd7a9d97efe91da6c"
readonly GRAFANA_URL="https://dl.grafana.com/oss/release/${GRAFANA_ARCHIVE}"
readonly GRAFANA_DIR="${PREFIX_ROOT}/grafana-${GRAFANA_VERSION}"

readonly PROMETHEUS_CONFIG_DIR="${CONFIG_ROOT}/prometheus"
readonly PROMETHEUS_ALERT_DIR="${PROMETHEUS_CONFIG_DIR}/alerts"
readonly ALERTMANAGER_CONFIG_DIR="${CONFIG_ROOT}/alertmanager"
readonly GRAFANA_CONFIG_DIR="${CONFIG_ROOT}/grafana"
readonly GRAFANA_PROVISIONING_DIR="${GRAFANA_CONFIG_DIR}/provisioning"
readonly GRAFANA_DASHBOARD_DIR="${GRAFANA_PROVISIONING_DIR}/dashboards"
readonly GRAFANA_DATASOURCE_DIR="${GRAFANA_PROVISIONING_DIR}/datasources"
readonly GRAFANA_PLUGIN_PROVISIONING_DIR="${GRAFANA_PROVISIONING_DIR}/plugins"
readonly GRAFANA_ALERT_PROVISIONING_DIR="${GRAFANA_PROVISIONING_DIR}/alerting"
readonly PROMETHEUS_STATE_DIR="${STATE_ROOT}/prometheus"
readonly ALERTMANAGER_STATE_DIR="${STATE_ROOT}/alertmanager"
readonly GRAFANA_STATE_DIR="${STATE_ROOT}/grafana"
readonly GRAFANA_DATA_DIR="${DATA_ROOT}/grafana"
readonly GRAFANA_LOG_DIR="${STATE_ROOT}/grafana/log"
readonly GRAFANA_PLUGIN_DIR="${DATA_ROOT}/grafana/plugins"

readonly PROMETHEUS_UNIT="${UNIT_PREFIX}-prometheus.service"
readonly ALERTMANAGER_UNIT="${UNIT_PREFIX}-alertmanager.service"
readonly GRAFANA_UNIT="${UNIT_PREFIX}-grafana.service"

die() {
  printf 'error: %s\n' "$*" >&2
  exit 1
}

info() {
  printf '%s\n' "$*"
}

require_user() {
  [[ "${EUID}" -ne 0 ]] || die "run as the bridge user, not root"
  [[ -n "${HOME:-}" && "${HOME}" != "/" ]] || die "HOME must identify the bridge user"
  for command_name in curl install jq sha256sum systemctl tar; do
    command -v "${command_name}" >/dev/null || die "${command_name} is required"
  done
  [[ -r "${PROJECT_ROOT}/monitoring/prometheus/prometheus.yml" ]] || die "run from a complete checkout"
  [[ -r "${PROJECT_ROOT}/monitoring/alertmanager/alertmanager.yml" ]] || die "run from a complete checkout"
  [[ -r "${PROJECT_ROOT}/monitoring/grafana/grafana.ini" ]] || die "run from a complete checkout"
  [[ -r "${PROJECT_ROOT}/monitoring/grafana/dashboards/codex-telegram-bridge.json" ]] || die "run from a complete checkout"
  [[ -r "${PROJECT_ROOT}/systemd/${PROMETHEUS_UNIT}" ]] || die "missing ${PROMETHEUS_UNIT} template"
  [[ -r "${PROJECT_ROOT}/systemd/${ALERTMANAGER_UNIT}" ]] || die "missing ${ALERTMANAGER_UNIT} template"
  [[ -r "${PROJECT_ROOT}/systemd/${GRAFANA_UNIT}" ]] || die "missing ${GRAFANA_UNIT} template"
}

ensure_directories() {
  install -d -m 700 \
    "${USER_UNIT_DIR}" \
    "${CONFIG_ROOT}" \
    "${PROMETHEUS_CONFIG_DIR}" \
    "${PROMETHEUS_ALERT_DIR}" \
    "${ALERTMANAGER_CONFIG_DIR}" \
    "${GRAFANA_CONFIG_DIR}" \
    "${GRAFANA_DASHBOARD_DIR}" \
    "${GRAFANA_DATASOURCE_DIR}" \
    "${GRAFANA_PLUGIN_PROVISIONING_DIR}" \
    "${GRAFANA_ALERT_PROVISIONING_DIR}" \
    "${PROMETHEUS_STATE_DIR}" \
    "${ALERTMANAGER_STATE_DIR}" \
    "${GRAFANA_STATE_DIR}" \
    "${GRAFANA_DATA_DIR}" \
    "${GRAFANA_LOG_DIR}" \
    "${GRAFANA_PLUGIN_DIR}" \
    "${CACHE_ROOT}" \
    "${PREFIX_ROOT}"
  # install -d preserves an existing directory's mode; restore the private
  # boundary when an earlier deployment created one with broader permissions.
  chmod 700 "${CONFIG_ROOT}" "${STATE_ROOT}" "${DATA_ROOT}" "${CACHE_ROOT}" "${PREFIX_ROOT}"
}

verify_archive() {
  local expected_sum="$1"
  local archive_path="$2"
  printf '%s  %s\n' "${expected_sum}" "${archive_path}" | sha256sum --check --status -
}

download_archive() {
  local archive_name="$1"
  local url="$2"
  local expected_sum="$3"
  local archive_path="${CACHE_ROOT}/${archive_name}"

  if [[ -f "${archive_path}" ]] && ! verify_archive "${expected_sum}" "${archive_path}"; then
    info "discarding a checksum-mismatched cached archive: ${archive_name}" >&2
    rm -f -- "${archive_path}"
  fi
  if [[ ! -f "${archive_path}" ]]; then
    info "downloading ${archive_name}" >&2
    curl --fail --location --retry 3 --connect-timeout 15 --output "${archive_path}.part" "${url}"
    verify_archive "${expected_sum}" "${archive_path}.part" || die "checksum verification failed for ${archive_name}"
    mv -f -- "${archive_path}.part" "${archive_path}"
  fi
  verify_archive "${expected_sum}" "${archive_path}" || die "checksum verification failed for ${archive_name}"
  printf '%s\n' "${archive_path}"
}

extract_archive() {
  local archive_name="$1"
  local url="$2"
  local expected_sum="$3"
  local destination="$4"
  local archive_path
  local stage
  local extracted

  [[ -x "${destination}/prometheus" || -x "${destination}/promtool" || -x "${destination}/alertmanager" || -x "${destination}/amtool" || -x "${destination}/bin/grafana" ]] && return 0

  archive_path="$(download_archive "${archive_name}" "${url}" "${expected_sum}")"
  stage="$(mktemp -d "${PREFIX_ROOT}/.extract.XXXXXX")"
  if ! tar --extract --gzip --file "${archive_path}" --directory "${stage}"; then
    rm -rf -- "${stage}"
    die "could not extract ${archive_name}"
  fi
  extracted="$(find "${stage}" -mindepth 1 -maxdepth 1 -type d -print -quit)"
  [[ -n "${extracted}" && -d "${extracted}" ]] || {
    rm -rf -- "${stage}"
    die "archive ${archive_name} did not contain a top-level directory"
  }
  [[ ! -e "${destination}" ]] || {
    rm -rf -- "${stage}"
    die "refusing to replace existing installation ${destination}"
  }
  mv -- "${extracted}" "${destination}"
  rmdir -- "${stage}" 2>/dev/null || true
}

install_binaries() {
  extract_archive "${PROMETHEUS_ARCHIVE}" "${PROMETHEUS_URL}" "${PROMETHEUS_SHA256}" "${PROMETHEUS_DIR}"
  extract_archive "${ALERTMANAGER_ARCHIVE}" "${ALERTMANAGER_URL}" "${ALERTMANAGER_SHA256}" "${ALERTMANAGER_DIR}"
  extract_archive "${GRAFANA_ARCHIVE}" "${GRAFANA_URL}" "${GRAFANA_SHA256}" "${GRAFANA_DIR}"
  [[ -x "${PROMETHEUS_DIR}/prometheus" && -x "${PROMETHEUS_DIR}/promtool" ]] || die "Prometheus binaries are missing"
  [[ -x "${ALERTMANAGER_DIR}/alertmanager" && -x "${ALERTMANAGER_DIR}/amtool" ]] || die "Alertmanager binaries are missing"
  [[ -x "${GRAFANA_DIR}/bin/grafana" ]] || die "Grafana binary is missing"
}

backup_existing_config() {
  local source_path="$1"
  local relative_name="$2"
  local backup_root
  backup_root="${STATE_ROOT}/backups/$(date -u +%Y%m%dT%H%M%SZ)"
  if [[ -e "${source_path}" ]]; then
    install -d -m 700 "$(dirname -- "${backup_root}/${relative_name}")"
    cp -p -- "${source_path}" "${backup_root}/${relative_name}"
    info "backed up ${source_path} to ${backup_root}/${relative_name}"
  fi
}

render_configs() {
  local temporary
  backup_existing_config "${PROMETHEUS_CONFIG_DIR}/prometheus.yml" prometheus.yml
  backup_existing_config "${PROMETHEUS_ALERT_DIR}/bridge.yml" alerts/bridge.yml
  backup_existing_config "${ALERTMANAGER_CONFIG_DIR}/alertmanager.yml" alertmanager.yml
  backup_existing_config "${GRAFANA_CONFIG_DIR}/grafana.ini" grafana.ini

  install -m 644 "${PROJECT_ROOT}/monitoring/prometheus/prometheus.yml" "${PROMETHEUS_CONFIG_DIR}/prometheus.yml"
  install -m 644 "${PROJECT_ROOT}/monitoring/prometheus/alerts/bridge.yml" "${PROMETHEUS_ALERT_DIR}/bridge.yml"
  install -m 644 "${PROJECT_ROOT}/monitoring/alertmanager/alertmanager.yml" "${ALERTMANAGER_CONFIG_DIR}/alertmanager.yml"
  install -m 644 "${PROJECT_ROOT}/monitoring/grafana/grafana.ini" "${GRAFANA_CONFIG_DIR}/grafana.ini"
  install -m 644 "${PROJECT_ROOT}/monitoring/grafana/dashboards/codex-telegram-bridge.json" "${GRAFANA_DASHBOARD_DIR}/codex-telegram-bridge.json"

  temporary="$(mktemp "${GRAFANA_CONFIG_DIR}/.grafana.ini.XXXXXX")"
  {
    cat "${GRAFANA_CONFIG_DIR}/grafana.ini"
    printf '\n[paths]\n'
    printf 'data = %s\n' "${GRAFANA_DATA_DIR}"
    printf 'logs = %s\n' "${GRAFANA_LOG_DIR}"
    printf 'plugins = %s\n' "${GRAFANA_PLUGIN_DIR}"
    printf 'provisioning = %s\n' "${GRAFANA_PROVISIONING_DIR}"
    printf '\n[plugins]\n'
    printf 'plugin_admin_enabled = false\n'
    printf 'preinstall_disabled = true\n'
    printf 'preinstall_auto_update = false\n'
    printf 'public_key_retrieval_disabled = true\n'
  } >"${temporary}"
  chmod 644 "${temporary}"
  mv -f -- "${temporary}" "${GRAFANA_CONFIG_DIR}/grafana.ini"

  temporary="$(mktemp "${GRAFANA_DASHBOARD_DIR}/.bridge.yml.XXXXXX")"
  sed "s|/etc/grafana/provisioning/dashboards|${GRAFANA_DASHBOARD_DIR//\//\/}|g" \
    "${PROJECT_ROOT}/monitoring/grafana/provisioning/dashboards/bridge.yml" >"${temporary}"
  chmod 644 "${temporary}"
  mv -f -- "${temporary}" "${GRAFANA_DASHBOARD_DIR}/bridge.yml"
  install -m 644 "${PROJECT_ROOT}/monitoring/grafana/provisioning/datasources/prometheus.yml" \
    "${GRAFANA_DATASOURCE_DIR}/prometheus.yml"
}

install_units() {
  backup_existing_config "${USER_UNIT_DIR}/${PROMETHEUS_UNIT}" "${PROMETHEUS_UNIT}"
  backup_existing_config "${USER_UNIT_DIR}/${ALERTMANAGER_UNIT}" "${ALERTMANAGER_UNIT}"
  backup_existing_config "${USER_UNIT_DIR}/${GRAFANA_UNIT}" "${GRAFANA_UNIT}"
  install -m 644 "${PROJECT_ROOT}/systemd/${PROMETHEUS_UNIT}" "${USER_UNIT_DIR}/${PROMETHEUS_UNIT}"
  install -m 644 "${PROJECT_ROOT}/systemd/${ALERTMANAGER_UNIT}" "${USER_UNIT_DIR}/${ALERTMANAGER_UNIT}"
  install -m 644 "${PROJECT_ROOT}/systemd/${GRAFANA_UNIT}" "${USER_UNIT_DIR}/${GRAFANA_UNIT}"
  systemctl --user daemon-reload
}

validate_configs() {
  info "validating Prometheus configuration"
  "${PROMETHEUS_DIR}/promtool" check config "${PROMETHEUS_CONFIG_DIR}/prometheus.yml"
  "${PROMETHEUS_DIR}/promtool" check rules "${PROMETHEUS_ALERT_DIR}/bridge.yml"
  info "validating Alertmanager configuration"
  "${ALERTMANAGER_DIR}/amtool" check-config "${ALERTMANAGER_CONFIG_DIR}/alertmanager.yml"
  info "validating Grafana dashboard JSON"
  jq empty "${GRAFANA_DASHBOARD_DIR}/codex-telegram-bridge.json"
}

start_services() {
  systemctl --user enable "${PROMETHEUS_UNIT}" "${ALERTMANAGER_UNIT}" "${GRAFANA_UNIT}"
  systemctl --user restart "${PROMETHEUS_UNIT}" "${ALERTMANAGER_UNIT}" "${GRAFANA_UNIT}"
}

stop_services() {
  systemctl --user stop "${GRAFANA_UNIT}" "${ALERTMANAGER_UNIT}" "${PROMETHEUS_UNIT}" 2>/dev/null || true
}

show_status() {
  systemctl --user --no-pager --full status "${PROMETHEUS_UNIT}" "${ALERTMANAGER_UNIT}" "${GRAFANA_UNIT}" || true
  printf '%s\n' 'Endpoints: Prometheus http://127.0.0.1:9090, Alertmanager http://127.0.0.1:9093, Grafana http://127.0.0.1:3000'
}

install_stack() {
  require_user
  ensure_directories
  install_binaries
  render_configs
  install_units
  validate_configs
  start_services
  show_status
}

case "${1:-}" in
  install)
    install_stack
    ;;
  start)
    require_user
    start_services
    show_status
    ;;
  stop)
    require_user
    stop_services
    ;;
  status)
    require_user
    show_status
    ;;
  validate)
    require_user
    install_binaries
    validate_configs
    ;;
  *)
    die "usage: $0 {install|start|stop|status|validate}"
    ;;
esac
