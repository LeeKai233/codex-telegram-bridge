use codex_telegram_adapter::{ReqwestTransport, TelegramBotApi};
use codex_telegram_cli::config::RustConfig;
use codex_telegram_cli::metrics::{MetricsRegistry, MetricsServer};
use codex_telegram_cli::migration::{import_python_database, inspect_legacy_database};
use codex_telegram_cli::replay::run_fixture;
use codex_telegram_cli::security::{TotpManager, is_private_regular_file};
use ctg_storage_sqlite::SqliteStore;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;

const UNSUPPORTED_ONBOARDING: &str = "pairing, binding, and onboarding require the Rust runtime state protocol; this CLI does not emulate Python state";

#[derive(Debug, Error)]
enum CliError {
    #[error("output: {0}")]
    Output(#[from] io::Error),
    #[error("{0}")]
    Usage(String),
    #[error("{0}")]
    Unsupported(String),
    #[error("{0}")]
    Runtime(String),
}

impl CliError {
    fn exit_code(&self) -> i32 {
        match self {
            Self::Usage(_) | Self::Unsupported(_) => 2,
            Self::Runtime(_) | Self::Output(_) => 1,
        }
    }
}

fn main() {
    let mut output = io::stdout().lock();
    match run(env::args().skip(1).collect(), &mut output) {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            let _ = writeln!(io::stderr(), "codex-telegram-cli: {error}");
            std::process::exit(error.exit_code());
        }
    }
}

fn run(arguments: Vec<String>, output: &mut dyn Write) -> Result<i32, CliError> {
    let (config_path, arguments) = split_global_config(arguments)?;
    let command = arguments.first().map(String::as_str).unwrap_or("validate");
    let rest = &arguments[usize::from(!arguments.is_empty())..];
    match command {
        "validate" | "config-check" => validate(config_path.as_deref(), rest, output),
        "generate-config" => generate_config(rest, output),
        "probe" => probe(config_path.as_deref(), rest, output),
        "metrics" => serve_metrics(config_path.as_deref(), rest, output),
        "daemon" => daemon(config_path.as_deref(), rest),
        "migration-inspect" => migration_inspect(rest, output),
        "import-python-state" => import_python_state(rest, output),
        "replay" => replay(rest, output),
        "doctor" => doctor(config_path.as_deref(), rest, output),
        "status" => status(config_path.as_deref(), rest, output),
        "app-server-watchdog" => watchdog(config_path.as_deref(), rest, output),
        "pair-code" => print_one_time_code("pair", config_path.as_deref(), output),
        "bind-code" => print_one_time_code("bind", config_path.as_deref(), output),
        "onboard" => onboard(config_path.as_deref(), rest, output),
        "configure-token" | "migrate-token" | "configure-tokens" | "totp-enroll" | "totp-reset" => {
            Err(CliError::Unsupported(format!(
                "{command} is unavailable: {UNSUPPORTED_ONBOARDING}"
            )))
        }
        "owner-reset" => owner_reset(config_path.as_deref(), rest, output),
        "lock" => lock(config_path.as_deref(), rest, output),
        "help" | "--help" | "-h" => {
            print_help(output)?;
            Ok(0)
        }
        other => Err(CliError::Usage(format!("unknown command: {other}"))),
    }
}

fn split_global_config(arguments: Vec<String>) -> Result<(Option<PathBuf>, Vec<String>), CliError> {
    let mut config_path = None;
    let mut remaining = Vec::new();
    let mut index = 0;
    while index < arguments.len() {
        if arguments[index] == "--config" {
            let path = arguments
                .get(index + 1)
                .ok_or_else(|| CliError::Usage("--config requires PATH".into()))?;
            if config_path.replace(PathBuf::from(path)).is_some() {
                return Err(CliError::Usage("--config may be specified once".into()));
            }
            index += 2;
        } else {
            remaining.push(arguments[index].clone());
            index += 1;
        }
    }
    Ok((config_path, remaining))
}

fn load_config(config_path: Option<&Path>) -> Result<RustConfig, CliError> {
    match config_path {
        Some(path) => RustConfig::load(path).map_err(|error| CliError::Runtime(error.to_string())),
        None => RustConfig::load_default().map_err(|error| CliError::Runtime(error.to_string())),
    }
}

fn validate(
    config_path: Option<&Path>,
    arguments: &[String],
    output: &mut dyn Write,
) -> Result<i32, CliError> {
    let json_output = parse_no_value_flags(arguments, &["--json"])?.contains(&"--json");
    let config = load_config(config_path)?;
    let bots = config
        .enabled_bot_bindings()
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    let surfaces = config
        .surface_bindings()
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    let issues = codex_telegram_cli::validate_configuration(
        &codex_telegram_credentials::CredentialFiles::discover(&config.credential_registry),
        &bots,
        &surfaces,
    );
    if json_output {
        let issues = issues
            .into_iter()
            .map(|issue| json!({"code": issue.code, "message": issue.message}))
            .collect::<Vec<_>>();
        write_json(
            output,
            json!({"enabled_bot_bindings": bots.len(), "surface_bindings": surfaces.len(), "issues": issues}),
        )?;
    } else {
        for issue in issues {
            writeln!(output, "{}: {}", issue.code, issue.message)?;
        }
        writeln!(
            output,
            "validated {} enabled bot binding(s) and {} surface binding(s)",
            bots.len(),
            surfaces.len()
        )?;
    }
    Ok(0)
}

fn generate_config(arguments: &[String], output: &mut dyn Write) -> Result<i32, CliError> {
    if arguments.len() > 1
        || arguments
            .first()
            .is_some_and(|value| value.starts_with('-'))
    {
        return Err(CliError::Usage("usage: generate-config [PATH]".into()));
    }
    let path = arguments
        .first()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("rust-vnext.toml"));
    RustConfig::default()
        .save_template(&path)
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    writeln!(
        output,
        "wrote Rust vNext config template: {}",
        path.display()
    )?;
    Ok(0)
}

fn probe(
    config_path: Option<&Path>,
    arguments: &[String],
    output: &mut dyn Write,
) -> Result<i32, CliError> {
    ensure_empty(arguments, "usage: probe")?;
    let config = load_config(config_path)?;
    let registry = config
        .credentials()
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    let transport = ReqwestTransport::new(Duration::from_secs(config.request_timeout_seconds))
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    let api = TelegramBotApi::with_api_base(transport, &config.api_base);
    for bot in config.bots.iter().filter(|bot| bot.enabled) {
        let token = config
            .token_for(bot, &registry)
            .map_err(|error| CliError::Runtime(error.to_string()))?;
        let profile = api
            .get_me(&token)
            .map_err(|error| CliError::Runtime(error.to_string()))?;
        writeln!(
            output,
            "{}: bot_id={} username={}",
            bot.instance_id,
            profile.id,
            profile.username.as_deref().unwrap_or("<none>")
        )?;
        for surface in config
            .surfaces
            .iter()
            .filter(|surface| surface.bot_instance_id == bot.instance_id)
        {
            let chat = api
                .get_chat(&token, surface.chat_id)
                .map_err(|error| CliError::Runtime(error.to_string()))?;
            let member = api
                .get_chat_member(&token, surface.chat_id, profile.id)
                .map_err(|error| CliError::Runtime(error.to_string()))?;
            writeln!(
                output,
                "{}: surface={:?} chat_id={} chat_type={} forum={} linked_chat_id={:?} member_status={} can_post={} can_edit={} can_delete={}",
                bot.instance_id,
                surface.kind,
                surface.chat_id,
                chat.chat_type,
                chat.is_forum,
                chat.linked_chat_id,
                member.status,
                member.can_post_messages,
                member.can_edit_messages,
                member.can_delete_messages,
            )?;
        }
    }
    Ok(0)
}

fn serve_metrics(
    config_path: Option<&Path>,
    arguments: &[String],
    output: &mut dyn Write,
) -> Result<i32, CliError> {
    ensure_empty(arguments, "usage: metrics")?;
    let config = load_config(config_path)?;
    let _server = MetricsServer::start(&config.metrics_bind, MetricsRegistry::default())
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    writeln!(output, "metrics listening on {}", config.metrics_bind)?;
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}

fn daemon(config_path: Option<&Path>, arguments: &[String]) -> Result<i32, CliError> {
    ensure_empty(arguments, "usage: daemon [--config PATH]")?;
    codex_telegram_cli::daemon::run(config_path)
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    Ok(0)
}

fn replay(arguments: &[String], output: &mut dyn Write) -> Result<i32, CliError> {
    let fixture = value_after(arguments, "--fixture")
        .ok_or_else(|| CliError::Usage("replay requires --fixture PATH".into()))?;
    let scenario = value_after(arguments, "--scenario").unwrap_or_else(|| "fixture".into());
    let implementation =
        value_after(arguments, "--implementation").unwrap_or_else(|| "rust-vnext".into());
    let repetitions = parse_u64_flag(arguments, "--repetitions", 1)?;
    let warmup = parse_u64_flag(arguments, "--warmup", 0)?;
    let output_path = value_after(arguments, "--output");
    ensure_replay_grammar(arguments)?;
    let report = run_fixture(fixture, scenario, implementation, repetitions, warmup)
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    let payload = serde_json::to_string_pretty(&report)
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    if let Some(path) = output_path {
        fs::write(path, format!("{payload}\n"))
            .map_err(|error| CliError::Runtime(error.to_string()))?;
    } else {
        writeln!(output, "{payload}")?;
    }
    Ok(0)
}

fn migration_inspect(arguments: &[String], output: &mut dyn Write) -> Result<i32, CliError> {
    if arguments.len() != 1 || arguments[0].starts_with('-') {
        return Err(CliError::Usage(
            "usage: migration-inspect SQLITE_PATH".into(),
        ));
    }
    let report = inspect_legacy_database(&arguments[0])
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    let json = serde_json::to_string_pretty(&report)
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    writeln!(output, "{json}")?;
    Ok(0)
}

fn import_python_state(arguments: &[String], output: &mut dyn Write) -> Result<i32, CliError> {
    let source = value_after(arguments, "--source")
        .ok_or_else(|| CliError::Usage("import-python-state requires --source PATH".into()))?;
    let target = value_after(arguments, "--target")
        .ok_or_else(|| CliError::Usage("import-python-state requires --target PATH".into()))?;
    let report = value_after(arguments, "--report")
        .ok_or_else(|| CliError::Usage("import-python-state requires --report PATH".into()))?;
    let dry_run = arguments.iter().any(|argument| argument == "--dry-run");
    if arguments.iter().any(|argument| {
        argument.starts_with('-')
            && !matches!(
                argument.as_str(),
                "--source" | "--target" | "--report" | "--dry-run"
            )
    }) {
        return Err(CliError::Usage(
            "usage: import-python-state --source PATH --target PATH --report PATH [--dry-run]"
                .into(),
        ));
    }
    let report = import_python_database(source, target, report, dry_run)
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    let json = serde_json::to_string_pretty(&report)
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    writeln!(output, "{json}")?;
    Ok(0)
}

fn print_one_time_code(
    kind: &str,
    config_path: Option<&Path>,
    output: &mut dyn Write,
) -> Result<i32, CliError> {
    let config = load_config(config_path)?;
    fs::create_dir_all(&config.state_directory)
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&config.state_directory, fs::Permissions::from_mode(0o700))
            .map_err(|error| CliError::Runtime(error.to_string()))?;
    }
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| CliError::Runtime(error.to_string()))?
        .as_millis() as i64;
    let entropy = format!("{kind}:{now_ms}:{}", std::process::id());
    let digest = Sha256::digest(entropy.as_bytes());
    let code = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) % 1_000_000;
    let code = format!("{code:06}");
    let digest = Sha256::digest(code.as_bytes());
    let store = SqliteStore::open(config.state_directory.join("state.sqlite3"))
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    store
        .upsert_workflow_record(
            "onboarding_code",
            kind,
            &json!({
                "sha256": format!("{:x}", digest),
                "expires_at_ms": now_ms.saturating_add(600_000),
                "failures": 0,
            }),
            now_ms,
        )
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    writeln!(output, "{kind}-code: {code}")?;
    Ok(0)
}

fn onboard(
    config_path: Option<&Path>,
    arguments: &[String],
    output: &mut dyn Write,
) -> Result<i32, CliError> {
    let code = doctor(config_path, arguments, output)?;
    if code != 0 {
        return Ok(code);
    }
    writeln!(
        output,
        "onboard: runtime checks complete; generate pair-code, send it to Control Bot, then generate bind-code and send it to Discussion Bot"
    )?;
    Ok(0)
}

fn doctor(
    config_path: Option<&Path>,
    arguments: &[String],
    output: &mut dyn Write,
) -> Result<i32, CliError> {
    let flags = parse_no_value_flags(arguments, &["--offline", "--json"])?;
    let json_output = flags.contains(&"--json");
    let config = load_config(config_path)?;
    let mut failures = Vec::new();
    let mut warnings = Vec::new();
    let registry = file_state(&config.credential_registry);
    if registry != "private" {
        failures.push(format!("credential registry: {registry}"));
    }
    let totp = file_state(&config.totp_secret_path);
    if totp != "private" {
        warnings.push(format!("TOTP secret: {totp}"));
    }
    let socket = file_state(&config.codex_socket);
    if socket != "private" {
        failures.push(format!("Codex app-server socket: {socket}"));
    }
    let state_directory = directory_state(&config.state_directory);
    if state_directory == "unsafe" {
        failures.push("state directory: unsafe".into());
    }
    if json_output {
        write_json(
            output,
            json!({
                "offline": true,
                "requested_offline": flags.contains(&"--offline"),
                "credentials": {"registry": registry, "tokens": "not read"},
                "totp": totp,
                "app_server": {"socket": socket, "probe": "skipped"},
                "state_directory": state_directory,
                "warnings": warnings,
                "failures": failures,
            }),
        )?;
    } else {
        writeln!(
            output,
            "[OK] Rust CLI doctor runs without Telegram or app-server network probes"
        )?;
        writeln!(
            output,
            "[OK] credential registry: {registry}; token contents not read"
        )?;
        writeln!(
            output,
            "[{}] TOTP secret: {totp}",
            if warnings.iter().any(|item| item.starts_with("TOTP")) {
                "WARN"
            } else {
                "OK"
            }
        )?;
        writeln!(
            output,
            "[{}] Codex app-server socket: {socket}; protocol probe skipped",
            if failures.iter().any(|item| item.starts_with("Codex")) {
                "FAIL"
            } else {
                "OK"
            }
        )?;
        writeln!(output, "[OK] state directory: {state_directory}")?;
    }
    Ok(i32::from(!failures.is_empty()))
}

fn status(
    config_path: Option<&Path>,
    arguments: &[String],
    output: &mut dyn Write,
) -> Result<i32, CliError> {
    let json_output = parse_no_value_flags(arguments, &["--json"])?.contains(&"--json");
    let config = load_config(config_path)?;
    let state_path = config.state_directory.join("state.sqlite3");
    let payload = json!({
        "credentials": {"registry": file_state(&config.credential_registry), "tokens": "not read"},
        "totp": file_state(&config.totp_secret_path),
        "owner": {"paired": "unavailable: runtime state protocol required"},
        "channel_binding": {"ready": "unavailable: runtime state protocol required"},
        "database": {"path": state_path, "state": file_state(&state_path), "bytes": file_size(&state_path)},
        "app_server": {"socket": file_state(&config.codex_socket), "protocol": "not probed"},
        "network": "not accessed",
    });
    if json_output {
        write_json(output, payload)?;
    } else {
        writeln!(
            output,
            "credential registry: {} (token contents not read)",
            file_state(&config.credential_registry)
        )?;
        writeln!(
            output,
            "TOTP secret: {}",
            file_state(&config.totp_secret_path)
        )?;
        writeln!(
            output,
            "owner: unavailable (runtime state protocol required)"
        )?;
        writeln!(
            output,
            "channel binding: unavailable (runtime state protocol required)"
        )?;
        writeln!(
            output,
            "database: {} bytes={}",
            file_state(&state_path),
            file_size(&state_path)
        )?;
        writeln!(
            output,
            "Codex socket: {} (protocol not probed)",
            file_state(&config.codex_socket)
        )?;
    }
    Ok(0)
}

fn watchdog(
    config_path: Option<&Path>,
    arguments: &[String],
    output: &mut dyn Write,
) -> Result<i32, CliError> {
    let recover = parse_no_value_flags(arguments, &["--recover"])?.contains(&"--recover");
    let config = load_config(config_path)?;
    let socket = file_state(&config.codex_socket);
    if socket == "private" {
        writeln!(
            output,
            "app-server watchdog: socket present/private; protocol not probed"
        )?;
        return Ok(0);
    }
    if recover {
        return Err(CliError::Unsupported("app-server-watchdog --recover is unavailable: automatic recovery is intentionally not implemented in the offline Rust CLI".into()));
    }
    writeln!(
        output,
        "app-server watchdog: socket {socket}; no recovery attempted"
    )?;
    Ok(1)
}

fn lock(
    config_path: Option<&Path>,
    arguments: &[String],
    output: &mut dyn Write,
) -> Result<i32, CliError> {
    ensure_empty(arguments, "usage: lock")?;
    let config = load_config(config_path)?;
    fs::create_dir_all(&config.state_directory)
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    let store = Arc::new(
        SqliteStore::open(config.state_directory.join("state.sqlite3"))
            .map_err(|error| CliError::Runtime(error.to_string()))?,
    );
    TotpManager::new(store, &config.totp_secret_path, config.totp_unlock_seconds)
        .lock()
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    writeln!(output, "Telegram write access is locked.")?;
    Ok(0)
}

fn owner_reset(
    config_path: Option<&Path>,
    arguments: &[String],
    output: &mut dyn Write,
) -> Result<i32, CliError> {
    if !arguments.is_empty() && arguments != ["--yes"] {
        return Err(CliError::Usage("usage: owner-reset [--yes]".into()));
    }
    if !arguments.iter().any(|argument| argument == "--yes") {
        return Err(CliError::Usage(
            "owner-reset requires --yes to remove the Rust owner binding".into(),
        ));
    }
    let config = load_config(config_path)?;
    let store = SqliteStore::open(config.state_directory.join("state.sqlite3"))
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    store
        .delete_workflow_record("onboarding", "owner")
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    store
        .delete_workflow_record("onboarding", "binding")
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    store
        .lock_totp()
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    writeln!(
        output,
        "owner-reset: owner and Telegram binding removed; Rust writes locked"
    )?;
    Ok(0)
}

fn parse_no_value_flags<'a>(
    arguments: &'a [String],
    accepted: &[&'a str],
) -> Result<Vec<&'a str>, CliError> {
    let mut flags = Vec::new();
    for argument in arguments {
        let flag = argument.as_str();
        if !accepted.contains(&flag) {
            return Err(CliError::Usage(format!("unexpected argument: {argument}")));
        }
        if flags.contains(&flag) {
            return Err(CliError::Usage(format!("duplicate flag: {argument}")));
        }
        flags.push(flag);
    }
    Ok(flags)
}

fn ensure_empty(arguments: &[String], usage: &str) -> Result<(), CliError> {
    if arguments.is_empty() {
        Ok(())
    } else {
        Err(CliError::Usage(usage.into()))
    }
}

fn ensure_replay_grammar(arguments: &[String]) -> Result<(), CliError> {
    let value_flags = [
        "--fixture",
        "--scenario",
        "--implementation",
        "--repetitions",
        "--warmup",
        "--output",
    ];
    let mut index = 0;
    while index < arguments.len() {
        let argument = arguments[index].as_str();
        if argument == "--json" {
            index += 1;
            continue;
        }
        if value_flags.contains(&argument) {
            let value = arguments
                .get(index + 1)
                .ok_or_else(|| CliError::Usage(format!("{argument} requires a value")))?;
            if value.starts_with('-') {
                return Err(CliError::Usage(format!("{argument} requires a value")));
            }
            index += 2;
            continue;
        }
        return Err(CliError::Usage(format!(
            "unexpected replay argument: {argument}"
        )));
    }
    Ok(())
}

fn value_after(arguments: &[String], name: &str) -> Option<String> {
    arguments
        .iter()
        .position(|argument| argument == name)
        .and_then(|index| arguments.get(index + 1))
        .cloned()
}

fn parse_u64_flag(arguments: &[String], name: &str, default: u64) -> Result<u64, CliError> {
    match value_after(arguments, name) {
        Some(value) => value
            .parse::<u64>()
            .map_err(|_| CliError::Usage(format!("{name} requires an unsigned integer"))),
        None => Ok(default),
    }
}

fn file_state(path: &Path) -> &'static str {
    if !path.exists() {
        "missing"
    } else if is_private_regular_file(path) {
        "private"
    } else {
        "insecure"
    }
}

fn directory_state(path: &Path) -> &'static str {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.is_dir() => "available",
        Ok(_) => "unsafe",
        Err(_) => "missing",
    }
}

fn file_size(path: &Path) -> u64 {
    fs::metadata(path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn write_json(output: &mut dyn Write, value: serde_json::Value) -> Result<(), CliError> {
    serde_json::to_writer(&mut *output, &value)
        .map_err(|error| CliError::Runtime(error.to_string()))?;
    writeln!(output).map_err(|error| CliError::Runtime(error.to_string()))
}

fn print_help(output: &mut dyn Write) -> Result<(), CliError> {
    writeln!(
        output,
        "commands: validate [--json], generate-config [PATH], probe, metrics, daemon, migration-inspect SQLITE_PATH, import-python-state --source PATH --target PATH --report PATH [--dry-run], replay --fixture PATH [--scenario NAME] [--implementation NAME] [--repetitions N] [--warmup N] [--output PATH], doctor [--offline] [--json], status [--json], app-server-watchdog [--recover], pair-code, bind-code, onboard [--timeout SECONDS], lock"
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_json_is_offline_and_secret_free() {
        let root = std::env::temp_dir().join(format!("ctg-cli-status-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let config = root.join("rust-vnext.toml");
        fs::write(&config, format!("credential_registry = \"{}\"\nstate_directory = \"{}\"\ntotp_secret_path = \"{}\"\ncodex_socket = \"{}\"\n", root.join("registry").display(), root.join("state").display(), root.join("totp").display(), root.join("socket").display())).unwrap();
        let mut output = Vec::new();
        assert_eq!(
            run(
                vec![
                    "--config".into(),
                    config.display().to_string(),
                    "status".into(),
                    "--json".into()
                ],
                &mut output
            )
            .unwrap(),
            0
        );
        let text = String::from_utf8(output).unwrap();
        assert!(text.contains("\"network\":\"not accessed\""));
        assert!(text.contains("\"tokens\":\"not read\""));
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pairing_code_is_persisted_and_recovery_stays_explicit() {
        let root = std::env::temp_dir().join(format!("ctg-cli-pair-{}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let config = root.join("rust-vnext.toml");
        fs::write(
            &config,
            format!(
                "credential_registry = \"{}\"\nstate_directory = \"{}\"\ntotp_secret_path = \"{}\"\ncodex_socket = \"{}\"\n",
                root.join("registry").display(),
                root.join("state").display(),
                root.join("totp").display(),
                root.join("socket").display()
            ),
        )
        .unwrap();
        let mut output = Vec::new();
        assert_eq!(
            run(
                vec![
                    "--config".into(),
                    config.display().to_string(),
                    "pair-code".into()
                ],
                &mut output
            )
            .unwrap(),
            0
        );
        assert!(
            String::from_utf8(output.clone())
                .unwrap()
                .contains("pair-code: ")
        );
        let recover = run(
            vec!["app-server-watchdog".into(), "--recover".into()],
            &mut output,
        )
        .unwrap_err();
        assert_eq!(recover.exit_code(), 2);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn invalid_grammar_is_a_usage_error() {
        let mut output = Vec::new();
        let error = run(vec!["status".into(), "--offline".into()], &mut output).unwrap_err();
        assert_eq!(error.exit_code(), 2);
    }
}
