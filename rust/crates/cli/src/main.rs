use codex_telegram_adapter::{ReqwestTransport, TelegramBotApi};
use codex_telegram_cli::config::RustConfig;
use codex_telegram_cli::metrics::{MetricsRegistry, MetricsServer};
use codex_telegram_cli::migration::inspect_legacy_database;
use codex_telegram_cli::replay::run_fixture;
use std::env;
use std::path::PathBuf;
use std::time::Duration;

fn main() {
    if let Err(error) = run(env::args().skip(1).collect()) {
        eprintln!("codex-telegram-cli: {error}");
        std::process::exit(2);
    }
}

fn run(arguments: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {
    let command = arguments.first().map(String::as_str).unwrap_or("validate");
    match command {
        "validate" | "config-check" => validate(),
        "generate-config" => {
            let path = arguments
                .get(1)
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("rust-vnext.toml"));
            let config = RustConfig::default();
            config.save_template(&path)?;
            println!("wrote Rust vNext config template");
            Ok(())
        }
        "probe" => probe(),
        "metrics" => serve_metrics(),
        "migration-inspect" => migration_inspect(&arguments[1..]),
        "replay" => replay(&arguments[1..]),
        "help" | "--help" | "-h" => {
            println!(
                "commands: validate, generate-config [path], probe, metrics, migration-inspect DB, replay --fixture PATH [--scenario NAME] [--repetitions N] [--warmup N]"
            );
            Ok(())
        }
        other => Err(format!("unknown command: {other}").into()),
    }
}

fn validate() -> Result<(), Box<dyn std::error::Error>> {
    let config = RustConfig::load_default()?;
    let bots = config.enabled_bot_bindings()?;
    let surfaces = config.surface_bindings()?;
    let adapter_issues = codex_telegram_cli::validate_configuration(
        &codex_telegram_credentials::CredentialFiles::discover(&config.credential_registry),
        &bots,
        &surfaces,
    );
    for issue in adapter_issues {
        println!("{}: {}", issue.code, issue.message);
    }
    match config.credentials() {
        Ok(registry) => {
            for bot in config.bots.iter().filter(|bot| bot.enabled) {
                if config.token_for(bot, &registry).is_err() {
                    println!("missing-credential-key: {}", bot.instance_id);
                }
            }
        }
        Err(error) => println!("credential-registry: {error}"),
    }
    println!(
        "validated {} enabled bot binding(s) and {} surface binding(s)",
        bots.len(),
        surfaces.len()
    );
    Ok(())
}

fn probe() -> Result<(), Box<dyn std::error::Error>> {
    let config = RustConfig::load_default()?;
    let registry = config.credentials()?;
    let transport = ReqwestTransport::new(Duration::from_secs(config.request_timeout_seconds))?;
    let api = TelegramBotApi::with_api_base(transport, &config.api_base);
    let mut profiles = std::collections::BTreeMap::new();
    for bot in config.bots.iter().filter(|bot| bot.enabled) {
        let token = config.token_for(bot, &registry)?;
        let profile = api.get_me(&token)?;
        println!(
            "{}: bot_id={} username={}",
            bot.instance_id,
            profile.id,
            profile.username.as_deref().unwrap_or("<none>")
        );
        profiles.insert(bot.instance_id.as_str(), (token, profile));
    }
    for surface in &config.surfaces {
        let (token, profile) = profiles
            .get(surface.bot_instance_id.as_str())
            .ok_or_else(|| {
                format!(
                    "surface references disabled bot {}",
                    surface.bot_instance_id
                )
            })?;
        let chat = api.get_chat(token, surface.chat_id)?;
        let member = api.get_chat_member(token, surface.chat_id, profile.id)?;
        println!(
            "surface {}: chat_type={} forum={} bot_status={}",
            surface.bot_instance_id, chat.chat_type, chat.is_forum, member.status
        );
        if matches!(
            surface.kind,
            codex_telegram_cli::config::SurfaceKind::ForumTopic
        ) && !chat.is_forum
        {
            return Err(format!("surface {} is not a Forum chat", surface.bot_instance_id).into());
        }
    }
    Ok(())
}

fn serve_metrics() -> Result<(), Box<dyn std::error::Error>> {
    let config = RustConfig::load_default()?;
    let _server = MetricsServer::start(&config.metrics_bind, MetricsRegistry::default())?;
    println!("metrics listening on {}", config.metrics_bind);
    loop {
        std::thread::sleep(Duration::from_secs(60));
    }
}

fn replay(arguments: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let fixture = value_after(arguments, "--fixture").ok_or("replay requires --fixture PATH")?;
    let scenario = value_after(arguments, "--scenario").unwrap_or_else(|| "fixture".into());
    let implementation =
        value_after(arguments, "--implementation").unwrap_or_else(|| "rust-vnext".into());
    let repetitions = value_after(arguments, "--repetitions")
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(1);
    let warmup = value_after(arguments, "--warmup")
        .map(|value| value.parse::<u64>())
        .transpose()?
        .unwrap_or(0);
    let output = value_after(arguments, "--output");
    let report = run_fixture(fixture, scenario, implementation, repetitions, warmup)?;
    let json = serde_json::to_string_pretty(&report)?;
    if let Some(path) = output {
        std::fs::write(path, &json)?;
    } else {
        println!("{json}");
    }
    Ok(())
}

fn migration_inspect(arguments: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let source = arguments
        .first()
        .ok_or("migration-inspect requires a SQLite path")?;
    let report = inspect_legacy_database(source)?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn value_after(arguments: &[String], name: &str) -> Option<String> {
    arguments
        .iter()
        .position(|argument| argument == name)
        .and_then(|index| arguments.get(index + 1))
        .cloned()
}
