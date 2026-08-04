//! CLI-facing validation with no token rendering or process side effects.

use codex_telegram_adapter::{
    BindingIssue, BotInstanceBinding, TelegramSurfaceBinding, validate_bindings,
};
use codex_telegram_credentials::CredentialFiles;
pub mod alerts;
pub mod config;
pub mod control;
pub mod daemon;
pub mod metrics;
pub mod migration;
pub mod perf;
pub mod replay;
pub mod security;
pub mod status_contract;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CliValidationIssue {
    pub code: String,
    pub message: String,
}

impl CliValidationIssue {
    fn from_binding(issue: BindingIssue) -> Self {
        Self {
            code: issue.code.to_owned(),
            message: issue.message.to_owned(),
        }
    }
}

/// Validates configuration paths and structural bindings without opening or printing token values.
pub fn validate_configuration(
    credentials: &CredentialFiles,
    bots: &[BotInstanceBinding],
    surfaces: &[TelegramSurfaceBinding],
) -> Vec<CliValidationIssue> {
    let mut issues: Vec<_> = validate_bindings(bots)
        .into_iter()
        .map(CliValidationIssue::from_binding)
        .collect();

    let _ = credentials;

    for surface in surfaces {
        if !bots
            .iter()
            .any(|bot| bot.instance_id == surface.bot_instance_id())
        {
            issues.push(CliValidationIssue {
                code: "unknown-surface-bot".to_owned(),
                message: format!(
                    "surface references unknown bot instance {}",
                    surface.bot_instance_id()
                ),
            });
        }
    }
    issues
}

#[cfg(test)]
mod tests {
    use super::*;
    use codex_telegram_adapter::BotCapability;
    use std::fs;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static NEXT_DIRECTORY: AtomicUsize = AtomicUsize::new(0);

    #[test]
    fn validation_never_reads_token_contents() {
        let directory = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/test-tmp")
            .join(format!(
                "codex-telegram-cli-{}-{}",
                std::process::id(),
                NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(&directory).unwrap();
        fs::write(directory.join("control.token"), "leak-me-never").unwrap();
        let credentials = CredentialFiles::discover(&directory);
        let bots = vec![BotInstanceBinding::new("control", BotCapability::Control)];

        let rendered = format!("{:?}", validate_configuration(&credentials, &bots, &[]));
        assert!(!rendered.contains("leak-me-never"));
        fs::remove_dir_all(directory).unwrap();
    }
}
