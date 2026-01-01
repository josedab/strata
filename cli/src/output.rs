//! Output formatting for CLI

use console::{style, Style};
use serde::Serialize;
use std::fmt::Display;
use tabled::{settings::Style as TableStyle, Table, Tabled};

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
    Yaml,
    Csv,
}

impl Default for OutputFormat {
    fn default() -> Self {
        Self::Table
    }
}

/// Format and print data in the specified format
pub fn print_output<T: Serialize + Tabled>(data: &[T], format: OutputFormat) {
    match format {
        OutputFormat::Table => {
            if data.is_empty() {
                println!("{}", style("No results found").dim());
            } else {
                let table = Table::new(data).with(TableStyle::rounded()).to_string();
                println!("{}", table);
            }
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(data).unwrap());
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(data).unwrap());
        }
        OutputFormat::Csv => {
            if let Some(first) = data.first() {
                // Print CSV header and data
                let json = serde_json::to_value(first).unwrap();
                if let serde_json::Value::Object(map) = json {
                    let headers: Vec<_> = map.keys().collect();
                    println!("{}", headers.iter().map(|s| s.as_str()).collect::<Vec<_>>().join(","));
                }
            }
            for item in data {
                let json = serde_json::to_value(item).unwrap();
                if let serde_json::Value::Object(map) = json {
                    let values: Vec<_> = map.values()
                        .map(|v| match v {
                            serde_json::Value::String(s) => s.clone(),
                            other => other.to_string(),
                        })
                        .collect();
                    println!("{}", values.join(","));
                }
            }
        }
    }
}

/// Print a single item
pub fn print_single<T: Serialize>(data: &T, format: OutputFormat) {
    match format {
        OutputFormat::Table | OutputFormat::Csv => {
            println!("{}", serde_json::to_string_pretty(data).unwrap());
        }
        OutputFormat::Json => {
            println!("{}", serde_json::to_string_pretty(data).unwrap());
        }
        OutputFormat::Yaml => {
            println!("{}", serde_yaml::to_string(data).unwrap());
        }
    }
}

/// Success message
pub fn success(msg: impl Display) {
    println!("{} {}", style("✓").green().bold(), msg);
}

/// Warning message
pub fn warning(msg: impl Display) {
    println!("{} {}", style("⚠").yellow().bold(), msg);
}

/// Error message
pub fn error(msg: impl Display) {
    eprintln!("{} {}", style("✗").red().bold(), msg);
}

/// Info message
pub fn info(msg: impl Display) {
    println!("{} {}", style("ℹ").blue().bold(), msg);
}

/// Print a key-value pair
pub fn print_kv(key: &str, value: impl Display) {
    let key_style = Style::new().cyan().bold();
    println!("{}: {}", key_style.apply_to(key), value);
}

/// Print a section header
pub fn print_header(title: &str) {
    println!();
    println!("{}", style(title).bold().underlined());
    println!();
}

/// Format bytes as human-readable size
pub fn format_bytes(bytes: u64) -> String {
    humansize::format_size(bytes, humansize::BINARY)
}

/// Format duration as human-readable string
pub fn format_duration(seconds: u64) -> String {
    let days = seconds / 86400;
    let hours = (seconds % 86400) / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    if days > 0 {
        format!("{}d {}h {}m", days, hours, minutes)
    } else if hours > 0 {
        format!("{}h {}m {}s", hours, minutes, secs)
    } else if minutes > 0 {
        format!("{}m {}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}

/// Format percentage
pub fn format_percent(value: f64) -> String {
    format!("{:.1}%", value)
}

/// Status indicator with color
pub fn status_indicator(status: &str) -> String {
    match status.to_lowercase().as_str() {
        "healthy" | "online" | "active" | "running" => {
            format!("{}", style(status).green())
        }
        "degraded" | "warning" | "draining" => {
            format!("{}", style(status).yellow())
        }
        "unhealthy" | "offline" | "error" | "failed" => {
            format!("{}", style(status).red())
        }
        _ => status.to_string(),
    }
}

/// Progress bar configuration
pub struct ProgressConfig {
    pub message: String,
    pub total: u64,
}

impl ProgressConfig {
    pub fn new(message: impl Into<String>, total: u64) -> Self {
        Self {
            message: message.into(),
            total,
        }
    }

    pub fn create_bar(&self) -> indicatif::ProgressBar {
        let bar = indicatif::ProgressBar::new(self.total);
        bar.set_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")
                .unwrap()
                .progress_chars("#>-"),
        );
        bar.set_message(self.message.clone());
        bar
    }
}

/// Spinner for indeterminate progress
pub fn create_spinner(message: impl Into<String>) -> indicatif::ProgressBar {
    let spinner = indicatif::ProgressBar::new_spinner();
    spinner.set_style(
        indicatif::ProgressStyle::default_spinner()
            .template("{spinner:.green} {msg}")
            .unwrap(),
    );
    spinner.set_message(message.into());
    spinner.enable_steady_tick(std::time::Duration::from_millis(100));
    spinner
}

/// Interactive confirmation prompt
pub fn confirm(prompt: &str) -> bool {
    dialoguer::Confirm::new()
        .with_prompt(prompt)
        .default(false)
        .interact()
        .unwrap_or(false)
}

/// Interactive text input
pub fn input(prompt: &str) -> Option<String> {
    dialoguer::Input::<String>::new()
        .with_prompt(prompt)
        .interact_text()
        .ok()
}

/// Interactive selection
pub fn select<T: ToString>(prompt: &str, items: &[T]) -> Option<usize> {
    dialoguer::Select::new()
        .with_prompt(prompt)
        .items(items)
        .interact()
        .ok()
}
