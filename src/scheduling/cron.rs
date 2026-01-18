//! Cron expression parsing and evaluation.
//!
//! Supports standard 5-field cron expressions:
//! ```text
//! ┌───────────── minute (0-59)
//! │ ┌───────────── hour (0-23)
//! │ │ ┌───────────── day of month (1-31)
//! │ │ │ ┌───────────── month (1-12)
//! │ │ │ │ ┌───────────── day of week (0-6, 0 = Sunday)
//! │ │ │ │ │
//! * * * * *
//! ```

use chrono::{DateTime, Datelike, NaiveDateTime, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::str::FromStr;
use thiserror::Error;

/// Errors that can occur when parsing cron expressions.
#[derive(Debug, Error)]
pub enum CronParseError {
    #[error("Invalid cron expression: expected 5 fields, got {0}")]
    InvalidFieldCount(usize),
    #[error("Invalid field '{field}': {reason}")]
    InvalidField { field: String, reason: String },
    #[error("Value {value} is out of range [{min}, {max}] for {field}")]
    OutOfRange {
        field: String,
        value: u32,
        min: u32,
        max: u32,
    },
    #[error("Invalid range: {0}-{1}")]
    InvalidRange(u32, u32),
    #[error("Invalid step value: {0}")]
    InvalidStep(String),
}

/// A single field in a cron expression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CronField {
    /// The set of valid values for this field.
    pub values: BTreeSet<u32>,
    /// Minimum allowed value.
    pub min: u32,
    /// Maximum allowed value.
    pub max: u32,
}

impl CronField {
    /// Create a new cron field with valid range.
    fn new(min: u32, max: u32) -> Self {
        Self {
            values: BTreeSet::new(),
            min,
            max,
        }
    }

    /// Parse a field expression.
    fn parse(&mut self, expr: &str) -> Result<(), CronParseError> {
        // Handle comma-separated values
        for part in expr.split(',') {
            self.parse_part(part.trim())?;
        }
        Ok(())
    }

    /// Parse a single part of a field expression.
    fn parse_part(&mut self, part: &str) -> Result<(), CronParseError> {
        // Handle step values (e.g., */5, 0-30/5)
        let (range_part, step) = if let Some(idx) = part.find('/') {
            let step_str = &part[idx + 1..];
            let step = step_str.parse::<u32>().map_err(|_| {
                CronParseError::InvalidStep(step_str.to_string())
            })?;
            if step == 0 {
                return Err(CronParseError::InvalidStep("0".to_string()));
            }
            (&part[..idx], Some(step))
        } else {
            (part, None)
        };

        // Handle different patterns
        let (start, end) = if range_part == "*" {
            (self.min, self.max)
        } else if let Some(idx) = range_part.find('-') {
            let start = range_part[..idx].parse::<u32>().map_err(|_| {
                CronParseError::InvalidField {
                    field: range_part.to_string(),
                    reason: "invalid start of range".to_string(),
                }
            })?;
            let end = range_part[idx + 1..].parse::<u32>().map_err(|_| {
                CronParseError::InvalidField {
                    field: range_part.to_string(),
                    reason: "invalid end of range".to_string(),
                }
            })?;
            if start > end {
                return Err(CronParseError::InvalidRange(start, end));
            }
            (start, end)
        } else {
            // Single value
            let value = range_part.parse::<u32>().map_err(|_| {
                CronParseError::InvalidField {
                    field: range_part.to_string(),
                    reason: "invalid value".to_string(),
                }
            })?;
            (value, value)
        };

        // Validate range
        if start < self.min || end > self.max {
            return Err(CronParseError::OutOfRange {
                field: range_part.to_string(),
                value: if start < self.min { start } else { end },
                min: self.min,
                max: self.max,
            });
        }

        // Add values with step
        let step = step.unwrap_or(1);
        let mut value = start;
        while value <= end {
            self.values.insert(value);
            value += step;
        }

        Ok(())
    }

    /// Check if a value matches this field.
    pub fn matches(&self, value: u32) -> bool {
        self.values.contains(&value)
    }

    /// Get the next matching value >= given value.
    pub fn next(&self, value: u32) -> Option<u32> {
        self.values.range(value..).next().copied()
    }

    /// Get the first matching value.
    pub fn first(&self) -> Option<u32> {
        self.values.iter().next().copied()
    }
}

/// A parsed cron expression.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronExpr {
    /// Original expression string.
    pub expr: String,
    /// Minute field (0-59).
    pub minute: CronField,
    /// Hour field (0-23).
    pub hour: CronField,
    /// Day of month field (1-31).
    pub day_of_month: CronField,
    /// Month field (1-12).
    pub month: CronField,
    /// Day of week field (0-6, 0 = Sunday).
    pub day_of_week: CronField,
}

impl CronExpr {
    /// Parse a cron expression string.
    pub fn parse(expr: &str) -> Result<Self, CronParseError> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(CronParseError::InvalidFieldCount(parts.len()));
        }

        let mut minute = CronField::new(0, 59);
        let mut hour = CronField::new(0, 23);
        let mut day_of_month = CronField::new(1, 31);
        let mut month = CronField::new(1, 12);
        let mut day_of_week = CronField::new(0, 6);

        minute.parse(parts[0])?;
        hour.parse(parts[1])?;
        day_of_month.parse(parts[2])?;
        month.parse(parts[3])?;
        day_of_week.parse(parts[4])?;

        Ok(Self {
            expr: expr.to_string(),
            minute,
            hour,
            day_of_month,
            month,
            day_of_week,
        })
    }

    /// Check if a datetime matches this cron expression.
    pub fn matches(&self, dt: &DateTime<Utc>) -> bool {
        self.minute.matches(dt.minute())
            && self.hour.matches(dt.hour())
            && self.day_of_month.matches(dt.day())
            && self.month.matches(dt.month())
            && self.day_of_week.matches(dt.weekday().num_days_from_sunday())
    }

    /// Calculate the next run time after the given datetime.
    pub fn next_run(&self, after: &DateTime<Utc>) -> Option<DateTime<Utc>> {
        // Start from the next minute
        let mut current = *after + chrono::Duration::minutes(1);
        // Zero out seconds
        current = current
            .with_second(0)
            .and_then(|dt| dt.with_nanosecond(0))
            .unwrap_or(current);

        // Search for up to 4 years (to handle leap years and edge cases)
        let max_iterations = 4 * 366 * 24 * 60;

        for _ in 0..max_iterations {
            if self.matches(&current) {
                return Some(current);
            }

            // Try to skip ahead intelligently
            if !self.month.matches(current.month()) {
                // Skip to next matching month
                if let Some(next_month) = self.month.next(current.month()) {
                    let year = current.year();
                    if let Some(dt) = NaiveDateTime::new(
                        chrono::NaiveDate::from_ymd_opt(year, next_month, 1)?,
                        chrono::NaiveTime::from_hms_opt(0, 0, 0)?,
                    )
                    .and_utc()
                    .into()
                    {
                        current = dt;
                        continue;
                    }
                } else {
                    // Wrap to next year
                    if let Some(first_month) = self.month.first() {
                        let year = current.year() + 1;
                        if let Some(dt) = NaiveDateTime::new(
                            chrono::NaiveDate::from_ymd_opt(year, first_month, 1)?,
                            chrono::NaiveTime::from_hms_opt(0, 0, 0)?,
                        )
                        .and_utc()
                        .into()
                        {
                            current = dt;
                            continue;
                        }
                    }
                }
            }

            // Move to next minute
            current = current + chrono::Duration::minutes(1);
        }

        None
    }

    /// Check if this expression represents a simple interval.
    pub fn is_simple_interval(&self) -> bool {
        // Check if only minute varies and others are wildcards
        self.hour.values.len() == 24
            && self.day_of_month.values.len() == 31
            && self.month.values.len() == 12
            && self.day_of_week.values.len() == 7
    }
}

impl FromStr for CronExpr {
    type Err = CronParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        CronExpr::parse(s)
    }
}

/// Common cron presets.
pub mod presets {
    use super::CronExpr;

    /// Every minute.
    pub fn every_minute() -> CronExpr {
        CronExpr::parse("* * * * *").unwrap()
    }

    /// Every 5 minutes.
    pub fn every_5_minutes() -> CronExpr {
        CronExpr::parse("*/5 * * * *").unwrap()
    }

    /// Every 15 minutes.
    pub fn every_15_minutes() -> CronExpr {
        CronExpr::parse("*/15 * * * *").unwrap()
    }

    /// Every hour at minute 0.
    pub fn hourly() -> CronExpr {
        CronExpr::parse("0 * * * *").unwrap()
    }

    /// Every day at midnight.
    pub fn daily() -> CronExpr {
        CronExpr::parse("0 0 * * *").unwrap()
    }

    /// Every day at 3 AM.
    pub fn daily_3am() -> CronExpr {
        CronExpr::parse("0 3 * * *").unwrap()
    }

    /// Every Sunday at midnight.
    pub fn weekly() -> CronExpr {
        CronExpr::parse("0 0 * * 0").unwrap()
    }

    /// First day of every month at midnight.
    pub fn monthly() -> CronExpr {
        CronExpr::parse("0 0 1 * *").unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn test_parse_wildcard() {
        let expr = CronExpr::parse("* * * * *").unwrap();
        assert_eq!(expr.minute.values.len(), 60);
        assert_eq!(expr.hour.values.len(), 24);
    }

    #[test]
    fn test_parse_single_value() {
        let expr = CronExpr::parse("30 4 * * *").unwrap();
        assert_eq!(expr.minute.values.len(), 1);
        assert!(expr.minute.matches(30));
        assert!(expr.hour.matches(4));
    }

    #[test]
    fn test_parse_range() {
        let expr = CronExpr::parse("0-30 * * * *").unwrap();
        assert_eq!(expr.minute.values.len(), 31);
        assert!(expr.minute.matches(0));
        assert!(expr.minute.matches(30));
        assert!(!expr.minute.matches(31));
    }

    #[test]
    fn test_parse_step() {
        let expr = CronExpr::parse("*/15 * * * *").unwrap();
        assert_eq!(expr.minute.values.len(), 4);
        assert!(expr.minute.matches(0));
        assert!(expr.minute.matches(15));
        assert!(expr.minute.matches(30));
        assert!(expr.minute.matches(45));
    }

    #[test]
    fn test_parse_list() {
        let expr = CronExpr::parse("0,15,30,45 * * * *").unwrap();
        assert_eq!(expr.minute.values.len(), 4);
    }

    #[test]
    fn test_parse_complex() {
        let expr = CronExpr::parse("0 9-17 * * 1-5").unwrap();
        assert!(expr.minute.matches(0));
        assert!(expr.hour.matches(9));
        assert!(expr.hour.matches(17));
        assert!(!expr.hour.matches(8));
        assert!(expr.day_of_week.matches(1)); // Monday
        assert!(!expr.day_of_week.matches(0)); // Sunday
    }

    #[test]
    fn test_matches() {
        let expr = CronExpr::parse("30 4 * * *").unwrap();
        let dt = Utc.with_ymd_and_hms(2024, 1, 15, 4, 30, 0).unwrap();
        assert!(expr.matches(&dt));

        let dt2 = Utc.with_ymd_and_hms(2024, 1, 15, 4, 31, 0).unwrap();
        assert!(!expr.matches(&dt2));
    }

    #[test]
    fn test_next_run() {
        let expr = CronExpr::parse("0 * * * *").unwrap();
        let now = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 0).unwrap();
        let next = expr.next_run(&now).unwrap();

        assert_eq!(next.minute(), 0);
        assert_eq!(next.hour(), 15);
    }

    #[test]
    fn test_next_run_daily() {
        let expr = CronExpr::parse("0 3 * * *").unwrap();
        let now = Utc.with_ymd_and_hms(2024, 1, 15, 14, 30, 0).unwrap();
        let next = expr.next_run(&now).unwrap();

        assert_eq!(next.hour(), 3);
        assert_eq!(next.minute(), 0);
        assert_eq!(next.day(), 16); // Next day
    }

    #[test]
    fn test_presets() {
        let daily = presets::daily();
        assert!(daily.minute.matches(0));
        assert!(daily.hour.matches(0));

        let hourly = presets::hourly();
        assert!(hourly.minute.matches(0));
        assert_eq!(hourly.hour.values.len(), 24);
    }

    #[test]
    fn test_invalid_expressions() {
        assert!(CronExpr::parse("* *").is_err());
        assert!(CronExpr::parse("60 * * * *").is_err());
        assert!(CronExpr::parse("* 25 * * *").is_err());
    }
}
