//! Workload classification.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A workload pattern
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadPattern {
    pub name: String,
    pub characteristics: WorkloadCharacteristics,
    pub recommended_settings: HashMap<String, f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkloadCharacteristics {
    /// Sequential vs random (0 = random, 1 = sequential)
    pub sequential_ratio: f64,
    /// Read vs write (0 = write-heavy, 1 = read-heavy)
    pub read_ratio: f64,
    /// Small vs large I/O (average I/O size)
    pub avg_io_size_kb: f64,
    /// Concurrency level
    pub concurrency: f64,
    /// Temporal pattern
    pub temporal_pattern: TemporalPattern,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TemporalPattern {
    Constant,
    Bursty,
    Periodic,
    Growing,
    Declining,
}

/// Classifies workload patterns
pub struct WorkloadClassifier {
    patterns: Vec<WorkloadPattern>,
}

impl WorkloadClassifier {
    pub fn new() -> Self {
        let patterns = vec![
            WorkloadPattern {
                name: "streaming".to_string(),
                characteristics: WorkloadCharacteristics {
                    sequential_ratio: 0.9,
                    read_ratio: 0.8,
                    avg_io_size_kb: 1024.0,
                    concurrency: 0.3,
                    temporal_pattern: TemporalPattern::Constant,
                },
                recommended_settings: HashMap::from([
                    ("prefetch_size_kb".to_string(), 4096.0),
                    ("cache_size_mb".to_string(), 2048.0),
                ]),
            },
            WorkloadPattern {
                name: "database".to_string(),
                characteristics: WorkloadCharacteristics {
                    sequential_ratio: 0.3,
                    read_ratio: 0.5,
                    avg_io_size_kb: 16.0,
                    concurrency: 0.8,
                    temporal_pattern: TemporalPattern::Bursty,
                },
                recommended_settings: HashMap::from([
                    ("write_buffer_size_mb".to_string(), 512.0),
                    ("cache_size_mb".to_string(), 4096.0),
                ]),
            },
            WorkloadPattern {
                name: "backup".to_string(),
                characteristics: WorkloadCharacteristics {
                    sequential_ratio: 0.95,
                    read_ratio: 0.1,
                    avg_io_size_kb: 4096.0,
                    concurrency: 0.2,
                    temporal_pattern: TemporalPattern::Periodic,
                },
                recommended_settings: HashMap::from([
                    ("write_buffer_size_mb".to_string(), 1024.0),
                    ("compaction_threads".to_string(), 8.0),
                ]),
            },
        ];

        Self { patterns }
    }

    /// Classify workload based on observed characteristics
    pub fn classify(&self, observed: &WorkloadCharacteristics) -> Option<&WorkloadPattern> {
        let mut best_match: Option<(&WorkloadPattern, f64)> = None;

        for pattern in &self.patterns {
            let distance = self.calculate_distance(&pattern.characteristics, observed);

            if let Some((_, best_dist)) = best_match {
                if distance < best_dist {
                    best_match = Some((pattern, distance));
                }
            } else {
                best_match = Some((pattern, distance));
            }
        }

        best_match.filter(|(_, dist)| *dist < 1.0).map(|(p, _)| p)
    }

    fn calculate_distance(
        &self,
        pattern: &WorkloadCharacteristics,
        observed: &WorkloadCharacteristics,
    ) -> f64 {
        let seq_diff = (pattern.sequential_ratio - observed.sequential_ratio).abs();
        let read_diff = (pattern.read_ratio - observed.read_ratio).abs();
        let size_diff =
            ((pattern.avg_io_size_kb.ln() - observed.avg_io_size_kb.ln()) / 10.0).abs();
        let conc_diff = (pattern.concurrency - observed.concurrency).abs();

        (seq_diff.powi(2) + read_diff.powi(2) + size_diff.powi(2) + conc_diff.powi(2)).sqrt()
    }
}

impl Default for WorkloadClassifier {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workload_classifier() {
        let classifier = WorkloadClassifier::new();

        let streaming = WorkloadCharacteristics {
            sequential_ratio: 0.95,
            read_ratio: 0.85,
            avg_io_size_kb: 1024.0,
            concurrency: 0.25,
            temporal_pattern: TemporalPattern::Constant,
        };

        let result = classifier.classify(&streaming);
        assert!(result.is_some());
        assert_eq!(result.unwrap().name, "streaming");
    }
}
