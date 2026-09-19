//! A small latency-summary helper - just enough for this benchmark's own reporting, not a
//! general-purpose statistics library.

use std::time::Duration;

use serde::Serialize;

/// A batch of latency samples, reduced to the numbers this benchmark reports: the minimum, a few
/// percentiles, the mean and the maximum, all in milliseconds.
#[derive(Serialize, Debug, Clone, Copy)]
pub struct Summary {
    pub count: usize,
    pub min_ms: f64,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
    pub mean_ms: f64,
}

impl Summary {
    /// Summarize `samples` - sorted internally, so callers can hand it whatever order they
    /// collected results in.
    ///
    /// # Panics
    ///
    /// Panics if `samples` is empty: every call site here only summarizes a batch it just ran, so
    /// an empty batch is a benchmark bug, not a result worth reporting as zeroes.
    pub fn of(mut samples: Vec<Duration>) -> Self {
        assert!(!samples.is_empty(), "cannot summarize zero latency samples");
        samples.sort_unstable();
        let count = samples.len();
        let to_ms = |d: Duration| d.as_secs_f64() * 1000.0;
        // Nearest-rank percentile: simple, and the batches here (tens to thousands of samples)
        // do not need interpolation's extra precision.
        let percentile = |q: f64| {
            let rank = (((count - 1) as f64) * q).round() as usize;
            to_ms(samples[rank.min(count - 1)])
        };
        let mean = samples.iter().map(|d| to_ms(*d)).sum::<f64>() / count as f64;

        Self {
            count,
            min_ms: to_ms(samples[0]),
            p50_ms: percentile(0.50),
            p90_ms: percentile(0.90),
            p99_ms: percentile(0.99),
            max_ms: to_ms(samples[count - 1]),
            mean_ms: mean,
        }
    }
}
