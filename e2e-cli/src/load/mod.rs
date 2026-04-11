//! Traffic generator for quickstart demo.
//!
//! Generates INSERT / UPDATE / DELETE operations against the source PG
//! to produce a realistic CDC workload during the dashboard demo.

pub mod generator;

#[allow(unused_imports)]
pub use generator::{GeneratorStats, TrafficGenerator};
