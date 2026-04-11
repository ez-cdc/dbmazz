//! Async polling helpers for the verify runner.
//!
//! These are generic "wait until X" utilities used by tier checks
//! to poll for state that propagates asynchronously (CDC latency,
//! normalizer settle time, etc.).

use std::time::{Duration, Instant};

use tokio::time::sleep;

/// Poll `predicate` until it returns `Ok(true)` or timeout expires.
///
/// Transient errors from the predicate are swallowed and treated as
/// "not yet". Only a timeout produces an error.
pub async fn wait_until<F, Fut>(
    mut predicate: F,
    timeout: Duration,
    poll_interval: Duration,
    description: &str,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<bool, anyhow::Error>>,
{
    let start = Instant::now();

    while start.elapsed() < timeout {
        match predicate().await {
            Ok(true) => return Ok(()),
            Ok(false) => {}
            Err(_) => {
                // Transient error — treat as "not yet".
            }
        }
        sleep(poll_interval).await;
    }

    Err(format!(
        "timed out after {:.1}s waiting for: {description}",
        start.elapsed().as_secs_f64()
    ))
}

/// Poll `producer` until it returns `Ok(expected)` or timeout expires.
///
/// Returns `Ok(())` on match or `Err` with the last observed value on timeout.
pub async fn wait_until_with_value<F, Fut, T>(
    mut producer: F,
    expected: &T,
    timeout: Duration,
    poll_interval: Duration,
    description: &str,
) -> Result<(), String>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, anyhow::Error>>,
    T: PartialEq + std::fmt::Debug,
{
    let start = Instant::now();
    let mut last_value: Option<T> = None;

    while start.elapsed() < timeout {
        match producer().await {
            Ok(val) => {
                if val == *expected {
                    return Ok(());
                }
                last_value = Some(val);
            }
            Err(_) => {
                // Transient error — retry.
            }
        }
        sleep(poll_interval).await;
    }

    let detail = match last_value {
        Some(v) => format!("last value: {v:?}"),
        None => "producer never returned a value".to_string(),
    };
    Err(format!(
        "timed out after {:.1}s waiting for {description}: expected {expected:?}, {detail}",
        start.elapsed().as_secs_f64()
    ))
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn wait_until_succeeds_immediately() {
        let result = wait_until(
            || async { Ok(true) },
            Duration::from_secs(1),
            Duration::from_millis(10),
            "always true",
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn wait_until_succeeds_after_retries() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = wait_until(
            move || {
                let c = counter_clone.clone();
                async move {
                    let val = c.fetch_add(1, Ordering::SeqCst);
                    Ok(val >= 3)
                }
            },
            Duration::from_secs(5),
            Duration::from_millis(10),
            "counter reaches 3",
        )
        .await;
        assert!(result.is_ok());
        assert!(counter.load(Ordering::SeqCst) >= 4);
    }

    #[tokio::test]
    async fn wait_until_times_out() {
        let result = wait_until(
            || async { Ok(false) },
            Duration::from_millis(100),
            Duration::from_millis(20),
            "never true",
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("timed out"));
    }

    #[tokio::test]
    async fn wait_until_swallows_transient_errors() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = wait_until(
            move || {
                let c = counter_clone.clone();
                async move {
                    let val = c.fetch_add(1, Ordering::SeqCst);
                    if val < 2 {
                        anyhow::bail!("transient error");
                    }
                    Ok(true)
                }
            },
            Duration::from_secs(5),
            Duration::from_millis(10),
            "succeeds after errors",
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn wait_until_with_value_succeeds() {
        let counter = Arc::new(AtomicU32::new(0));
        let counter_clone = counter.clone();

        let result = wait_until_with_value(
            move || {
                let c = counter_clone.clone();
                async move {
                    let val = c.fetch_add(1, Ordering::SeqCst);
                    Ok(val as i64)
                }
            },
            &3_i64,
            Duration::from_secs(5),
            Duration::from_millis(10),
            "counter value",
        )
        .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn wait_until_with_value_times_out() {
        let result = wait_until_with_value(
            || async { Ok(42_i64) },
            &99_i64,
            Duration::from_millis(100),
            Duration::from_millis(20),
            "never matches",
        )
        .await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("timed out"));
        assert!(err.contains("42"));
    }
}
