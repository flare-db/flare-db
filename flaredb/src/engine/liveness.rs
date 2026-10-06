//! Harness-liveness supervision.
//!
//! [`idle_guard`] waits for a bundle to finish, but fails if the SDK worker
//! stops sending traffic. The Fn transport receive loops, the data, control,
//! and state channels call [`touch`] on every message the worker sends, which
//! writes to a process-global clock ([`Liveness`]). [`idle_guard`] returns the
//! bundle's own result as soon as it is ready; otherwise it errors once the
//! clock has seen no [`touch`] for the full timeout window.
//!
//! Measuring inactivity instead of elapsed time is what lets a stalled worker
//! be told apart from a large bundle that is still progressing.

use std::future::Future;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Monotonic milliseconds since the first call in this process.
fn now_ms() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// A per-purpose liveness clock. The production engine shares one process-global
/// instance via [`global`]; tests construct their own.
#[derive(Clone, Default)]
pub struct Liveness {
    last_activity_ms: std::sync::Arc<AtomicU64>,
}

impl Liveness {
    /// A clock initialized to "now" (so the first idle window starts clean).
    pub fn new() -> Self {
        Self {
            last_activity_ms: std::sync::Arc::new(AtomicU64::new(now_ms())),
        }
    }

    /// Record that the worker just produced activity.
    pub fn touch(&self) {
        self.last_activity_ms.store(now_ms(), Ordering::SeqCst);
    }

    /// Resolve once the worker has been idle (no [`Liveness::touch`]) for at
    /// least `max_idle`.
    pub async fn wait_idle(&self, max_idle: Duration) {
        let max = max_idle.as_millis() as u64;
        loop {
            let idle = now_ms().saturating_sub(self.last_activity_ms.load(Ordering::SeqCst));
            if idle >= max {
                return;
            }
            // Coarse poll: correctness only needs to eventually observe idle.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }
}

/// The process-global liveness clock shared by the transport and the bundle
/// supervisors.
pub fn global() -> &'static Liveness {
    static GLOBAL: OnceLock<Liveness> = OnceLock::new();
    GLOBAL.get_or_init(Liveness::new)
}

/// Record harness activity on the global clock. Called by the transport receive
/// loops whenever the worker sends data, a control response, or a state request.
pub fn touch() {
    global().touch();
}

/// Await `future`, failing with `what` if the worker is idle for `max_idle`.
///
/// Unlike a wall-clock deadline, progress on the data/control/state streams
/// keeps resetting the window, so a large-but-progressing bundle runs to
/// completion while a genuinely hung worker is still caught.
pub async fn idle_guard<F, T>(future: F, max_idle: Duration, what: &str) -> anyhow::Result<T>
where
    F: Future<Output = anyhow::Result<T>>,
{
    tokio::pin!(future);
    tokio::select! {
        result = &mut future => result,
        _ = global().wait_idle(max_idle) => Err(anyhow::anyhow!(
            "worker idle for {}s (no data/control/state activity): {what}",
            max_idle.as_secs()
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn wait_idle_resolves_after_an_idle_period() {
        let liveness = Liveness::new();
        let started = Instant::now();
        liveness.wait_idle(Duration::from_millis(50)).await;
        assert!(started.elapsed() >= Duration::from_millis(40));
    }

    #[tokio::test]
    async fn touch_resets_the_idle_window() {
        let liveness = Liveness::new();
        let started = Instant::now();
        // Touch every 10ms for ~80ms, then require 60ms of *continuous* idle.
        let mut touched = false;
        for _ in 0..8 {
            liveness.touch();
            touched = true;
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(touched);
        liveness.wait_idle(Duration::from_millis(60)).await;
        // 80ms of activity + 60ms idle ~ 140ms; a fixed 60ms deadline would have
        // "timed out" during the active phase, so this just proves the window is
        // measured from the last touch, not from construction.
        assert!(started.elapsed() >= Duration::from_millis(130));
    }

    #[tokio::test]
    async fn idle_guard_fires_only_after_no_activity() {
        // A future that never resolves: idle_guard must fail once the idle window
        // elapses (no touch happens here).
        let result = idle_guard(
            std::future::pending::<anyhow::Result<()>>(),
            Duration::from_millis(50),
            "test",
        )
        .await;
        assert!(result.is_err());
    }
}
