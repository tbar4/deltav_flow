/// Initialize the tracing subscriber for the `deltav_core` crate.
///
/// This convenience function reads `RUST_LOG` from the environment (default
/// level is `info`) and attempts to install a global tracing subscriber. The
/// function is safe to call multiple times (it quietly ignores initialization
/// errors produced by `try_init`).
pub fn init() {
    let env = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::new(env))
        .try_init();
}
