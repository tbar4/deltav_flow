//! Convenience helpers for creating `CleanupFn` values passed to
//! `Pipeline::on_shutdown`.
//!
//! These helpers make it easy to convert async cleanup logic or blocking
//! synchronous logic into the boxed `CleanupFn` type used by the `Pipeline`.

use std::future::Future;
use std::sync::Arc;
use tokio::task;
use tokio::io::Error as TokioIoError;
use deltav_utils::DeltavFlowResult;

/// Wrap an async function into a `CleanupFn`.
///
/// Example:
///
/// ```no_run
/// use deltav_core::pipeline::helpers::async_cleanup;
/// let f = async_cleanup(|| async { Ok(()) });
/// ```
pub fn async_cleanup<F, Fut>(f: F) -> super::CleanupFn
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = DeltavFlowResult<()>> + Send + 'static,
{
    Box::new(move || Box::pin(f()))
}

/// Wrap a blocking (synchronous) cleanup function so it runs on a blocking thread.
///
/// Useful for performing synchronous file flushes or other non-async cleanup
/// work without blocking the async runtime.
pub fn blocking_cleanup<F>(f: F) -> super::CleanupFn
where
    F: Fn() -> DeltavFlowResult<()> + Send + Sync + 'static,
{
    let f = Arc::new(f);
    Box::new(move || {
        let f = f.clone();
        Box::pin(async move {
            match task::spawn_blocking(move || (f)()).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(e)) => Err(e),
                Err(join_err) => Err(deltav_utils::error::Error::TokioError(
                    TokioIoError::new(std::io::ErrorKind::Other, join_err.to_string()),
                )),
            }
        })
    })
}
