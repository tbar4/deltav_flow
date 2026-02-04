use async_trait::async_trait;
use deltav_utils::{DeltavFlowResult, DataStream};

pub mod http_client;
pub mod traits;

/// Trait implemented by pipeline data sources.
///
/// A `Source` produces an async stream of byte chunks that the pipeline will
/// consume and forward to the `Destination` during a run.
#[async_trait]
pub trait Source: Send {
    async fn extract(&self) -> DeltavFlowResult<DataStream>;
}