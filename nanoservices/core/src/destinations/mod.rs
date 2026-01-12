use async_trait::async_trait;
use deltav_utils::{DataStream, DeltavFlowResult};
pub mod file;

/// Trait implemented by pipeline data destinations.
///
/// A `Destination` accepts an async byte-stream produced by the `Source` and
/// handles persisting it (for example, writing to a file or uploading to S3).
#[async_trait]
pub trait Destination: Send {
    async fn load(&mut self, stream: DataStream) -> DeltavFlowResult<()>;
}