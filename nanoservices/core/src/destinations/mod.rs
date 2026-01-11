use async_trait::async_trait;
use deltav_utils::{DataStream, DeltavFlowResult};
pub mod file;

#[async_trait]
pub trait Destination: Send {
    async fn load(&mut self, stream: DataStream) -> DeltavFlowResult<()>;
}