use async_trait::async_trait;
use deltav_utils::{DeltavFlowResult, DataStream};

pub mod http_client;

#[async_trait]
pub trait Source: Send {
    async fn extract(&self) -> DeltavFlowResult<DataStream>;
}