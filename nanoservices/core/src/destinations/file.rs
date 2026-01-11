use async_trait::async_trait;
use deltav_utils::{DataStream, DeltavFlowResult};
use super::Destination;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use futures_util::StreamExt;

#[derive(Debug)]
pub struct FileDestination(pub String);
#[async_trait]
impl Destination for FileDestination {
    async fn load(&mut self, mut stream: DataStream) -> DeltavFlowResult<()> {
        let mut file = File::create(&self.0).await?;
        while let Some(chunk) = stream.next().await {
            file.write(&chunk?).await?;
        }
        file.flush().await?;
        file.shutdown().await?;
        Ok(())
    }
}