use async_trait::async_trait;
use deltav_utils::{DeltavStream, DeltavFlowResult};
use super::Destination;
use std::fs::File;


#[derive(Debug)]
pub struct FileDestination(pub String);
#[async_trait]
impl Destination for FileDestination {
    async fn load(&mut self, stream: DeltavStream) -> DeltavFlowResult<()> {
        let mut file = File::create(&self.0)?;
        let schema = stream.0[0].schema();
        let mut writer = arrow::ipc::writer::StreamWriter::try_new(&mut file, &schema)?;
        for batch in stream.0.iter() {
            writer.write(batch)?;
        }
        writer.finish()?;
        Ok(())
    }
}