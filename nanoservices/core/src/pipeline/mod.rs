use deltav_utils::DeltavFlowResult;

use super::sources::Source;
use super::destinations::Destination;

pub struct Pipeline {
    source: Box<dyn Source>,
    destination: Box<dyn Destination>,
}

impl Pipeline {
    pub fn new(source: Box<dyn Source>, destination: Box<dyn Destination>) -> Self {
        Self { source, destination }
    }

    pub async fn run(&mut self) -> DeltavFlowResult<()> {
        let stream = self.source.extract().await?;
        self.destination.load(stream).await?;
        Ok(())
    }
}