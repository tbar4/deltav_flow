use deltav_flow::core::{destinations::Destination, pipeline::Pipeline, sources::Source};
use deltav_flow::core::sources::http_client::HttpSource;
use deltav_flow::core::destinations::file::FileDestination;
use deltav_flow::utils::DeltavFlowResult;

#[tokio::main]
async fn main() -> DeltavFlowResult<()> {
    let source: Box <dyn Source> = Box::new(HttpSource::default());
    let destination: Box<dyn Destination> = Box::new(FileDestination("./tests/data/stream/new_stream.blob".to_string()));
    let mut pipe = Pipeline::new(source, destination);

    let source1: Box <dyn Source> = Box::new(HttpSource::default());
    let destination1: Box<dyn Destination> = Box::new(FileDestination("./tests/data/stream/new_stream1.blob".to_string()));
    let mut pipe1 = Pipeline::new(source1, destination1);
    
    let _ = tokio::try_join!(
        pipe.run(),
        pipe1.run(),
    )?;
    Ok(())
}