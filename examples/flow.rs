use deltav_core::pipeline::Scheduler;
use deltav_core::sources::http_client::HttpSourceBuilder;
use deltav_flow::core::{destinations::Destination, pipeline::Pipeline, sources::Source};
use deltav_flow::core::sources::http_client::HttpSource;
use deltav_flow::core::destinations::file::FileDestination;
use deltav_flow::utils::DeltavFlowResult;
use reqwest::Method;

#[tokio::main]
async fn main() -> DeltavFlowResult<()> {
    // Initialize logging for the example. Use RUST_LOG env var to control level.
    deltav_flow::core::logging::init();

    // Start a metrics exporter on an OS-assigned port and shut it down on Ctrl-C.
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    println!("Starting metrics exporter at http://{}:{}/metrics", addr.ip(), addr.port());
    // Use Ctrl-C as a graceful shutdown signal for the exporter
    let shutdown = async { let _ = tokio::signal::ctrl_c().await; };
    let _metrics_handle = deltav_core::metrics::start_exporter_with_shutdown(listener, shutdown);

    let source: Box <dyn Source> = Box::new(HttpSource::default());
    let destination: Box<dyn Destination> = Box::new(FileDestination("./tests/data/stream/new_stream.blob".to_string()));
    let mut pipe = Pipeline::new(source, destination);
    pipe.set_name("http_default");

    let source1: Box <dyn Source> = Box::new(HttpSource::default());
    let destination1: Box<dyn Destination> = Box::new(FileDestination("./tests/data/stream/new_stream1.blob".to_string()));
    let mut pipe1 = Pipeline::new(source1, destination1);
    pipe1.set_name("http_1");

    let source2: Box<dyn Source> = Box::new(HttpSourceBuilder::new(Method::GET, "https://arxiv.org/pdf/2401.07444".to_string()).build()?);

    let destination2: Box<dyn Destination> = Box::new(FileDestination("./tests/data/stream/pdf.pdf".to_string()));
    let mut pipe2 = Pipeline::new(source2, destination2);
    pipe2.set_name("pdf_fetch");
    
    
    let pipes = vec![pipe1, pipe, pipe2];

    let mut scheduler = Scheduler::new();
    for p in pipes { scheduler.add_pipeline(p);}
    scheduler.start().await?;
    Ok(())
}