use std::sync::Arc;

use arrow::ipc::writer::FileWriter;
use arrow_json::ReaderBuilder;
use arrow_json::reader::infer_json_schema;
use deltav_core::pipeline::Scheduler;
use deltav_core::sources::http_client::HttpSourceBuilder;
use deltav_flow::core::{destinations::Destination, pipeline::Pipeline, sources::Source};
use deltav_flow::core::sources::http_client::HttpSource;
use deltav_flow::core::destinations::file::FileDestination;
use deltav_flow::utils::DeltavFlowResult;
use polars::frame::column;
use polars::io::ArrowReader;
use reqwest::Method;
use tokio::fs::File;
use tokio::io::BufReader;
use polars::prelude::*;
use datafusion::prelude::*;
use reqwest::Client;

#[tokio::main]
async fn main() -> DeltavFlowResult<()> {
    let client = Client::new();
    let resp = client.get("https://api.spaceflightnewsapi.net/v4/articles?limit=1").send().await?;
    let app_code = resp.headers();

    let body = match app_code.get("content-type") {
        Some(content_type) if content_type.to_str().unwrap() == "application/json" => {
            let json: serde_json::Value = resp.json().await?;
            json
        },
        _ => {
            let text = resp.text().await?;
            serde_json::Value::String(text)
        }
    };
    println!("App code: {:?}", body);

    /*
    // Initialize logging for the example. Use RUST_LOG env var to control level.
    deltav_flow::core::logging::init();
 
    // Start a metrics exporter on an OS-assigned port and shut it down on Ctrl-C.
    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?;
    println!("Starting metrics exporter at http://{}:{}/metrics", addr.ip(), addr.port());
    // Use Ctrl-C as a graceful shutdown signal for the exporter
    let shutdown = async { let _ = tokio::signal::ctrl_c().await; };
    let _metrics_handle = deltav_core::metrics::start_exporter_with_shutdown(listener, shutdown);


    let spacedevs = HttpSourceBuilder::new(
        Method::GET,
        "https://api.spaceflightnewsapi.net/v4/articles".to_string(),
    );
    let source: Box <dyn Source> = Box::new(spacedevs.build()?);
    let destination: Box<dyn Destination> = Box::new(FileDestination("./tests/data/stream/new_stream.ipc".to_string()));
    let mut pipe = Pipeline::new(source, destination);
    pipe.set_name("http_default");

    let source1: Box <dyn Source> = Box::new(HttpSource::default());
    let destination1: Box<dyn Destination> = Box::new(FileDestination("./tests/data/stream/new_stream1.ipc".to_string()));
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
    */
    /* 
    let test_json = std::fs::File::open("./tests/data/stream/new_stream.json")?;

    let mut reader = std::io::BufReader::new(test_json);
    let schema = infer_json_schema(&mut reader, None)?;
    let schema_copy = schema.clone();


    let json = std::fs::File::open("./tests/data/stream/new_stream.json")?;

    let rd = std::io::BufReader::new(json);
    let mut batch = ReaderBuilder::new(Arc::new(schema.0)).build(rd)?;

    let file = std::fs::File::create("./spacedevs.ipc")?;
    let mut writer = FileWriter::try_new(file, &schema_copy.0)?;
     
    while let Some(b) =  batch.next().transpose()? {
        println!("Read Arrow RecordBatch with {} rows and {} columns:\n{:?}", b.num_rows(), b.num_columns(), b);
        writer.write(&b)?;
        
    }

    writer.finish()?;
    */
    /*
    let df = IpcReader::new(std::fs::File::open("./spacedevs.arrow")?).finish()?;

    let df_results = df
        .lazy()
        .select([col("results")])
        .explode([col("results")])
        .unnest([col("results")])
        .explode([col("authors")])
        .unnest([col("authors")])
        .collect()?;

    println!("df: {df_results:#?}");
    */
    Ok(())
}