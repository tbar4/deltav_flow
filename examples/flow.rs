use std::sync::Arc;
use std::time::Duration;

use deltav_flow::{
    PipelineBuilder, DeltavFlow, Trigger, Source, Destination,
    DeltavFlowResult, DeltavStream,
};
use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

/// Example source that generates fake data.
struct FakeWeatherSource;

#[async_trait::async_trait]
impl Source for FakeWeatherSource {
    fn name(&self) -> &str { "weather_source" }
    fn produces(&self) -> &str { "raw_weather" }

    async fn extract(&self) -> DeltavFlowResult<DeltavStream> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("station", DataType::Utf8, false),
            Field::new("temp_f", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["KNYC", "KLAX", "KORD"])),
                Arc::new(Int32Array::from(vec![32, 68, 15])),
            ],
        ).unwrap();
        tracing::info!("extracted {} weather observations", batch.num_rows());
        Ok(DeltavStream::new(vec![batch]))
    }
}

/// Example destination that prints data to stdout.
struct StdoutDestination;

#[async_trait::async_trait]
impl Destination for StdoutDestination {
    fn name(&self) -> &str { "stdout" }
    fn consumes(&self) -> &str { "raw_weather" }

    async fn load(&self, stream: DeltavStream) -> DeltavFlowResult<()> {
        for batch in stream.batches() {
            tracing::info!("loaded {} rows to stdout", batch.num_rows());
            println!("{:?}", batch);
        }
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    deltav_flow::core::logging::init();

    let (trigger, pipeline) = PipelineBuilder::new("weather_etl")
        .trigger(Trigger::Interval(Duration::from_secs(10)))
        .source(FakeWeatherSource)
        .destination(StdoutDestination)
        .build()?;

    DeltavFlow::new()
        .add_pipeline(trigger, pipeline)
        .run()
        .await?;

    Ok(())
}
