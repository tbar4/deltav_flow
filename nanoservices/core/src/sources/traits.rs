use async_trait::async_trait;
use deltav_utils::{DeltavFlowResult, DeltavStream};

/// A data source that extracts data and declares what it produces.
#[async_trait]
pub trait Source: Send + Sync {
    /// Human-readable name for logging and metrics
    fn name(&self) -> &str;

    /// The named data output this source produces
    fn produces(&self) -> &str;

    /// Extract data from the source
    async fn extract(&self) -> DeltavFlowResult<DeltavStream>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    struct TestSource;

    #[async_trait]
    impl Source for TestSource {
        fn name(&self) -> &str { "test_source" }
        fn produces(&self) -> &str { "test_data" }
        async fn extract(&self) -> DeltavFlowResult<DeltavStream> {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
            ]));
            let batch = RecordBatch::try_new(
                schema,
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            ).unwrap();
            Ok(DeltavStream::new(vec![batch]))
        }
    }

    #[tokio::test]
    async fn source_trait_works() {
        let src = TestSource;
        assert_eq!(src.name(), "test_source");
        assert_eq!(src.produces(), "test_data");
        let stream = src.extract().await.unwrap();
        assert_eq!(stream.batches().len(), 1);
        assert_eq!(stream.batches()[0].num_rows(), 3);
    }
}
