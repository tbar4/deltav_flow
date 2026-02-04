use async_trait::async_trait;
use deltav_utils::{DeltavFlowResult, DeltavStream};

/// A data destination that loads data and declares what it consumes.
#[async_trait]
pub trait Destination: Send + Sync {
    fn name(&self) -> &str;

    /// The named data input this destination requires
    fn consumes(&self) -> &str;

    /// Load data to the destination
    async fn load(&self, stream: DeltavStream) -> DeltavFlowResult<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingDestination {
        count: AtomicUsize,
    }

    #[async_trait]
    impl Destination for CountingDestination {
        fn name(&self) -> &str { "counter" }
        fn consumes(&self) -> &str { "input_data" }
        async fn load(&self, stream: DeltavStream) -> DeltavFlowResult<()> {
            let total: usize = stream.batches().iter().map(|b| b.num_rows()).sum();
            self.count.fetch_add(total, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn destination_trait_works() {
        let dest = CountingDestination { count: AtomicUsize::new(0) };
        assert_eq!(dest.name(), "counter");
        assert_eq!(dest.consumes(), "input_data");

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        ).unwrap();
        let stream = DeltavStream::new(vec![batch]);

        dest.load(stream).await.unwrap();
        assert_eq!(dest.count.load(Ordering::SeqCst), 3);
    }
}
