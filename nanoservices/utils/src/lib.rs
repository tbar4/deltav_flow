pub mod error;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use error::Error;
use std::pin::Pin;
use std::sync::Arc;
use futures_util::Stream;

pub type DeltavFlowResult<T> = Result<T, Error>;
pub type DataStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>> + Send>>;

/// Core data type passed between pipeline tasks.
/// Wraps a vector of Arrow RecordBatches with a shared schema.
#[derive(Debug, Clone)]
pub struct DeltavStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl DeltavStream {
    pub fn new(batches: Vec<RecordBatch>) -> Self {
        let schema = if batches.is_empty() {
            Arc::new(arrow::datatypes::Schema::empty())
        } else {
            batches[0].schema()
        };
        Self { schema, batches }
    }

    pub fn empty(schema: SchemaRef) -> Self {
        Self { schema, batches: vec![] }
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    pub fn into_batches(self) -> Vec<RecordBatch> {
        self.batches
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    #[test]
    fn deltav_stream_wraps_record_batches() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        ).unwrap();

        let stream = DeltavStream::new(vec![batch.clone()]);
        assert_eq!(stream.batches().len(), 1);
        assert_eq!(stream.batches()[0].num_rows(), 3);
        assert_eq!(stream.schema(), schema);
    }

    #[test]
    fn deltav_stream_empty() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("x", DataType::Int32, false),
        ]));
        let stream = DeltavStream::empty(schema.clone());
        assert_eq!(stream.batches().len(), 0);
        assert_eq!(stream.schema(), schema);
    }
}
