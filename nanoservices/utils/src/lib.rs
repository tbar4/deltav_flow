pub mod error;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use error::Error;
use std::pin::Pin;
use std::sync::Arc;
use futures_util::Stream;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;

pub type DeltavFlowResult<T> = Result<T, Error>;
//pub type DataStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>> + Send>>;

#[derive(Clone)]
pub struct DeltavStream(pub Arc<Vec<RecordBatch>>);

impl DeltavStream {
    pub fn new(stream: Vec<RecordBatch>) -> Self {
        DeltavStream(Arc::new(stream))
    }
}
