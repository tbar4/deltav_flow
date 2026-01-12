pub mod error;
use error::Error;
use std::pin::Pin;
use futures_util::Stream;

pub type DeltavFlowResult<T> = Result<T, Error>;
pub type DataStream = Pin<Box<dyn Stream<Item = Result<Vec<u8>, Error>> + Send>>;