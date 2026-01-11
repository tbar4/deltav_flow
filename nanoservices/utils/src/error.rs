use thiserror::Error;
use reqwest::Error as ReqwestError;
use tokio::io::Error as TokioIoError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Reqwest Error: {0}")]
    RestSourceError(#[from] ReqwestError),

    #[error("Tokio Error: {0}")]
    TokioError(#[from] TokioIoError),

}