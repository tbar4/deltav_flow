use thiserror::Error;
use reqwest::Error as ReqwestError;
use tokio::io::Error as TokioIoError;
use polars::error::PolarsError;
use url::ParseError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Reqwest Error: {0}")]
    RestSourceError(#[from] ReqwestError),

    #[error("Tokio Error: {0}")]
    TokioError(#[from] TokioIoError),

    #[error("Polars Error: {0}")]
    PolarsDataError(#[from] PolarsError),

    #[error("Url Error: {0}")]
    UrlParseError(#[from] ParseError),
}