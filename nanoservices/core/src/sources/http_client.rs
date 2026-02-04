use std::collections::HashMap;
use std::io::Cursor;

use async_trait::async_trait;
use reqwest::{Client as ReqwestClient, Method, Request, Response};
use deltav_utils::{DeltavFlowResult, DeltavStream};
use arrow::ipc::reader::StreamReader;
use futures_util::TryStreamExt;

use super::Source;

#[derive(Debug)]
pub struct HttpSourceBuilder {
    method: Method,
    url: String,
    token: Option<String>,
    params: Option<HashMap<String, String>>,
    headers: Option<HashMap<String, String>>,
}

impl Default for HttpSourceBuilder {
    fn default() -> Self {
        Self {
            method: Method::GET,
            url: "https://jsonplaceholder.typicode.com/todos/1".to_string(),
            token: None,
            params: None,
            headers: None,
        }
    }
}

impl HttpSourceBuilder {
    pub fn new(method: Method, url: String) -> Self {
        Self { method, url, token: None, params: None, headers: None }
    }

    pub fn method(mut self, method: Method) -> Self {
        self.method = method;
        self
    }

    pub fn url<T: Into<String>>(mut self, url: T) -> Self {
        self.url = url.into();
        self
    }

    pub fn token<T: Into<String>>(mut self, token: Option<T>) -> Self {
        self.token = token.map(|t| t.into());
        self
    }

    pub fn params(mut self, params: HashMap<String, String>) -> Self {
        self.params = Some(params);
        self
    }

    pub fn headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = Some(headers);
        self
    }

    pub fn get<T: Into<String>>(url: T) -> Self {
        Self::new(Method::GET, url.into())
    }

    pub fn post<T: Into<String>>(url: T) -> Self {
        Self::new(Method::POST, url.into())
    }

    pub fn put<T: Into<String>>(url: T) -> Self {
        Self::new(Method::PUT, url.into())
    }

    pub fn delete<T: Into<String>>(url: T) -> Self {
        Self::new(Method::DELETE, url.into())
    }

    pub fn header<T: Into<String>>(mut self, key: T, value: T) -> Self {
        let mut headers = self.headers.unwrap_or_default();
        headers.insert(key.into(), value.into());
        self.headers = Some(headers);
        self
    }

    pub fn build(self) -> DeltavFlowResult<HttpSource> {
        let client = ReqwestClient::new();
        let mut request = client.request(self.method.clone(), &self.url);

        // Add parameters first (if any)
        if let Some(params) = self.params {
            request = request.query(&params);
        }

        // Add authentication token (if any)
        if let Some(token) = self.token {
            request = request.bearer_auth(token);
        }

        // Add custom headers (if any)
        if let Some(headers) = self.headers {
            for (key, value) in headers {
                request = request.header(key, value);
            }
        }

        let request = request.build()?;

        Ok(HttpSource(request))
    }
}

pub struct HttpSource(Request);

impl Default for HttpSource {
    fn default() -> Self {
        let req = HttpSourceBuilder::default().build().unwrap();
        req
    }
}

impl HttpSource {
    pub fn new(req: Request) -> Self {
        Self(req)
    }

    pub fn get_request(&self) -> Request {
        self.0.try_clone().unwrap()
    }

    async fn fetch_data(&self) -> DeltavFlowResult<Response> {
        let client: ReqwestClient = ReqwestClient::new();
        let resp = client.execute(self.get_request()).await?;
        
        // Check if the response was successful
        if !resp.status().is_success() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("HTTP request failed with status: {}", resp.status())
            ).into());
        }
        
        Ok(resp)
    }
}

#[async_trait]
impl Source for HttpSource {
    async fn extract(&self) -> DeltavFlowResult<DeltavStream> {
        let resp = self.fetch_data().await?;
        
        tracing::info!("HTTP request successful to {}", self.0.url());
        
        // 1. Collect full bytes (fixes partial stream / UnexpectedEof)
        let bytes: Vec<u8> = resp
            .bytes_stream()
            .try_fold(Vec::new(), |mut acc, chunk| async move {
                acc.extend_from_slice(&chunk);
                Ok(acc)
            })
            .await?;

        tracing::info!("Fetched {} bytes from {}", bytes.len(), self.0.url());

        // 2. Validate non-empty
        if bytes.is_empty() {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "Empty response").into());
        }

        // 3. Arrow IPC StreamReader on Cursor (handles buffer filling correctly)
        let cursor = Cursor::new(&bytes[..]);
        let mut reader = match StreamReader::try_new(cursor, None) {
            Ok(reader) => reader,
            Err(e) => {
                tracing::error!("Failed to create Arrow StreamReader: {}", e);
                return Err(e.into());
            }
        };

        // 4. Collect all batches into Vec
        let mut batches = Vec::new();
        while let Some(result) = reader.next() {
            match result {
                Ok(batch) => {
                    batches.push(batch);
                }
                Err(e) => {
                    tracing::error!("Failed to decode RecordBatch: {}", e);
                    return Err(e.into());
                }
            }
        }

        // 5. Handle decode errors gracefully
        if batches.is_empty() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("No valid RecordBatches decoded (bytes: {})", bytes.len())
            ).into());
        }

        tracing::info!("Decoded {} RecordBatches (rows: {})", 
                   batches.len(), batches.iter().map(|b| b.num_rows()).sum::<usize>());

        // 6. Return as DeltavStream
        Ok(DeltavStream::new(batches))
    }
}