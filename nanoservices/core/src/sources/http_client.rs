use std::collections::HashMap;

use async_trait::async_trait;
use reqwest::{Client as ReqwestClient, Method, Request, Response};
use futures_util::stream::StreamExt;
use deltav_utils::{DeltavFlowResult, DataStream};
use super::Source;

#[derive(Debug)]
pub struct HttpSourceBuilder {
    method: Method,
    url: String,
    token: Option<String>,
    params: Option<HashMap<String, String>>,
}

impl Default for HttpSourceBuilder {
    fn default() -> Self {
    
        Self {
            method: Method::GET,
            url: "https://jsonplaceholder.typicode.com/todos/1".to_string(),
            token: None,
            params: None,
        }
    }
}

impl HttpSourceBuilder {
    pub fn new(method: Method, url: String) -> Self {
        Self { method, url, token: None, params: None }
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

    fn get_request(&self) -> Request {
        self.0.try_clone().unwrap()
    }

    async fn fetch_data(&self) -> DeltavFlowResult<Response> {
        let client: ReqwestClient = ReqwestClient::new();
        let resp = client.execute(self.get_request()).await?;
        Ok(resp)
    }
}

#[async_trait]
impl Source for HttpSource {
    async fn extract(&self) -> DeltavFlowResult<DataStream> {
        let resp = self.fetch_data().await?;
        //let content_length = self.content_length(&resp).await;
        let stream = resp.bytes_stream().map(|result| result.map(|bytes| bytes.to_vec()).map_err(Into::into));
        Ok(Box::pin(stream))
    }
}