use async_trait::async_trait;
use reqwest::{Request, Response, Url, Client as ReqwestClient};
use futures_util::stream::StreamExt;
use deltav_utils::{DeltavFlowResult, DataStream};
use super::Source;

#[derive(Debug)]
pub struct HttpSource(pub Request);

impl Default for HttpSource {
    fn default() -> Self {
        let method = reqwest::Method::GET;
        let url = Url::parse("https://jsonplaceholder.typicode.com/todos/1").unwrap();
        Self(Request::new(method, url))
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
        let stream = resp.bytes_stream().map(|result| result.map(|bytes| bytes.to_vec()));
        Ok(Box::pin(stream))
    }
}
