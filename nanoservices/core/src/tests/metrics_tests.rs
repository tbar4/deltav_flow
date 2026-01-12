#[cfg(test)]
mod metrics_tests {
    use super::super::metrics;
    use crate::pipeline::Pipeline;
    use crate::sources::http_client::HttpSourceBuilder;
    use crate::destinations::file::FileDestination;
    use deltav_flow::utils::DeltavFlowResult;
    use std::time::Duration as StdDuration;

    #[tokio::test]
    async fn metrics_are_recorded() {
        // initialize logging so we can run without panicking
        crate::logging::init();

        // create a pipeline that will perform one run
        let source = Box::new(HttpSourceBuilder::new(reqwest::Method::GET, "https://jsonplaceholder.typicode.com/todos/1".to_string()).build().unwrap());
        let destination: Box<dyn crate::destinations::Destination> = Box::new(FileDestination("./tests/data/stream/metric_test.blob".to_string()));
        let mut p = Pipeline::new(source, destination);
        p.set_name("metrics_test");
        p.set_interval(tokio::time::Duration::from_millis(5));

        let (tx, rx) = tokio::sync::watch::channel(false);
        let handle = tokio::spawn(async move {
            p.worker(rx).await;
        });

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
        tx.send(true).unwrap();
        let _ = handle.await;

        // Check that metrics gather contains our metric name
        let txt = metrics::gather_text();
        assert!(txt.contains("pipeline_runs_total"));
        assert!(txt.contains("metrics_test"));
    }
}