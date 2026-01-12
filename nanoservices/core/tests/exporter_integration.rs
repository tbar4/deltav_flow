use std::net::TcpListener;
use std::time::Duration;
use reqwest::blocking::Client;
use std::thread;

use deltav_core::metrics;

#[test]
fn exporter_serves_metrics() {
    // Reset or ensure metrics are in known state if needed (gather_text reads from global registry).

    // Bind to port 0 so the OS assigns an available port.
    let std_listener = TcpListener::bind("127.0.0.1:0").expect("failed to bind");
    let addr = std_listener.local_addr().unwrap();

    // Start exporter server using the bound listener and a oneshot shutdown handle.
    let (handle, shutdown_tx) = metrics::start_exporter(std_listener);

    // Emit some metric activity
    metrics::inc_run("test_pipeline");

    // Wait briefly for the server to start
    thread::sleep(Duration::from_millis(100));

    // Query the /metrics endpoint
    let url = format!("http://{}:{}/metrics", addr.ip(), addr.port());
    // Use blocking reqwest client for simplicity in tests
    let client = Client::builder().timeout(Duration::from_secs(2)).build().unwrap();
    let resp = client.get(&url).send().expect("failed to query metrics");
    assert!(resp.status().is_success());
    let body_text = resp.text().expect("failed to read body");

    // Check that the metrics text contains our pipeline metric label and the counter name
    assert!(body_text.contains("pipeline_runs_total"), "metrics output missing runs metric: {}", body_text);
    assert!(body_text.contains("test_pipeline"), "metrics output missing our label: {}", body_text);

    // Clean up: request graceful shutdown and allow server to stop
    shutdown_tx.send(()).expect("failed to send shutdown");
    thread::sleep(Duration::from_millis(50));
}