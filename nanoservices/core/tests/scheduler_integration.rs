use deltav_core::pipeline::Scheduler;
use deltav_core::pipeline::Pipeline;
use deltav_core::sources::Source;
use deltav_core::destinations::Destination;
use deltav_utils::DeltavFlowResult;
use futures_util::stream::{self, StreamExt};
use std::sync::{Arc, atomic::{AtomicBool, Ordering}};

struct MockSource;
#[async_trait::async_trait]
impl Source for MockSource {
    async fn extract(&self) -> DeltavFlowResult<deltav_utils::DataStream> {
        let s = stream::once(async { Ok(vec![1u8]) });
        Ok(Box::pin(s))
    }
}

struct MockDestination;
#[async_trait::async_trait]
impl Destination for MockDestination {
    async fn load(&mut self, mut stream: deltav_utils::DataStream) -> DeltavFlowResult<()> {
        while let Some(chunk) = stream.next().await {
            let _ = chunk?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn scheduler_start_with_shutdown_finishes_and_runs_cleanup() {
    deltav_core::logging::init();
    let source: Box<dyn Source> = Box::new(MockSource);
    let destination: Box<dyn Destination> = Box::new(MockDestination);
    let mut p1 = Pipeline::new(source, destination);
    p1.set_interval(tokio::time::Duration::from_millis(5));

    let cleaned1 = Arc::new(AtomicBool::new(false));
    let c1 = cleaned1.clone();
    p1.on_shutdown(move || {
        let c1 = c1.clone();
        async move {
            c1.store(true, Ordering::SeqCst);
            Ok(())
        }
    });

    let source2: Box<dyn Source> = Box::new(MockSource);
    let destination2: Box<dyn Destination> = Box::new(MockDestination);
    let mut p2 = Pipeline::new(source2, destination2);
    p2.set_interval(tokio::time::Duration::from_millis(7));

    let cleaned2 = Arc::new(AtomicBool::new(false));
    let c2 = cleaned2.clone();
    p2.on_shutdown(move || {
        let c2 = c2.clone();
        async move {
            c2.store(true, Ordering::SeqCst);
            Ok(())
        }
    });

    let mut scheduler = Scheduler::new();
    scheduler.add_pipeline(p1);
    scheduler.add_pipeline(p2);

    // oneshot channel to trigger shutdown
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let handle = tokio::spawn(async move {
        // start_with_shutdown consumes the scheduler
        scheduler.start_with_shutdown(async move {
            let _ = rx.await;
        })
        .await
        .unwrap();
    });

    // let the scheduler run a bit
    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

    // trigger shutdown
    let _ = tx.send(());

    // wait for scheduler to finish
    let _ = handle.await;

    assert!(cleaned1.load(Ordering::SeqCst));
    assert!(cleaned2.load(Ordering::SeqCst));
}
