use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use crate::events::trigger::{Trigger, TriggerEvent};

/// Spawns a tokio task that emits TriggerEvents on a fixed interval.
/// Returns a JoinHandle that can be aborted to stop the timer.
pub fn spawn_interval_trigger(
    pipeline: String,
    interval: Duration,
    sender: mpsc::Sender<TriggerEvent>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        // Skip the first immediate tick
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let event = TriggerEvent {
                pipeline: pipeline.clone(),
                trigger: Trigger::Interval(interval),
            };
            if sender.send(event).await.is_err() {
                // Receiver dropped, stop producing
                break;
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn interval_trigger_produces_events() {
        let (tx, mut rx) = mpsc::channel(16);

        let handle = spawn_interval_trigger(
            "test_pipeline".to_string(),
            Duration::from_millis(50),
            tx,
        );

        // Should receive at least 2 events within 200ms
        let e1 = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(e1.pipeline, "test_pipeline");

        let e2 = tokio::time::timeout(Duration::from_millis(200), rx.recv())
            .await
            .expect("timeout")
            .expect("channel closed");
        assert_eq!(e2.pipeline, "test_pipeline");

        handle.abort();
    }

    #[tokio::test]
    async fn interval_trigger_stops_when_receiver_drops() {
        let (tx, rx) = mpsc::channel(1);

        let handle = spawn_interval_trigger(
            "test".to_string(),
            Duration::from_millis(10),
            tx,
        );

        // Drop receiver
        drop(rx);

        // Handle should complete (not hang)
        let result = tokio::time::timeout(Duration::from_millis(500), handle).await;
        assert!(result.is_ok());
    }
}
