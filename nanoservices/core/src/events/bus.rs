use tokio::sync::mpsc;
use crate::events::trigger::TriggerEvent;

/// Channel-based event bus for delivering trigger events to the scheduler.
pub struct EventBus {
    sender: mpsc::Sender<TriggerEvent>,
    receiver: mpsc::Receiver<TriggerEvent>,
}

impl EventBus {
    pub fn new(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self { sender, receiver }
    }

    /// Get a sender handle that can be cloned and given to trigger producers.
    pub fn sender(&self) -> mpsc::Sender<TriggerEvent> {
        self.sender.clone()
    }

    /// Receive the next trigger event. Returns None when all senders are dropped.
    pub async fn recv(&mut self) -> Option<TriggerEvent> {
        self.receiver.recv().await
    }

    /// Split into sender and receiver (consumes self).
    pub fn split(self) -> (mpsc::Sender<TriggerEvent>, mpsc::Receiver<TriggerEvent>) {
        (self.sender, self.receiver)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::events::trigger::Trigger;
    use std::time::Duration;

    #[tokio::test]
    async fn event_bus_sends_and_receives() {
        let mut bus = EventBus::new(16);
        let sender = bus.sender();

        sender.send(TriggerEvent {
            pipeline: "test_pipeline".to_string(),
            trigger: Trigger::Interval(Duration::from_secs(60)),
        }).await.unwrap();

        let event = bus.recv().await.unwrap();
        assert_eq!(event.pipeline, "test_pipeline");
        assert!(matches!(event.trigger, Trigger::Interval(_)));
    }

    #[tokio::test]
    async fn event_bus_multiple_senders() {
        let mut bus = EventBus::new(16);
        let sender1 = bus.sender();
        let sender2 = bus.sender();

        sender1.send(TriggerEvent {
            pipeline: "pipeline_a".to_string(),
            trigger: Trigger::Interval(Duration::from_secs(10)),
        }).await.unwrap();

        sender2.send(TriggerEvent {
            pipeline: "pipeline_b".to_string(),
            trigger: Trigger::Webhook { path: "/trigger/b".to_string() },
        }).await.unwrap();

        let e1 = bus.recv().await.unwrap();
        let e2 = bus.recv().await.unwrap();
        assert_eq!(e1.pipeline, "pipeline_a");
        assert_eq!(e2.pipeline, "pipeline_b");
    }
}
