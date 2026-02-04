pub use deltav_core as core;
pub use deltav_utils as utils;

// Convenience re-exports for common usage
pub use deltav_core::builder::PipelineBuilder;
pub use deltav_core::engine::DeltavFlow;
pub use deltav_core::events::trigger::Trigger;
pub use deltav_core::sources::traits::Source;
pub use deltav_core::transforms::traits::Transform;
pub use deltav_core::destinations::traits::Destination;
pub use deltav_utils::{DeltavFlowResult, DeltavStream};
