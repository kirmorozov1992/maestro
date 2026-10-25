//! Application use cases and orchestration services.
//!
//! Synchronous services coordinate domain rules with the state store.

mod agents;
mod error;
mod jobs;

pub(crate) use agents::{AgentAssignment, AgentsService};
pub(crate) use error::map_store_error;
#[cfg_attr(
    not(test),
    expect(
        unused_imports,
        reason = "M5.3 stop handler will map this application request"
    )
)]
pub(crate) use jobs::StopRequest;
pub(crate) use jobs::{JobSubmission, JobsService, StopOutcome};
