//! Application use cases and orchestration services.
//!
//! Synchronous services coordinate domain rules with the state store.

mod agents;
mod error;
mod jobs;

#[expect(
    unused_imports,
    reason = "the transport layer consumes application services in M5"
)]
pub(crate) use agents::{AgentAssignment, AgentsService};
pub(crate) use error::map_store_error;
#[expect(
    unused_imports,
    reason = "the transport layer consumes application services in M5"
)]
pub(crate) use jobs::{JobSubmission, JobsService, StopOutcome, StopRequest};
