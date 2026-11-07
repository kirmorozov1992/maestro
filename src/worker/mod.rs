//! Worker-side API client, agent runtime, and local process executor.
//!
//! Registration, heartbeat and single-assignment polling run in the agent.
//! Assignments execute with bounded output and explicit lifecycle reporting.
pub(crate) mod agent;
pub(crate) mod client;
pub(crate) mod executor;
pub(crate) mod launch_registry;
pub(crate) mod working_directory;

mod lifecycle;

mod shutdown;
