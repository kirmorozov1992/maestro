//! Worker-side API client, agent runtime, and local process executor.
//!
//! Registration, heartbeat and single-assignment polling are wired in the agent
//! runtime. Assignments are deferred until the executor/reporting stages ship.
pub(crate) mod agent;
pub(crate) mod client;
pub(crate) mod launch_registry;
pub(crate) mod working_directory;
