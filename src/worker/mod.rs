//! Worker-side API client, agent runtime, and local process executor.
//!
//! Agent registration and heartbeat are wired in M6.2. Assignment polling and
//! process execution are implemented in later M6 tasks.
pub(crate) mod agent;
pub(crate) mod client;
pub(crate) mod launch_registry;
pub(crate) mod working_directory;
