//! Worker-side API client, agent runtime, and local process executor.
//!
//! Registration, heartbeat and single-assignment polling run in the agent.
//! A local executor is available; assignment execution stays deferred until
//! execution data and lifecycle reporting are connected.
pub(crate) mod agent;
pub(crate) mod client;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "M6.7 will wire the local executor after execution data and reporting are defined"
    )
)]
pub(crate) mod executor;
pub(crate) mod launch_registry;
pub(crate) mod working_directory;
