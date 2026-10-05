//! Domain types and lifecycle rules.

mod id;
mod job_spec;
mod timestamp;

pub use id::{AgentId, AllocationId, JobId};
pub use job_spec::{
    JobSpec, JobSpecError, MAX_ARGUMENTS_BYTES, MAX_COMMAND_BYTES, MAX_ENVIRONMENT_BYTES,
    MAX_WORKING_DIR_BYTES,
};
pub use timestamp::Timestamp;
