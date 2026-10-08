//! Domain types and lifecycle rules.

mod agent;
mod allocation;
mod dto;
mod event;
mod id;
mod job;
mod job_spec;
mod state_machine;
mod timestamp;

pub use agent::{Agent, AgentAvailability, AgentHealth};
pub use allocation::{Allocation, AllocationStatus};
pub use dto::{AgentSnapshot, AllocationSnapshot, JobSnapshot};
pub use event::{JobEvent, LostReason, ProcessResult, StopReason};
pub use id::{AgentId, AllocationId, JobId};
pub use job::{Job, JobStatus, TerminalResult};
pub use job_spec::{
    JobSpec, JobSpecError, MAX_ARGUMENTS_BYTES, MAX_COMMAND_BYTES, MAX_ENVIRONMENT_BYTES,
    MAX_WORKING_DIR_BYTES,
};
pub use state_machine::{ApplyOutcome, TransitionError, apply_event};
pub use timestamp::Timestamp;
