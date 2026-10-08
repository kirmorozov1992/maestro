use crate::domain::{
    AgentId, Allocation, AllocationId, AllocationStatus, Job, JobEvent, JobId, JobStatus,
    ProcessResult, TerminalResult, Timestamp,
};
use std::{error::Error, fmt};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplyOutcome {
    Applied,
    Duplicate,
    Noop,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransitionError {
    JobIdMismatch {
        expected: JobId,
        actual: JobId,
    },
    AllocationMissing {
        allocation_id: Option<AllocationId>,
    },
    AllocationMismatch {
        expected: AllocationId,
        actual: AllocationId,
    },
    AllocationLinkMismatch {
        expected_job_id: JobId,
        actual_job_id: JobId,
    },
    AgentMismatch {
        expected: AgentId,
        actual: AgentId,
    },
    InconsistentState {
        job_status: JobStatus,
        allocation_status: AllocationStatus,
    },
    InvalidTransition {
        job_status: JobStatus,
        event: &'static str,
    },
    StaleEvent {
        occurred_at: Timestamp,
        current_at: Timestamp,
    },
    TerminalConflict {
        job_status: JobStatus,
        event: &'static str,
    },
}

impl TransitionError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::JobIdMismatch { .. } => "job_id_mismatch",
            Self::AllocationMissing { .. } => "allocation_missing",
            Self::AllocationMismatch { .. } | Self::AllocationLinkMismatch { .. } => {
                "allocation_mismatch"
            }
            Self::AgentMismatch { .. } => "agent_mismatch",
            Self::InconsistentState { .. } => "inconsistent_state",
            Self::InvalidTransition { .. } => "invalid_transition",
            Self::StaleEvent { .. } => "stale_event",
            Self::TerminalConflict { .. } => "terminal_conflict",
        }
    }
}

impl fmt::Display for TransitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::JobIdMismatch { expected, actual } => {
                write!(
                    formatter,
                    "job id mismatch: expected {expected}, got {actual}"
                )
            }
            Self::AllocationMissing { allocation_id } => {
                write!(
                    formatter,
                    "allocation is required for this event: {allocation_id:?}"
                )
            }
            Self::AllocationMismatch { expected, actual } => {
                write!(
                    formatter,
                    "allocation id mismatch: expected {expected}, got {actual}"
                )
            }
            Self::AllocationLinkMismatch {
                expected_job_id,
                actual_job_id,
            } => write!(
                formatter,
                "allocation job id mismatch: expected {expected_job_id}, got {actual_job_id}"
            ),
            Self::AgentMismatch { expected, actual } => {
                write!(
                    formatter,
                    "agent id mismatch: expected {expected}, got {actual}"
                )
            }
            Self::InconsistentState {
                job_status,
                allocation_status,
            } => write!(
                formatter,
                "inconsistent job and allocation states: {job_status:?}, {allocation_status:?}"
            ),
            Self::InvalidTransition { job_status, event } => {
                write!(
                    formatter,
                    "event {event} is invalid for job state {job_status:?}"
                )
            }
            Self::StaleEvent {
                occurred_at,
                current_at,
            } => write!(
                formatter,
                "event timestamp {} precedes current timestamp {}",
                occurred_at.as_unix_millis(),
                current_at.as_unix_millis()
            ),
            Self::TerminalConflict { job_status, event } => {
                write!(
                    formatter,
                    "event {event} conflicts with terminal job state {job_status:?}"
                )
            }
        }
    }
}

impl Error for TransitionError {}

pub fn apply_event(
    job: &mut Job,
    allocation: Option<&mut Allocation>,
    event: &JobEvent,
) -> Result<ApplyOutcome, TransitionError> {
    if let JobEvent::Submitted {
        job_id,
        occurred_at,
    } = event
    {
        if *job_id != job.id() {
            return Err(TransitionError::JobIdMismatch {
                expected: job.id(),
                actual: *job_id,
            });
        }
        if *occurred_at != job.submitted_at() {
            return Err(TransitionError::InvalidTransition {
                job_status: job.status(),
                event: "submitted",
            });
        }
        return Ok(ApplyOutcome::Noop);
    }

    validate_event_context(job, allocation.as_deref(), event)?;

    if let Some(terminal) = terminal_event(event)
        && is_terminal_status(job.status())
    {
        return if is_exact_terminal_duplicate(job, allocation.as_deref(), event, terminal) {
            Ok(ApplyOutcome::Duplicate)
        } else {
            Err(TransitionError::TerminalConflict {
                job_status: job.status(),
                event: event_name(event),
            })
        };
    }

    let transition = plan_transition(job, allocation.as_deref(), event)?;
    validate_timestamps(job, allocation.as_deref(), event)?;

    job.apply_lifecycle_update(
        transition.job_status,
        transition.updated_at,
        transition.started_at,
        transition.finished_at,
        transition.terminal_result,
        transition.terminal_allocation,
    );

    if let (Some(allocation), Some(update)) = (allocation, transition.allocation) {
        allocation.apply_lifecycle_update(update.status, update.started_at, update.finished_at);
    }

    Ok(ApplyOutcome::Applied)
}

fn validate_event_context(
    job: &Job,
    allocation: Option<&Allocation>,
    event: &JobEvent,
) -> Result<(), TransitionError> {
    let (event_job_id, event_agent_id) = match event {
        JobEvent::Submitted { .. } => return Ok(()),
        JobEvent::Assigned {
            job_id, agent_id, ..
        }
        | JobEvent::Started {
            job_id, agent_id, ..
        }
        | JobEvent::Finished {
            job_id, agent_id, ..
        }
        | JobEvent::Lost {
            job_id, agent_id, ..
        } => (*job_id, Some(*agent_id)),
        JobEvent::Stopped {
            job_id, agent_id, ..
        } => (*job_id, *agent_id),
    };

    if event_job_id != job.id() {
        return Err(TransitionError::JobIdMismatch {
            expected: job.id(),
            actual: event_job_id,
        });
    }

    if let JobEvent::Stopped {
        allocation_id,
        agent_id,
        ..
    } = event
        && allocation_id.is_some() != agent_id.is_some()
    {
        return Err(invalid_transition(job, "stopped"));
    }

    let Some(allocation) = allocation else {
        if let Some(allocation_id) = event_allocation_id(event) {
            return Err(TransitionError::AllocationMissing {
                allocation_id: Some(allocation_id),
            });
        }
        return Ok(());
    };

    if let Some(expected_allocation_id) = event_allocation_id(event)
        && expected_allocation_id != allocation.id()
    {
        return Err(TransitionError::AllocationMismatch {
            expected: expected_allocation_id,
            actual: allocation.id(),
        });
    }

    if allocation.job_id() != job.id() {
        return Err(TransitionError::AllocationLinkMismatch {
            expected_job_id: job.id(),
            actual_job_id: allocation.job_id(),
        });
    }

    if let Some(expected_agent_id) = event_agent_id
        && expected_agent_id != allocation.agent_id()
    {
        return Err(TransitionError::AgentMismatch {
            expected: expected_agent_id,
            actual: allocation.agent_id(),
        });
    }

    if !statuses_are_consistent(job.status(), allocation.status()) {
        return Err(TransitionError::InconsistentState {
            job_status: job.status(),
            allocation_status: allocation.status(),
        });
    }

    Ok(())
}

fn statuses_are_consistent(job_status: JobStatus, allocation_status: AllocationStatus) -> bool {
    matches!(
        (job_status, allocation_status),
        (JobStatus::Pending, AllocationStatus::Assigned)
            | (JobStatus::Assigned, AllocationStatus::Assigned)
            | (JobStatus::Running, AllocationStatus::Running)
            | (JobStatus::Succeeded, AllocationStatus::Succeeded)
            | (JobStatus::Failed, AllocationStatus::Failed)
            | (JobStatus::Stopped, AllocationStatus::Stopped)
            | (JobStatus::Lost, AllocationStatus::Lost)
    )
}

fn validate_timestamps(
    job: &Job,
    allocation: Option<&Allocation>,
    event: &JobEvent,
) -> Result<(), TransitionError> {
    let occurred_at = event_occurred_at(event);
    if occurred_at < job.updated_at() {
        return Err(TransitionError::StaleEvent {
            occurred_at,
            current_at: job.updated_at(),
        });
    }

    if let Some(allocation) = allocation {
        let current_at = match event {
            JobEvent::Assigned { .. }
            | JobEvent::Started { .. }
            | JobEvent::Finished {
                result: ProcessResult::SpawnFailed,
                ..
            } => allocation.assigned_at(),
            JobEvent::Finished {
                result: ProcessResult::Exited { .. },
                ..
            } => allocation
                .started_at()
                .ok_or(TransitionError::InconsistentState {
                    job_status: job.status(),
                    allocation_status: allocation.status(),
                })?,
            JobEvent::Stopped { .. } | JobEvent::Lost { .. } => allocation
                .started_at()
                .unwrap_or_else(|| allocation.assigned_at()),
            JobEvent::Submitted { .. } => return Ok(()),
        };
        if occurred_at < current_at {
            return Err(TransitionError::StaleEvent {
                occurred_at,
                current_at,
            });
        }
    }

    Ok(())
}

fn event_occurred_at(event: &JobEvent) -> Timestamp {
    match event {
        JobEvent::Submitted { occurred_at, .. }
        | JobEvent::Assigned { occurred_at, .. }
        | JobEvent::Started { occurred_at, .. }
        | JobEvent::Finished { occurred_at, .. }
        | JobEvent::Stopped { occurred_at, .. }
        | JobEvent::Lost { occurred_at, .. } => *occurred_at,
    }
}

fn terminal_event(event: &JobEvent) -> Option<(JobStatus, AllocationStatus, TerminalResult)> {
    match event {
        JobEvent::Finished {
            result: ProcessResult::SpawnFailed,
            ..
        } => Some((
            JobStatus::Failed,
            AllocationStatus::Failed,
            TerminalResult::SpawnFailed,
        )),
        JobEvent::Finished {
            result: ProcessResult::Exited { exit_code },
            ..
        } => {
            let status = if *exit_code == 0 {
                JobStatus::Succeeded
            } else {
                JobStatus::Failed
            };
            let allocation_status = if *exit_code == 0 {
                AllocationStatus::Succeeded
            } else {
                AllocationStatus::Failed
            };
            Some((
                status,
                allocation_status,
                TerminalResult::Exited {
                    exit_code: *exit_code,
                },
            ))
        }
        JobEvent::Stopped { .. } => Some((
            JobStatus::Stopped,
            AllocationStatus::Stopped,
            TerminalResult::Stopped,
        )),
        JobEvent::Lost { .. } => Some((
            JobStatus::Lost,
            AllocationStatus::Lost,
            TerminalResult::Lost,
        )),
        JobEvent::Submitted { .. } | JobEvent::Assigned { .. } | JobEvent::Started { .. } => None,
    }
}

fn is_terminal_status(status: JobStatus) -> bool {
    matches!(
        status,
        JobStatus::Succeeded | JobStatus::Failed | JobStatus::Stopped | JobStatus::Lost
    )
}

fn is_exact_terminal_duplicate(
    job: &Job,
    allocation: Option<&Allocation>,
    event: &JobEvent,
    (expected_job_status, expected_allocation_status, expected_result): (
        JobStatus,
        AllocationStatus,
        TerminalResult,
    ),
) -> bool {
    let occurred_at = event_occurred_at(event);
    if job.status() != expected_job_status
        || job.updated_at() != occurred_at
        || job.finished_at() != Some(occurred_at)
        || job.terminal_result() != Some(expected_result)
        || job.terminal_allocation() != event_terminal_context(event)
    {
        return false;
    }

    match event_allocation_id(event) {
        Some(_) => allocation.is_some_and(|allocation| {
            allocation.status() == expected_allocation_status
                && allocation.started_at() == job.started_at()
                && allocation.finished_at() == Some(occurred_at)
        }),
        None => allocation.is_none(),
    }
}

fn event_terminal_context(event: &JobEvent) -> Option<(AllocationId, AgentId)> {
    match event {
        JobEvent::Submitted { .. } => None,
        JobEvent::Assigned {
            allocation_id,
            agent_id,
            ..
        }
        | JobEvent::Started {
            allocation_id,
            agent_id,
            ..
        }
        | JobEvent::Finished {
            allocation_id,
            agent_id,
            ..
        }
        | JobEvent::Lost {
            allocation_id,
            agent_id,
            ..
        } => Some((*allocation_id, *agent_id)),
        JobEvent::Stopped {
            allocation_id: Some(allocation_id),
            agent_id: Some(agent_id),
            ..
        } => Some((*allocation_id, *agent_id)),
        JobEvent::Stopped { .. } => None,
    }
}

fn event_name(event: &JobEvent) -> &'static str {
    match event {
        JobEvent::Submitted { .. } => "submitted",
        JobEvent::Assigned { .. } => "assigned",
        JobEvent::Started { .. } => "started",
        JobEvent::Finished { .. } => "finished",
        JobEvent::Stopped { .. } => "stopped",
        JobEvent::Lost { .. } => "lost",
    }
}

struct Transition {
    job_status: JobStatus,
    updated_at: Timestamp,
    started_at: Option<Timestamp>,
    finished_at: Option<Timestamp>,
    terminal_result: Option<TerminalResult>,
    terminal_allocation: Option<(AllocationId, AgentId)>,
    allocation: Option<AllocationUpdate>,
}

struct AllocationUpdate {
    status: AllocationStatus,
    started_at: Option<Timestamp>,
    finished_at: Option<Timestamp>,
}

fn plan_transition(
    job: &Job,
    allocation: Option<&Allocation>,
    event: &JobEvent,
) -> Result<Transition, TransitionError> {
    match event {
        JobEvent::Submitted { .. } => Err(invalid_transition(job, "submitted")),
        JobEvent::Assigned { occurred_at, .. } => {
            if job.status() != JobStatus::Pending {
                return Err(invalid_transition(job, "assigned"));
            }
            let allocation =
                require_allocation(job, allocation, AllocationStatus::Assigned, event)?;
            Ok(Transition {
                job_status: JobStatus::Assigned,
                updated_at: *occurred_at,
                started_at: job.started_at(),
                finished_at: job.finished_at(),
                terminal_result: job.terminal_result(),
                terminal_allocation: job.terminal_allocation(),
                allocation: Some(AllocationUpdate {
                    status: AllocationStatus::Assigned,
                    started_at: allocation.started_at(),
                    finished_at: allocation.finished_at(),
                }),
            })
        }
        JobEvent::Started { occurred_at, .. } => {
            if job.status() != JobStatus::Assigned {
                return Err(invalid_transition(job, "started"));
            }
            let allocation =
                require_allocation(job, allocation, AllocationStatus::Assigned, event)?;
            Ok(Transition {
                job_status: JobStatus::Running,
                updated_at: *occurred_at,
                started_at: Some(*occurred_at),
                finished_at: job.finished_at(),
                terminal_result: job.terminal_result(),
                terminal_allocation: job.terminal_allocation(),
                allocation: Some(AllocationUpdate {
                    status: AllocationStatus::Running,
                    started_at: Some(*occurred_at),
                    finished_at: allocation.finished_at(),
                }),
            })
        }
        JobEvent::Finished {
            occurred_at,
            result: ProcessResult::SpawnFailed,
            ..
        } => {
            if job.status() != JobStatus::Assigned {
                return Err(invalid_transition(job, "finished"));
            }
            let allocation =
                require_allocation(job, allocation, AllocationStatus::Assigned, event)?;
            Ok(terminal_transition(
                job,
                allocation,
                *occurred_at,
                JobStatus::Failed,
                AllocationStatus::Failed,
                TerminalResult::SpawnFailed,
            ))
        }
        JobEvent::Finished {
            occurred_at,
            result: ProcessResult::Exited { exit_code },
            ..
        } => {
            if job.status() != JobStatus::Running {
                return Err(invalid_transition(job, "finished"));
            }
            let allocation = require_allocation(job, allocation, AllocationStatus::Running, event)?;
            let (job_status, allocation_status) = if *exit_code == 0 {
                (JobStatus::Succeeded, AllocationStatus::Succeeded)
            } else {
                (JobStatus::Failed, AllocationStatus::Failed)
            };
            Ok(terminal_transition(
                job,
                allocation,
                *occurred_at,
                job_status,
                allocation_status,
                TerminalResult::Exited {
                    exit_code: *exit_code,
                },
            ))
        }
        JobEvent::Stopped {
            allocation_id,
            agent_id,
            occurred_at,
            ..
        } => match job.status() {
            JobStatus::Pending if allocation_id.is_none() && agent_id.is_none() => {
                if allocation.is_some() {
                    return Err(invalid_transition(job, "stopped"));
                }
                Ok(Transition {
                    job_status: JobStatus::Stopped,
                    updated_at: *occurred_at,
                    started_at: job.started_at(),
                    finished_at: Some(*occurred_at),
                    terminal_result: Some(TerminalResult::Stopped),
                    terminal_allocation: None,
                    allocation: None,
                })
            }
            JobStatus::Assigned | JobStatus::Running
                if allocation_id.is_some() && agent_id.is_some() =>
            {
                let expected_status = if job.status() == JobStatus::Assigned {
                    AllocationStatus::Assigned
                } else {
                    AllocationStatus::Running
                };
                let allocation = require_allocation(job, allocation, expected_status, event)?;
                Ok(terminal_transition(
                    job,
                    allocation,
                    *occurred_at,
                    JobStatus::Stopped,
                    AllocationStatus::Stopped,
                    TerminalResult::Stopped,
                ))
            }
            _ => Err(invalid_transition(job, "stopped")),
        },
        JobEvent::Lost { occurred_at, .. } => match job.status() {
            JobStatus::Assigned | JobStatus::Running => {
                let expected_status = if job.status() == JobStatus::Assigned {
                    AllocationStatus::Assigned
                } else {
                    AllocationStatus::Running
                };
                let allocation = require_allocation(job, allocation, expected_status, event)?;
                Ok(terminal_transition(
                    job,
                    allocation,
                    *occurred_at,
                    JobStatus::Lost,
                    AllocationStatus::Lost,
                    TerminalResult::Lost,
                ))
            }
            _ => Err(invalid_transition(job, "lost")),
        },
    }
}

fn require_allocation<'a>(
    job: &Job,
    allocation: Option<&'a Allocation>,
    expected_status: AllocationStatus,
    event: &JobEvent,
) -> Result<&'a Allocation, TransitionError> {
    let allocation = allocation.ok_or_else(|| TransitionError::AllocationMissing {
        allocation_id: event_allocation_id(event),
    })?;
    if allocation.status() != expected_status {
        return Err(TransitionError::InconsistentState {
            job_status: job.status(),
            allocation_status: allocation.status(),
        });
    }
    Ok(allocation)
}

fn terminal_transition(
    job: &Job,
    allocation: &Allocation,
    occurred_at: Timestamp,
    job_status: JobStatus,
    allocation_status: AllocationStatus,
    terminal_result: TerminalResult,
) -> Transition {
    Transition {
        job_status,
        updated_at: occurred_at,
        started_at: job.started_at(),
        finished_at: Some(occurred_at),
        terminal_result: Some(terminal_result),
        terminal_allocation: Some((allocation.id(), allocation.agent_id())),
        allocation: Some(AllocationUpdate {
            status: allocation_status,
            started_at: allocation.started_at(),
            finished_at: Some(occurred_at),
        }),
    }
}

fn event_allocation_id(event: &JobEvent) -> Option<AllocationId> {
    match event {
        JobEvent::Assigned { allocation_id, .. }
        | JobEvent::Started { allocation_id, .. }
        | JobEvent::Finished { allocation_id, .. }
        | JobEvent::Lost { allocation_id, .. } => Some(*allocation_id),
        JobEvent::Stopped { allocation_id, .. } => *allocation_id,
        JobEvent::Submitted { .. } => None,
    }
}

fn invalid_transition(job: &Job, event: &'static str) -> TransitionError {
    TransitionError::InvalidTransition {
        job_status: job.status(),
        event,
    }
}

#[cfg(test)]
mod tests {
    use super::{ApplyOutcome, apply_event};
    use crate::domain::{
        AgentId, Allocation, AllocationId, AllocationStatus, Job, JobEvent, JobId, JobSpec,
        JobStatus, LostReason, ProcessResult, StopReason, TerminalResult, Timestamp,
    };
    use std::num::NonZeroU32;

    const SUBMITTED_AT: u64 = 1_742_000_000_100;
    const ASSIGNED_AT: u64 = 1_742_000_000_200;
    const STARTED_AT: u64 = 1_742_000_000_300;
    const FINISHED_AT: u64 = 1_742_000_000_400;

    fn timestamp(value: u64) -> Timestamp {
        Timestamp::from_unix_millis(value)
    }

    fn fixture() -> (Job, Allocation) {
        let job_id = "550e8400-e29b-41d4-a716-446655440000"
            .parse::<JobId>()
            .expect("fixed job id should parse");
        let agent_id = "550e8400-e29b-41d4-a716-446655440001"
            .parse::<AgentId>()
            .expect("fixed agent id should parse");
        let allocation_id = "550e8400-e29b-41d4-a716-446655440002"
            .parse::<AllocationId>()
            .expect("fixed allocation id should parse");
        let job = Job::new(
            job_id,
            JobSpec::new("echo", vec!["hello".into()], None, None)
                .expect("fixed job spec should be valid"),
            timestamp(SUBMITTED_AT),
        );
        let allocation = Allocation::new(
            allocation_id,
            job_id,
            agent_id,
            NonZeroU32::new(1).expect("one is nonzero"),
            timestamp(ASSIGNED_AT),
        );

        (job, allocation)
    }

    fn apply_with_allocation(job: &mut Job, allocation: &mut Allocation, event: JobEvent) {
        assert_eq!(
            apply_event(job, Some(allocation), &event).expect("valid event should be accepted"),
            ApplyOutcome::Applied
        );
    }

    fn assign(job: &mut Job, allocation: &mut Allocation) {
        apply_with_allocation(
            job,
            allocation,
            JobEvent::Assigned {
                job_id: job.id(),
                allocation_id: allocation.id(),
                agent_id: allocation.agent_id(),
                occurred_at: timestamp(ASSIGNED_AT),
            },
        );
    }

    fn start(job: &mut Job, allocation: &mut Allocation) {
        apply_with_allocation(
            job,
            allocation,
            JobEvent::Started {
                job_id: job.id(),
                allocation_id: allocation.id(),
                agent_id: allocation.agent_id(),
                occurred_at: timestamp(STARTED_AT),
            },
        );
    }

    fn finish(job: &mut Job, allocation: &mut Allocation, occurred_at: u64, result: ProcessResult) {
        apply_with_allocation(
            job,
            allocation,
            JobEvent::Finished {
                job_id: job.id(),
                allocation_id: allocation.id(),
                agent_id: allocation.agent_id(),
                occurred_at: timestamp(occurred_at),
                result,
            },
        );
    }

    fn put_job_in_state(target: JobStatus, job: &mut Job, allocation: &mut Allocation) {
        match target {
            JobStatus::Pending => {}
            JobStatus::Assigned => assign(job, allocation),
            JobStatus::Running => {
                assign(job, allocation);
                start(job, allocation);
            }
            JobStatus::Succeeded => {
                assign(job, allocation);
                start(job, allocation);
                finish(
                    job,
                    allocation,
                    FINISHED_AT,
                    ProcessResult::Exited { exit_code: 0 },
                );
            }
            JobStatus::Failed => {
                assign(job, allocation);
                finish(job, allocation, STARTED_AT, ProcessResult::SpawnFailed);
            }
            JobStatus::Stopped => {
                let event = JobEvent::Stopped {
                    job_id: job.id(),
                    allocation_id: None,
                    agent_id: None,
                    occurred_at: timestamp(STARTED_AT),
                    reason: StopReason::UserRequested,
                };
                assert_eq!(
                    apply_event(job, None, &event).expect("pending stop should be accepted"),
                    ApplyOutcome::Applied
                );
            }
            JobStatus::Lost => {
                assign(job, allocation);
                apply_with_allocation(
                    job,
                    allocation,
                    JobEvent::Lost {
                        job_id: job.id(),
                        allocation_id: allocation.id(),
                        agent_id: allocation.agent_id(),
                        occurred_at: timestamp(STARTED_AT),
                        reason: LostReason::HeartbeatExpired,
                    },
                );
            }
        }
    }

    #[test]
    fn submitted_event_is_a_noop_for_all_job_states() {
        let states = [
            JobStatus::Pending,
            JobStatus::Assigned,
            JobStatus::Running,
            JobStatus::Succeeded,
            JobStatus::Failed,
            JobStatus::Stopped,
            JobStatus::Lost,
        ];

        for state in states {
            let (mut job, mut allocation) = fixture();
            put_job_in_state(state, &mut job, &mut allocation);
            assert_eq!(job.status(), state);
            let before = job.clone();
            let event = JobEvent::Submitted {
                job_id: job.id(),
                occurred_at: timestamp(SUBMITTED_AT),
            };

            assert_eq!(
                apply_event(&mut job, None, &event)
                    .expect("original submitted fact should be a no-op"),
                ApplyOutcome::Noop
            );
            assert_eq!(job, before);
        }
    }

    #[test]
    fn applies_assignment_to_pending_job() {
        let (mut job, mut allocation) = fixture();

        assign(&mut job, &mut allocation);

        assert_eq!(job.status(), JobStatus::Assigned);
        assert_eq!(job.updated_at(), timestamp(ASSIGNED_AT));
        assert_eq!(job.started_at(), None);
        assert_eq!(job.finished_at(), None);
        assert_eq!(job.terminal_result(), None);
        assert_eq!(allocation.status(), AllocationStatus::Assigned);
        assert_eq!(allocation.started_at(), None);
        assert_eq!(allocation.finished_at(), None);
    }

    #[test]
    fn starts_assigned_job_and_allocation() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);

        start(&mut job, &mut allocation);

        assert_eq!(job.status(), JobStatus::Running);
        assert_eq!(job.updated_at(), timestamp(STARTED_AT));
        assert_eq!(job.started_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(job.finished_at(), None);
        assert_eq!(job.terminal_result(), None);
        assert_eq!(allocation.status(), AllocationStatus::Running);
        assert_eq!(allocation.started_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(allocation.finished_at(), None);
    }

    #[test]
    fn spawn_failure_fails_assigned_entities_without_started() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);

        finish(
            &mut job,
            &mut allocation,
            STARTED_AT,
            ProcessResult::SpawnFailed,
        );

        assert_eq!(job.status(), JobStatus::Failed);
        assert_eq!(job.updated_at(), timestamp(STARTED_AT));
        assert_eq!(job.started_at(), None);
        assert_eq!(job.finished_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(job.terminal_result(), Some(TerminalResult::SpawnFailed));
        assert_eq!(allocation.status(), AllocationStatus::Failed);
        assert_eq!(allocation.started_at(), None);
        assert_eq!(allocation.finished_at(), Some(timestamp(STARTED_AT)));
    }

    #[test]
    fn maps_zero_exit_to_succeeded() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        start(&mut job, &mut allocation);

        finish(
            &mut job,
            &mut allocation,
            FINISHED_AT,
            ProcessResult::Exited { exit_code: 0 },
        );

        assert_eq!(job.status(), JobStatus::Succeeded);
        assert_eq!(job.updated_at(), timestamp(FINISHED_AT));
        assert_eq!(job.started_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(job.finished_at(), Some(timestamp(FINISHED_AT)));
        assert_eq!(
            job.terminal_result(),
            Some(TerminalResult::Exited { exit_code: 0 })
        );
        assert_eq!(allocation.status(), AllocationStatus::Succeeded);
        assert_eq!(allocation.started_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(allocation.finished_at(), Some(timestamp(FINISHED_AT)));
    }

    #[test]
    fn maps_nonzero_exit_to_failed() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        start(&mut job, &mut allocation);

        finish(
            &mut job,
            &mut allocation,
            FINISHED_AT,
            ProcessResult::Exited { exit_code: 17 },
        );

        assert_eq!(job.status(), JobStatus::Failed);
        assert_eq!(job.updated_at(), timestamp(FINISHED_AT));
        assert_eq!(job.started_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(job.finished_at(), Some(timestamp(FINISHED_AT)));
        assert_eq!(
            job.terminal_result(),
            Some(TerminalResult::Exited { exit_code: 17 })
        );
        assert_eq!(allocation.status(), AllocationStatus::Failed);
        assert_eq!(allocation.started_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(allocation.finished_at(), Some(timestamp(FINISHED_AT)));
    }

    #[test]
    fn stops_pending_job_without_allocation() {
        let (mut job, _) = fixture();
        let event = JobEvent::Stopped {
            job_id: job.id(),
            allocation_id: None,
            agent_id: None,
            occurred_at: timestamp(ASSIGNED_AT),
            reason: StopReason::UserRequested,
        };

        assert_eq!(
            apply_event(&mut job, None, &event)
                .expect("pending stop without allocation should be accepted"),
            ApplyOutcome::Applied
        );

        assert_eq!(job.status(), JobStatus::Stopped);
        assert_eq!(job.updated_at(), timestamp(ASSIGNED_AT));
        assert_eq!(job.started_at(), None);
        assert_eq!(job.finished_at(), Some(timestamp(ASSIGNED_AT)));
        assert_eq!(job.terminal_result(), Some(TerminalResult::Stopped));
    }

    #[test]
    fn stops_assigned_allocation() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        let event = JobEvent::Stopped {
            job_id: job.id(),
            allocation_id: Some(allocation.id()),
            agent_id: Some(allocation.agent_id()),
            occurred_at: timestamp(STARTED_AT),
            reason: StopReason::UserRequested,
        };

        assert_eq!(
            apply_event(&mut job, Some(&mut allocation), &event)
                .expect("assigned stop should be accepted"),
            ApplyOutcome::Applied
        );

        assert_eq!(job.status(), JobStatus::Stopped);
        assert_eq!(job.updated_at(), timestamp(STARTED_AT));
        assert_eq!(job.started_at(), None);
        assert_eq!(job.finished_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(job.terminal_result(), Some(TerminalResult::Stopped));
        assert_eq!(allocation.status(), AllocationStatus::Stopped);
        assert_eq!(allocation.started_at(), None);
        assert_eq!(allocation.finished_at(), Some(timestamp(STARTED_AT)));
    }

    #[test]
    fn stops_running_allocation() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        start(&mut job, &mut allocation);
        let event = JobEvent::Stopped {
            job_id: job.id(),
            allocation_id: Some(allocation.id()),
            agent_id: Some(allocation.agent_id()),
            occurred_at: timestamp(FINISHED_AT),
            reason: StopReason::UserRequested,
        };

        assert_eq!(
            apply_event(&mut job, Some(&mut allocation), &event)
                .expect("running stop should be accepted"),
            ApplyOutcome::Applied
        );

        assert_eq!(job.status(), JobStatus::Stopped);
        assert_eq!(job.updated_at(), timestamp(FINISHED_AT));
        assert_eq!(job.started_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(job.finished_at(), Some(timestamp(FINISHED_AT)));
        assert_eq!(job.terminal_result(), Some(TerminalResult::Stopped));
        assert_eq!(allocation.status(), AllocationStatus::Stopped);
        assert_eq!(allocation.started_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(allocation.finished_at(), Some(timestamp(FINISHED_AT)));
    }

    #[test]
    fn marks_assigned_allocation_lost() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        let event = JobEvent::Lost {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(STARTED_AT),
            reason: LostReason::HeartbeatExpired,
        };

        assert_eq!(
            apply_event(&mut job, Some(&mut allocation), &event)
                .expect("assigned allocation loss should be accepted"),
            ApplyOutcome::Applied
        );

        assert_eq!(job.status(), JobStatus::Lost);
        assert_eq!(job.updated_at(), timestamp(STARTED_AT));
        assert_eq!(job.started_at(), None);
        assert_eq!(job.finished_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(job.terminal_result(), Some(TerminalResult::Lost));
        assert_eq!(allocation.status(), AllocationStatus::Lost);
        assert_eq!(allocation.started_at(), None);
        assert_eq!(allocation.finished_at(), Some(timestamp(STARTED_AT)));
    }

    #[test]
    fn marks_running_allocation_lost() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        start(&mut job, &mut allocation);
        let event = JobEvent::Lost {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(FINISHED_AT),
            reason: LostReason::HeartbeatExpired,
        };

        assert_eq!(
            apply_event(&mut job, Some(&mut allocation), &event)
                .expect("running allocation loss should be accepted"),
            ApplyOutcome::Applied
        );

        assert_eq!(job.status(), JobStatus::Lost);
        assert_eq!(job.updated_at(), timestamp(FINISHED_AT));
        assert_eq!(job.started_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(job.finished_at(), Some(timestamp(FINISHED_AT)));
        assert_eq!(job.terminal_result(), Some(TerminalResult::Lost));
        assert_eq!(allocation.status(), AllocationStatus::Lost);
        assert_eq!(allocation.started_at(), Some(timestamp(STARTED_AT)));
        assert_eq!(allocation.finished_at(), Some(timestamp(FINISHED_AT)));
    }

    #[test]
    fn exited_from_assigned_is_rejected_without_mutation() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        let before_job = job.clone();
        let before_allocation = allocation.clone();
        let event = JobEvent::Finished {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(STARTED_AT),
            result: ProcessResult::Exited { exit_code: 0 },
        };

        let error = apply_event(&mut job, Some(&mut allocation), &event)
            .expect_err("exited requires a running job");

        assert_eq!(error.code(), "invalid_transition");
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum EventCase {
        Assigned,
        Started,
        SpawnFailed,
        Exited,
        StoppedWithoutContext,
        StoppedWithContext,
        Lost,
    }

    fn event_for_case(case: EventCase, job: &Job, allocation: &Allocation) -> JobEvent {
        match case {
            EventCase::Assigned => JobEvent::Assigned {
                job_id: job.id(),
                allocation_id: allocation.id(),
                agent_id: allocation.agent_id(),
                occurred_at: timestamp(FINISHED_AT),
            },
            EventCase::Started => JobEvent::Started {
                job_id: job.id(),
                allocation_id: allocation.id(),
                agent_id: allocation.agent_id(),
                occurred_at: timestamp(FINISHED_AT),
            },
            EventCase::SpawnFailed => JobEvent::Finished {
                job_id: job.id(),
                allocation_id: allocation.id(),
                agent_id: allocation.agent_id(),
                occurred_at: timestamp(FINISHED_AT),
                result: ProcessResult::SpawnFailed,
            },
            EventCase::Exited => JobEvent::Finished {
                job_id: job.id(),
                allocation_id: allocation.id(),
                agent_id: allocation.agent_id(),
                occurred_at: timestamp(FINISHED_AT),
                result: ProcessResult::Exited { exit_code: 1 },
            },
            EventCase::StoppedWithoutContext => JobEvent::Stopped {
                job_id: job.id(),
                allocation_id: None,
                agent_id: None,
                occurred_at: timestamp(FINISHED_AT),
                reason: StopReason::UserRequested,
            },
            EventCase::StoppedWithContext => JobEvent::Stopped {
                job_id: job.id(),
                allocation_id: Some(allocation.id()),
                agent_id: Some(allocation.agent_id()),
                occurred_at: timestamp(FINISHED_AT),
                reason: StopReason::UserRequested,
            },
            EventCase::Lost => JobEvent::Lost {
                job_id: job.id(),
                allocation_id: allocation.id(),
                agent_id: allocation.agent_id(),
                occurred_at: timestamp(FINISHED_AT),
                reason: LostReason::HeartbeatExpired,
            },
        }
    }

    #[test]
    fn rejects_job_id_mismatch_without_mutation() {
        let (mut job, mut allocation) = fixture();
        let before_job = job.clone();
        let before_allocation = allocation.clone();
        let wrong_job_id = "550e8400-e29b-41d4-a716-446655440003"
            .parse::<JobId>()
            .expect("fixed wrong job id should parse");
        let event = JobEvent::Assigned {
            job_id: wrong_job_id,
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT),
        };

        let error = apply_event(&mut job, Some(&mut allocation), &event)
            .expect_err("an event for another job must be rejected");

        assert_eq!(error.code(), "job_id_mismatch");
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[test]
    fn rejects_missing_allocation() {
        let (mut job, allocation) = fixture();
        let before_job = job.clone();
        let event = JobEvent::Assigned {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT),
        };

        let error =
            apply_event(&mut job, None, &event).expect_err("assignment requires its allocation");

        assert_eq!(error.code(), "allocation_missing");
        assert_eq!(job, before_job);
    }

    #[test]
    fn rejects_allocation_id_mismatch_without_mutation() {
        let (mut job, mut allocation) = fixture();
        let before_job = job.clone();
        let before_allocation = allocation.clone();
        let wrong_allocation_id = "550e8400-e29b-41d4-a716-446655440003"
            .parse::<AllocationId>()
            .expect("fixed wrong allocation id should parse");
        let event = JobEvent::Assigned {
            job_id: job.id(),
            allocation_id: wrong_allocation_id,
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT),
        };

        let error = apply_event(&mut job, Some(&mut allocation), &event)
            .expect_err("an event for another allocation must be rejected");

        assert_eq!(error.code(), "allocation_mismatch");
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[test]
    fn rejects_agent_id_mismatch_without_mutation() {
        let (mut job, mut allocation) = fixture();
        let before_job = job.clone();
        let before_allocation = allocation.clone();
        let wrong_agent_id = "550e8400-e29b-41d4-a716-446655440003"
            .parse::<AgentId>()
            .expect("fixed wrong agent id should parse");
        let event = JobEvent::Assigned {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: wrong_agent_id,
            occurred_at: timestamp(ASSIGNED_AT),
        };

        let error = apply_event(&mut job, Some(&mut allocation), &event)
            .expect_err("an event claiming another agent must be rejected");

        assert_eq!(error.code(), "agent_mismatch");
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[test]
    fn rejects_allocation_link_mismatch() {
        let (mut job, allocation) = fixture();
        let wrong_job_id = "550e8400-e29b-41d4-a716-446655440003"
            .parse::<JobId>()
            .expect("fixed wrong job id should parse");
        let mut mislinked_allocation = Allocation::new(
            allocation.id(),
            wrong_job_id,
            allocation.agent_id(),
            allocation.attempt(),
            allocation.assigned_at(),
        );
        let before_job = job.clone();
        let before_allocation = mislinked_allocation.clone();
        let event = JobEvent::Assigned {
            job_id: job.id(),
            allocation_id: mislinked_allocation.id(),
            agent_id: mislinked_allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT),
        };

        let error = apply_event(&mut job, Some(&mut mislinked_allocation), &event)
            .expect_err("allocation linked to another job must be rejected");

        assert_eq!(error.code(), "allocation_mismatch");
        assert_eq!(job, before_job);
        assert_eq!(mislinked_allocation, before_allocation);
    }

    #[test]
    fn rejects_inconsistent_job_and_allocation_statuses() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        start(&mut job, &mut allocation);
        allocation = Allocation::new(
            allocation.id(),
            allocation.job_id(),
            allocation.agent_id(),
            allocation.attempt(),
            allocation.assigned_at(),
        );
        let before_job = job.clone();
        let before_allocation = allocation.clone();
        let event = JobEvent::Lost {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(FINISHED_AT),
            reason: LostReason::HeartbeatExpired,
        };

        let error = apply_event(&mut job, Some(&mut allocation), &event)
            .expect_err("running job with assigned allocation is inconsistent");

        assert_eq!(error.code(), "inconsistent_state");
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[test]
    fn rejects_disallowed_nonterminal_state_event_pairs() {
        let cases = [
            EventCase::Assigned,
            EventCase::Started,
            EventCase::SpawnFailed,
            EventCase::Exited,
            EventCase::StoppedWithoutContext,
            EventCase::StoppedWithContext,
            EventCase::Lost,
        ];
        let allowed_pairs = [
            (JobStatus::Pending, EventCase::Assigned),
            (JobStatus::Pending, EventCase::StoppedWithoutContext),
            (JobStatus::Assigned, EventCase::Started),
            (JobStatus::Assigned, EventCase::SpawnFailed),
            (JobStatus::Assigned, EventCase::StoppedWithContext),
            (JobStatus::Assigned, EventCase::Lost),
            (JobStatus::Running, EventCase::Exited),
            (JobStatus::Running, EventCase::StoppedWithContext),
            (JobStatus::Running, EventCase::Lost),
        ];

        for status in [JobStatus::Pending, JobStatus::Assigned, JobStatus::Running] {
            for case in cases {
                if allowed_pairs.contains(&(status, case)) {
                    continue;
                }

                let (mut job, mut allocation) = fixture();
                put_job_in_state(status, &mut job, &mut allocation);
                let event = event_for_case(case, &job, &allocation);
                let before_job = job.clone();
                let before_allocation = allocation.clone();
                let result = if case == EventCase::StoppedWithoutContext {
                    apply_event(&mut job, None, &event)
                } else {
                    apply_event(&mut job, Some(&mut allocation), &event)
                };

                let error = result.expect_err("disallowed state/event pair must be rejected");

                assert_eq!(error.code(), "invalid_transition", "{status:?} / {case:?}");
                assert_eq!(job, before_job, "{status:?} / {case:?}");
                assert_eq!(allocation, before_allocation, "{status:?} / {case:?}");
            }
        }
    }

    #[test]
    fn rejects_event_older_than_job_without_mutation() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        let before_job = job.clone();
        let before_allocation = allocation.clone();
        let event = JobEvent::Started {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT - 1),
        };

        let error = apply_event(&mut job, Some(&mut allocation), &event)
            .expect_err("event older than job update must be rejected");

        assert_eq!(error.code(), "stale_event");
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[test]
    fn rejects_event_older_than_allocation_without_mutation() {
        let (mut job, mut allocation) = fixture();
        let before_job = job.clone();
        let before_allocation = allocation.clone();
        let event = JobEvent::Assigned {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT - 1),
        };

        let error = apply_event(&mut job, Some(&mut allocation), &event)
            .expect_err("assignment older than allocation creation must be rejected");

        assert_eq!(error.code(), "stale_event");
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[test]
    fn accepts_timestamp_equal_to_job_and_allocation() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        let event = JobEvent::Started {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT),
        };

        assert_eq!(
            apply_event(&mut job, Some(&mut allocation), &event)
                .expect("equal timestamps should be accepted"),
            ApplyOutcome::Applied
        );
        assert_eq!(job.updated_at(), timestamp(ASSIGNED_AT));
        assert_eq!(job.started_at(), Some(timestamp(ASSIGNED_AT)));
        assert_eq!(allocation.started_at(), Some(timestamp(ASSIGNED_AT)));
    }

    #[test]
    fn uses_assigned_at_for_assignment_start_and_spawn_failure() {
        let (mut job, mut allocation) = fixture();
        let assign_before_allocation = JobEvent::Assigned {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT - 1),
        };
        let error = apply_event(&mut job, Some(&mut allocation), &assign_before_allocation)
            .expect_err("assignment must not predate allocation");
        assert_eq!(error.code(), "stale_event");

        assign(&mut job, &mut allocation);
        let started_before_allocation = JobEvent::Started {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT - 1),
        };
        let error = apply_event(&mut job, Some(&mut allocation), &started_before_allocation)
            .expect_err("start must not predate allocation");
        assert_eq!(error.code(), "stale_event");

        let spawn_failed_before_allocation = JobEvent::Finished {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT - 1),
            result: ProcessResult::SpawnFailed,
        };
        let error = apply_event(
            &mut job,
            Some(&mut allocation),
            &spawn_failed_before_allocation,
        )
        .expect_err("spawn failure must not predate allocation");
        assert_eq!(error.code(), "stale_event");
        assert_eq!(job.status(), JobStatus::Assigned);
        assert_eq!(allocation.status(), AllocationStatus::Assigned);
    }

    #[test]
    fn uses_started_at_for_exited_result() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        start(&mut job, &mut allocation);
        let before_job = job.clone();
        let before_allocation = allocation.clone();
        let event = JobEvent::Finished {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(STARTED_AT - 1),
            result: ProcessResult::Exited { exit_code: 0 },
        };

        let error = apply_event(&mut job, Some(&mut allocation), &event)
            .expect_err("exit result must not predate process start");

        assert_eq!(error.code(), "stale_event");
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[test]
    fn uses_started_or_assigned_at_for_stop_and_lost() {
        let (mut assigned_job, mut assigned_allocation) = fixture();
        assign(&mut assigned_job, &mut assigned_allocation);
        let stop_event = JobEvent::Stopped {
            job_id: assigned_job.id(),
            allocation_id: Some(assigned_allocation.id()),
            agent_id: Some(assigned_allocation.agent_id()),
            occurred_at: timestamp(ASSIGNED_AT - 1),
            reason: StopReason::UserRequested,
        };
        let error = apply_event(
            &mut assigned_job,
            Some(&mut assigned_allocation),
            &stop_event,
        )
        .expect_err("assigned stop must not predate assignment");
        assert_eq!(error.code(), "stale_event");

        let lost_event = JobEvent::Lost {
            job_id: assigned_job.id(),
            allocation_id: assigned_allocation.id(),
            agent_id: assigned_allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT - 1),
            reason: LostReason::HeartbeatExpired,
        };
        let error = apply_event(
            &mut assigned_job,
            Some(&mut assigned_allocation),
            &lost_event,
        )
        .expect_err("assigned loss must not predate assignment");
        assert_eq!(error.code(), "stale_event");

        let (mut running_job, mut running_allocation) = fixture();
        assign(&mut running_job, &mut running_allocation);
        start(&mut running_job, &mut running_allocation);
        let stop_event = JobEvent::Stopped {
            job_id: running_job.id(),
            allocation_id: Some(running_allocation.id()),
            agent_id: Some(running_allocation.agent_id()),
            occurred_at: timestamp(STARTED_AT - 1),
            reason: StopReason::UserRequested,
        };
        let error = apply_event(&mut running_job, Some(&mut running_allocation), &stop_event)
            .expect_err("running stop must not predate process start");
        assert_eq!(error.code(), "stale_event");

        let lost_event = JobEvent::Lost {
            job_id: running_job.id(),
            allocation_id: running_allocation.id(),
            agent_id: running_allocation.agent_id(),
            occurred_at: timestamp(STARTED_AT - 1),
            reason: LostReason::HeartbeatExpired,
        };
        let error = apply_event(&mut running_job, Some(&mut running_allocation), &lost_event)
            .expect_err("running loss must not predate process start");
        assert_eq!(error.code(), "stale_event");
    }

    #[test]
    fn returns_duplicate_for_exact_terminal_event_without_rewriting_timestamps() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        start(&mut job, &mut allocation);
        let event = JobEvent::Finished {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(FINISHED_AT),
            result: ProcessResult::Exited { exit_code: 0 },
        };
        apply_event(&mut job, Some(&mut allocation), &event)
            .expect("first finish should be applied");
        let before_job = job.clone();
        let before_allocation = allocation.clone();

        assert_eq!(
            apply_event(&mut job, Some(&mut allocation), &event)
                .expect("exact terminal repeat should be accepted as duplicate"),
            ApplyOutcome::Duplicate
        );
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[test]
    fn pending_stop_replay_is_duplicate_without_allocation() {
        let (mut job, _) = fixture();
        let event = JobEvent::Stopped {
            job_id: job.id(),
            allocation_id: None,
            agent_id: None,
            occurred_at: timestamp(STARTED_AT),
            reason: StopReason::UserRequested,
        };
        apply_event(&mut job, None, &event).expect("pending stop should be applied");
        let before_job = job.clone();

        assert_eq!(
            apply_event(&mut job, None, &event)
                .expect("exact pending stop replay should be duplicate"),
            ApplyOutcome::Duplicate
        );
        assert_eq!(job, before_job);
    }

    #[test]
    fn stopped_event_with_partial_context_is_rejected() {
        let (mut job, allocation) = fixture();
        let pending_stop = JobEvent::Stopped {
            job_id: job.id(),
            allocation_id: None,
            agent_id: None,
            occurred_at: timestamp(STARTED_AT),
            reason: StopReason::UserRequested,
        };
        apply_event(&mut job, None, &pending_stop).expect("pending stop should be applied");
        let before_job = job.clone();
        let event_with_partial_context = JobEvent::Stopped {
            job_id: job.id(),
            allocation_id: None,
            agent_id: Some(allocation.agent_id()),
            occurred_at: timestamp(STARTED_AT),
            reason: StopReason::UserRequested,
        };

        let error = apply_event(&mut job, None, &event_with_partial_context)
            .expect_err("a stopped event must include both context IDs or neither");

        assert_eq!(error.code(), "invalid_transition");
        assert_eq!(job, before_job);
    }

    #[test]
    fn allocated_stop_duplicate_requires_the_recorded_context() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        let event = JobEvent::Stopped {
            job_id: job.id(),
            allocation_id: Some(allocation.id()),
            agent_id: Some(allocation.agent_id()),
            occurred_at: timestamp(STARTED_AT),
            reason: StopReason::UserRequested,
        };
        apply_event(&mut job, Some(&mut allocation), &event)
            .expect("assigned stop should be applied");
        let before_job = job.clone();
        let before_allocation = allocation.clone();

        assert_eq!(
            apply_event(&mut job, Some(&mut allocation), &event)
                .expect("exact allocated stop replay should be duplicate"),
            ApplyOutcome::Duplicate
        );
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);

        let event_without_context = JobEvent::Stopped {
            job_id: job.id(),
            allocation_id: None,
            agent_id: None,
            occurred_at: timestamp(STARTED_AT),
            reason: StopReason::UserRequested,
        };
        let error = apply_event(&mut job, None, &event_without_context)
            .expect_err("stop replay must carry the original allocation context");

        assert_eq!(error.code(), "terminal_conflict");
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[test]
    fn rejects_conflicting_terminal_result() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        start(&mut job, &mut allocation);
        finish(
            &mut job,
            &mut allocation,
            FINISHED_AT,
            ProcessResult::Exited { exit_code: 0 },
        );
        let before_job = job.clone();
        let before_allocation = allocation.clone();
        let event = JobEvent::Finished {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(FINISHED_AT),
            result: ProcessResult::Exited { exit_code: 1 },
        };

        let error = apply_event(&mut job, Some(&mut allocation), &event)
            .expect_err("different exit result must conflict with the terminal result");

        assert_eq!(error.code(), "terminal_conflict");
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[test]
    fn rejects_terminal_event_with_different_time() {
        let (mut job, mut allocation) = fixture();
        assign(&mut job, &mut allocation);
        start(&mut job, &mut allocation);
        finish(
            &mut job,
            &mut allocation,
            FINISHED_AT,
            ProcessResult::Exited { exit_code: 0 },
        );
        let before_job = job.clone();
        let before_allocation = allocation.clone();
        let event = JobEvent::Finished {
            job_id: job.id(),
            allocation_id: allocation.id(),
            agent_id: allocation.agent_id(),
            occurred_at: timestamp(FINISHED_AT + 1),
            result: ProcessResult::Exited { exit_code: 0 },
        };

        let error = apply_event(&mut job, Some(&mut allocation), &event)
            .expect_err("same terminal outcome at a different time must conflict");

        assert_eq!(error.code(), "terminal_conflict");
        assert_eq!(job, before_job);
        assert_eq!(allocation, before_allocation);
    }

    #[test]
    fn submitted_event_requires_original_timestamp() {
        let (mut job, _) = fixture();
        let before_job = job.clone();
        let event = JobEvent::Submitted {
            job_id: job.id(),
            occurred_at: timestamp(SUBMITTED_AT + 1),
        };

        let error = apply_event(&mut job, None, &event)
            .expect_err("submitted event must match original timestamp");

        assert_eq!(error.code(), "invalid_transition");
        assert_eq!(job, before_job);
    }

    #[test]
    fn old_allocation_callback_cannot_update_replacement_allocation() {
        let (mut job, old_allocation) = fixture();
        let replacement_id = "550e8400-e29b-41d4-a716-446655440004"
            .parse::<AllocationId>()
            .expect("fixed replacement allocation id should parse");
        let mut replacement = Allocation::new(
            replacement_id,
            old_allocation.job_id(),
            old_allocation.agent_id(),
            old_allocation.attempt(),
            old_allocation.assigned_at(),
        );
        let before_job = job.clone();
        let before_replacement = replacement.clone();
        let event = JobEvent::Assigned {
            job_id: job.id(),
            allocation_id: old_allocation.id(),
            agent_id: old_allocation.agent_id(),
            occurred_at: timestamp(ASSIGNED_AT),
        };

        let error = apply_event(&mut job, Some(&mut replacement), &event)
            .expect_err("callback from an old allocation must be rejected");

        assert_eq!(error.code(), "allocation_mismatch");
        assert_eq!(job, before_job);
        assert_eq!(replacement, before_replacement);
    }
}
