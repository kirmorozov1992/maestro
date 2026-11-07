//! Execute one assignment with explicit ownership across cancelled HTTP/wait futures.
use crate::{
    domain::{AgentId, AllocationId, JobId, Timestamp},
    transport::v1::{
        AllocationEventRequest, AssignmentResponse, EventOutcomeV1, JobSpecResponse,
        ProcessResultV1, StopReasonV1,
    },
    worker::{
        client::AgentHttpClient,
        executor::{Executor, ExecutorError, RunningProcess},
    },
};
use std::{
    future::{Future, pending},
    process::ExitStatus,
    time::Duration,
};
use tokio::time::Instant;

pub(super) trait AssignmentHandler: Send {
    fn handle(
        &mut self,
        client: &AgentHttpClient,
        assignment: AssignmentResponse,
    ) -> impl Future<Output = Result<(), ExecutorError>> + Send;
    fn shutdown(
        &mut self,
        _client: &AgentHttpClient,
    ) -> impl Future<Output = Result<(), ExecutorError>> + Send {
        async { Ok(()) }
    }
}

// Existing polling tests inject consumers without process ownership.
#[cfg(test)]
impl<Handle, Handling> AssignmentHandler for Handle
where
    Handle: FnMut(AssignmentResponse) -> Handling + Send,
    Handling: Future<Output = ()> + Send,
{
    async fn handle(
        &mut self,
        _client: &AgentHttpClient,
        assignment: AssignmentResponse,
    ) -> Result<(), ExecutorError> {
        self(assignment).await;
        Ok(())
    }
}

struct EventClock {
    anchor: Timestamp,
    since: Instant,
}
impl EventClock {
    fn new(anchor: Timestamp) -> Self {
        Self {
            anchor,
            since: Instant::now(),
        }
    }
    fn now(&self) -> Timestamp {
        let elapsed = u64::try_from(self.since.elapsed().as_millis()).unwrap_or(u64::MAX);
        Timestamp::from_unix_millis(self.anchor.as_unix_millis().saturating_add(elapsed))
    }
}

struct ActiveAssignment {
    job_id: JobId,
    allocation_id: AllocationId,
    agent_id: AgentId,
    clock: EventClock,
    process: Option<RunningProcess>,
    started_accepted: bool,
    terminal_attempted: bool,
    stop_reason: Option<StopReasonV1>,
}
impl ActiveAssignment {
    fn started(&self) -> AllocationEventRequest {
        AllocationEventRequest::Started {
            job_id: self.job_id,
            agent_id: self.agent_id,
            occurred_at: self.clock.now(),
        }
    }
    fn finished(&self, result: ProcessResultV1) -> AllocationEventRequest {
        AllocationEventRequest::Finished {
            job_id: self.job_id,
            agent_id: self.agent_id,
            occurred_at: self.clock.now(),
            result,
        }
    }
    fn stopped(&self, reason: StopReasonV1) -> AllocationEventRequest {
        AllocationEventRequest::Stopped {
            job_id: self.job_id,
            agent_id: self.agent_id,
            occurred_at: self.clock.now(),
            reason,
        }
    }
    async fn cleanup(&mut self) -> Result<(), ExecutorError> {
        if let Some(process) = self.process.as_mut() {
            // A completed wait (including a capture error) has already reaped
            // the child. Otherwise kill and reap explicitly before releasing it.
            if process.id().is_some() {
                let result = process.terminate_and_wait().await;
                if let Err(error) = result
                    && process.id().is_some()
                {
                    return Err(error);
                }
                // Capture errors after successful reap do not undo cleanup.
            }
        }
        self.process = None;
        Ok(())
    }
    async fn report(&self, client: &AgentHttpClient, event: AllocationEventRequest) -> bool {
        match client.report_event(self.allocation_id, event).await {
            Ok(response)
                if matches!(
                    response.outcome,
                    EventOutcomeV1::Applied | EventOutcomeV1::Duplicate
                ) =>
            {
                true
            }
            Ok(_) => {
                tracing::warn!(event="agent_lifecycle_report_unacknowledged", job_id=%self.job_id,
                    allocation_id=%self.allocation_id, agent_id=%self.agent_id);
                false
            }
            Err(error) => {
                // AgentClientError Debug contains only safe categories/status.
                tracing::warn!(event="agent_lifecycle_report_failed", job_id=%self.job_id,
                    allocation_id=%self.allocation_id, agent_id=%self.agent_id, error=?error);
                false
            }
        }
    }
}

pub(super) struct LifecycleRunner {
    executor: Executor,
    active: Option<ActiveAssignment>,
    poll_interval: Duration,
}
impl LifecycleRunner {
    pub(super) fn new(executor: Executor, poll_interval: Duration) -> Self {
        Self {
            executor,
            active: None,
            poll_interval,
        }
    }
}

fn process_result(status: ExitStatus) -> ProcessResultV1 {
    match status.code() {
        Some(exit_code) => ProcessResultV1::Exited { exit_code },
        None => ProcessResultV1::ExecutionFailed,
    }
}

enum WaitOutcome {
    Completed(Result<ExitStatus, ExecutorError>),
    StopRequested,
}

async fn wait_for_stop(
    client: &AgentHttpClient,
    job_id: JobId,
    allocation_id: AllocationId,
    agent_id: AgentId,
    interval: Duration,
) {
    loop {
        tokio::time::sleep(interval).await;
        match client.poll_stop(agent_id, allocation_id).await {
            Ok(Some(command))
                if command.job_id == job_id
                    && command.allocation_id == allocation_id
                    && command.agent_id == agent_id =>
            {
                return;
            }
            Ok(Some(_)) => {
                tracing::warn!(event="agent_stop_command_rejected", %job_id, %allocation_id, %agent_id)
            }
            Ok(None) => {}
            Err(error) => {
                tracing::warn!(event="agent_stop_poll_failed", %job_id, %allocation_id, %agent_id, error=?error)
            }
        }
    }
}

async fn wait_or_stop(
    process: &mut RunningProcess,
    client: &AgentHttpClient,
    job_id: JobId,
    allocation_id: AllocationId,
    agent_id: AgentId,
    interval: Duration,
) -> WaitOutcome {
    let completed = tokio::select! {
        biased;
        outcome = process.wait() => Some(outcome),
        () = wait_for_stop(client, job_id, allocation_id, agent_id, interval) => None,
    };
    if let Some(outcome) = completed {
        return WaitOutcome::Completed(outcome);
    }
    // wait may still be draining inherited pipes after the child exited.
    // Preserve that actual outcome instead of relabelling it as a user stop.
    match process.try_wait() {
        Ok(Some(_)) => WaitOutcome::Completed(process.wait().await),
        Ok(None) => WaitOutcome::StopRequested,
        Err(error) => WaitOutcome::Completed(Err(error)),
    }
}

impl AssignmentHandler for LifecycleRunner {
    async fn handle(
        &mut self,
        client: &AgentHttpClient,
        mut assignment: AssignmentResponse,
    ) -> Result<(), ExecutorError> {
        // Missing legacy execution data must never become a partially rebuilt job.
        let spec = assignment
            .execution
            .take()
            .and_then(|data| data.into_job_spec().ok());
        let valid = spec
            .as_ref()
            .is_some_and(|spec| JobSpecResponse::from(spec) == assignment.job.spec);
        let Some(server_time) = assignment.server_time.filter(|_| valid) else {
            tracing::warn!(event="agent_assignment_execution_rejected", job_id=%assignment.job.id,
                allocation_id=%assignment.allocation.id, agent_id=%assignment.allocation.agent_id);
            return pending().await;
        };
        let Some(spec) = spec else {
            return pending().await;
        };
        let anchor = server_time
            .max(assignment.job.updated_at)
            .max(assignment.allocation.assigned_at);
        self.active = Some(ActiveAssignment {
            job_id: assignment.job.id,
            allocation_id: assignment.allocation.id,
            agent_id: assignment.allocation.agent_id,
            clock: EventClock::new(anchor),
            process: None,
            started_accepted: false,
            terminal_attempted: false,
            stop_reason: None,
        });
        // This reference borrows persistent state; cancelling the future drops
        // the borrow, not the active process/report guards.
        let Some(active) = self.active.as_mut() else {
            return pending().await;
        };
        let terminal = match self.executor.spawn(&spec) {
            Ok(process) => {
                active.process = Some(process);
                if !active.report(client, active.started()).await {
                    active.cleanup().await?;
                    return pending().await;
                }
                active.started_accepted = true;
                let Some(process) = active.process.as_mut() else {
                    return pending().await;
                };
                let outcome = wait_or_stop(
                    process,
                    client,
                    active.job_id,
                    active.allocation_id,
                    active.agent_id,
                    self.poll_interval,
                )
                .await;
                tracing::debug!(event="agent_process_output_summary", job_id=%active.job_id,
                    allocation_id=%active.allocation_id, agent_id=%active.agent_id,
                    stdout_bytes=process.stdout().bytes().len(), stdout_truncated=process.stdout().is_truncated(),
                    stdout_drain_timed_out=process.stdout().drain_timed_out(),
                    stderr_bytes=process.stderr().bytes().len(), stderr_truncated=process.stderr().is_truncated(),
                    stderr_drain_timed_out=process.stderr().drain_timed_out());
                match outcome {
                    WaitOutcome::StopRequested => {
                        // Persistent before the first kill/reap await; shutdown
                        // must retain the user's reason if this future is dropped.
                        active.stop_reason = Some(StopReasonV1::UserRequested);
                        active.cleanup().await?;
                        active.stopped(StopReasonV1::UserRequested)
                    }
                    WaitOutcome::Completed(Ok(status)) => active.finished(process_result(status)),
                    WaitOutcome::Completed(Err(error)) => {
                        tracing::warn!(event="agent_process_wait_failed", job_id=%active.job_id,
                            allocation_id=%active.allocation_id, agent_id=%active.agent_id, code=error.code());
                        active.cleanup().await?;
                        active.finished(ProcessResultV1::ExecutionFailed)
                    }
                }
            }
            Err(error) => {
                tracing::warn!(event="agent_process_spawn_failed", job_id=%active.job_id,
                    allocation_id=%active.allocation_id, agent_id=%active.agent_id, code=error.code());
                active.finished(ProcessResultV1::SpawnFailed)
            }
        };
        active.terminal_attempted = true;
        if !active.report(client, terminal).await {
            return pending().await;
        }
        self.active = None;
        Ok(())
    }

    async fn shutdown(&mut self, client: &AgentHttpClient) -> Result<(), ExecutorError> {
        if let Some(active) = self.active.as_mut() {
            active.cleanup().await?;
            if active.started_accepted && !active.terminal_attempted {
                active.terminal_attempted = true;
                let event =
                    active.stopped(active.stop_reason.unwrap_or(StopReasonV1::AgentShutdown));
                // Shutdown cannot retry or claim successful network delivery.
                active.report(client, event).await;
            }
        }
        self.active = None;
        Ok(())
    }
}

#[cfg(test)]
mod tests;
