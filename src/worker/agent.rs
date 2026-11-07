use crate::{
    config::AgentConfig,
    domain::AgentId,
    transport::v1::{AllocationStatusV1, AssignmentResponse, JobStatusV1},
    worker::{
        client::{AgentClientError, AgentHttpClient},
        executor::{Executor, ExecutorError},
        launch_registry::LaunchRegistry,
        lifecycle::{AssignmentHandler, LifecycleRunner},
    },
};
use std::{error::Error, fmt, future::Future, time::Duration};

const AGENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(crate) enum AgentRuntimeError {
    Client(AgentClientError),
    Logging(Box<dyn Error + Send + Sync>),
    Runtime(std::io::Error),
    LaunchRegistryPoisoned,
    Executor(ExecutorError),
}

impl fmt::Display for AgentRuntimeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Client(error) => write!(
                formatter,
                "agent startup failed: {}",
                SafeAgentClientError(error)
            ),
            Self::Logging(_) => formatter.write_str("agent logging could not be initialized"),
            Self::Runtime(_) => formatter.write_str("agent runtime could not be initialized"),
            Self::Executor(error) => {
                write!(formatter, "agent process handling failed: {}", error.code())
            }
            Self::LaunchRegistryPoisoned => {
                formatter.write_str("agent assignment registry is unavailable")
            }
        }
    }
}

struct SafeAgentClientError<'a>(&'a AgentClientError);

impl fmt::Display for SafeAgentClientError<'_> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            AgentClientError::InvalidTimeout => formatter.write_str("invalid_request_timeout"),
            AgentClientError::ClientBuild(_) => formatter.write_str("client_initialization_error"),
            AgentClientError::Transport(error) if error.is_timeout() => {
                formatter.write_str("transport_timeout")
            }
            AgentClientError::Transport(_) => formatter.write_str("transport_error"),
            AgentClientError::Server { status, .. } => {
                write!(formatter, "server_error (HTTP status {status})")
            }
            AgentClientError::InvalidResponse { status, .. } => {
                write!(formatter, "invalid_response (HTTP status {status})")
            }
            AgentClientError::UnexpectedStatus { status } => {
                write!(formatter, "unexpected_status (HTTP status {status})")
            }
        }
    }
}

impl Error for AgentRuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Client(source) => Some(source),
            Self::Logging(source) => Some(source.as_ref()),
            Self::Runtime(source) => Some(source),
            Self::LaunchRegistryPoisoned => None,
            Self::Executor(error) => Some(error),
        }
    }
}

struct RegisteredAgent {
    client: AgentHttpClient,
    id: AgentId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HeartbeatConnectionState {
    Connected,
    Disconnected,
}

impl HeartbeatConnectionState {
    fn observe(&mut self, request_succeeded: bool) -> Option<Self> {
        let next = if request_succeeded {
            Self::Connected
        } else {
            Self::Disconnected
        };
        if *self == next {
            None
        } else {
            *self = next;
            Some(next)
        }
    }
}

pub(crate) fn run(config: AgentConfig) -> Result<(), AgentRuntimeError> {
    initialize_logging()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(AgentRuntimeError::Runtime)?;

    runtime.block_on(run_with_shutdown(config, shutdown_signal()))
}

pub(crate) async fn run_with_shutdown<Shutdown>(
    config: AgentConfig,
    shutdown: Shutdown,
) -> Result<(), AgentRuntimeError>
where
    Shutdown: Future<Output = ()> + Send,
{
    let executor = Executor::new(&config.working_dir).map_err(AgentRuntimeError::Executor)?;
    run_with_handler(config, shutdown, LifecycleRunner::new(executor)).await
}

async fn run_with_handler<Shutdown, Handle>(
    config: AgentConfig,
    shutdown: Shutdown,
    mut handle_assignment: Handle,
) -> Result<(), AgentRuntimeError>
where
    Shutdown: Future<Output = ()> + Send,
    Handle: AssignmentHandler,
{
    let client = AgentHttpClient::new(config.server_addr, AGENT_REQUEST_TIMEOUT)
        .map_err(AgentRuntimeError::Client)?;
    let response = client
        .register(&config.agent_address)
        .await
        .map_err(AgentRuntimeError::Client)?;
    let agent = RegisteredAgent {
        client,
        id: response.id,
    };

    tracing::info!(event = "agent_registered", agent_id = %agent.id);
    let result = tokio::select! {
        biased;
        () = shutdown => Ok(()),
        result = agent.assignment_loop(config.poll_interval, &mut handle_assignment) => result,
        () = agent.heartbeat_loop(config.heartbeat_interval) => Ok(()),
    };
    handle_assignment
        .shutdown(&agent.client)
        .await
        .map_err(AgentRuntimeError::Executor)?;
    tracing::info!(event = "agent_shutdown", agent_id = %agent.id);
    result
}

fn assignment_is_eligible(assignment: &AssignmentResponse, agent_id: AgentId) -> bool {
    assignment.allocation.agent_id == agent_id
        && assignment.allocation.job_id == assignment.job.id
        && assignment.job.status == JobStatusV1::Assigned
        && assignment.allocation.status == AllocationStatusV1::Assigned
        && assignment.allocation.attempt > 0
        && assignment.job.started_at.is_none()
        && assignment.job.finished_at.is_none()
        && assignment.job.terminal_result.is_none()
        && assignment.allocation.started_at.is_none()
        && assignment.allocation.finished_at.is_none()
}

impl RegisteredAgent {
    async fn assignment_loop<Handle>(
        &self,
        interval: Duration,
        handle_assignment: &mut Handle,
    ) -> Result<(), AgentRuntimeError>
    where
        Handle: AssignmentHandler,
    {
        let registry = LaunchRegistry::default();
        loop {
            // Wait after every result, including empty, duplicate and failure,
            // to prevent a busy loop. The first poll uses the same interval.
            tokio::time::sleep(interval).await;
            let assignment = match self.client.next_assignment(self.id).await {
                Ok(Some(assignment)) => assignment,
                Ok(None) => continue,
                Err(error) => {
                    tracing::warn!(
                        event = "agent_assignment_poll_failed",
                        agent_id = %self.id,
                        error = %SafeAgentClientError(&error)
                    );
                    continue;
                }
            };
            if !assignment_is_eligible(&assignment, self.id) {
                tracing::warn!(event = "agent_assignment_rejected", agent_id = %self.id);
                continue;
            }
            let claimed = registry
                .claim(assignment.allocation.id)
                .map_err(|_| AgentRuntimeError::LaunchRegistryPoisoned)?;
            if !claimed {
                tracing::debug!(
                    event = "agent_assignment_duplicate",
                    agent_id = %self.id,
                    job_id = %assignment.job.id,
                    allocation_id = %assignment.allocation.id
                );
                continue;
            }
            // The consumer must resolve only after terminal handling completes.
            // Awaiting it owns the single local slot; heartbeat runs separately.
            handle_assignment
                .handle(&self.client, assignment)
                .await
                .map_err(AgentRuntimeError::Executor)?;
        }
    }

    async fn heartbeat_loop(&self, interval: Duration) {
        let mut connection_state = HeartbeatConnectionState::Connected;
        loop {
            tokio::time::sleep(interval).await;
            match self.client.heartbeat(self.id).await {
                Ok(_) => {
                    if connection_state.observe(true) == Some(HeartbeatConnectionState::Connected) {
                        tracing::info!(event = "agent_connected", agent_id = %self.id);
                    }
                }
                Err(error) => {
                    if connection_state.observe(false)
                        == Some(HeartbeatConnectionState::Disconnected)
                    {
                        tracing::warn!(
                            event = "agent_disconnected",
                            agent_id = %self.id,
                            error = %SafeAgentClientError(&error)
                        );
                    }
                }
            }
        }
    }
}

fn initialize_logging() -> Result<(), AgentRuntimeError> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(AgentRuntimeError::Logging)
}

async fn shutdown_signal() {
    if let Err(error) = tokio::signal::ctrl_c().await {
        tracing::error!(
            event = "shutdown_signal_wait_failed",
            signal = "ctrl_c",
            error = %error
        );
    }
    tracing::info!(event = "shutdown_signal_received");
}

#[cfg(test)]
mod tests {
    use super::{
        HeartbeatConnectionState, SafeAgentClientError, run_with_handler, run_with_shutdown,
    };
    use crate::{
        config::AgentConfig, domain::AgentId, transport::v1::AssignmentResponse,
        worker::client::AgentHttpClient,
    };
    use axum::{
        Json, Router,
        body::Body,
        extract::{Path, State},
        http::{Method, StatusCode},
        response::{IntoResponse, Response},
        routing::{get, post},
    };
    use serde_json::{Value, json};
    use std::{
        collections::VecDeque,
        future::pending,
        net::SocketAddr,
        path::PathBuf,
        sync::Arc,
        time::{Duration, Instant},
    };
    use tokio::{
        net::TcpListener,
        sync::{Mutex, Notify, mpsc, oneshot},
        task::JoinHandle,
    };

    const AGENT_ID: &str = "550e8400-e29b-41d4-a716-446655440001";
    const ECHOED_ERROR_CODE: &str = "private-code-worker-test";
    const ECHOED_ERROR_MESSAGE: &str = "reflected worker-test https://proxy.invalid/private";

    #[test]
    fn heartbeat_connection_state_reports_only_transitions() {
        let mut state = HeartbeatConnectionState::Connected;

        assert_eq!(
            state.observe(false),
            Some(HeartbeatConnectionState::Disconnected)
        );
        assert_eq!(state.observe(false), None);
        assert_eq!(
            state.observe(true),
            Some(HeartbeatConnectionState::Connected)
        );
        assert_eq!(state.observe(true), None);
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedRequest {
        method: Method,
        path: String,
        body: Value,
        received_at: Instant,
    }

    enum PollReply {
        Empty,
        Error,
        Assignment(Box<AssignmentResponse>),
        Stalled(Arc<Notify>),
    }

    struct MockState {
        requests: Mutex<Vec<RecordedRequest>>,
        request_notification: Notify,
        registration_status: StatusCode,
        heartbeat_statuses: Mutex<VecDeque<StatusCode>>,
        assignment_replies: Mutex<VecDeque<PollReply>>,
    }

    struct MockServer {
        address: SocketAddr,
        state: Arc<MockState>,
        shutdown: Option<oneshot::Sender<()>>,
        task: JoinHandle<()>,
    }

    impl MockServer {
        async fn start(
            registration_status: StatusCode,
            heartbeat_statuses: impl IntoIterator<Item = StatusCode>,
        ) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("mock server should bind a loopback port");
            let address = listener
                .local_addr()
                .expect("mock server should expose its local address");
            let state = Arc::new(MockState {
                requests: Mutex::default(),
                request_notification: Notify::new(),
                registration_status,
                heartbeat_statuses: Mutex::new(heartbeat_statuses.into_iter().collect()),
                assignment_replies: Mutex::default(),
            });
            let (shutdown, shutdown_receiver) = oneshot::channel();
            let task = tokio::spawn({
                let state = Arc::clone(&state);
                async move {
                    axum::serve(listener, mock_router(state))
                        .with_graceful_shutdown(async move {
                            let _ = shutdown_receiver.await;
                        })
                        .await
                        .expect("mock server should serve requests");
                }
            });

            Self {
                address,
                state,
                shutdown: Some(shutdown),
                task,
            }
        }

        async fn enqueue_assignments(&self, replies: impl IntoIterator<Item = PollReply>) {
            self.state.assignment_replies.lock().await.extend(replies);
        }

        async fn wait_for_requests(&self, count: usize) -> Vec<RecordedRequest> {
            self.wait_for_matching_requests(count, |_| true).await
        }

        async fn wait_for_polls(&self, count: usize) -> Vec<RecordedRequest> {
            self.wait_for_matching_requests(count, |request| request.method == Method::GET)
                .await
        }

        async fn wait_for_matching_requests(
            &self,
            count: usize,
            matches: impl Fn(&RecordedRequest) -> bool,
        ) -> Vec<RecordedRequest> {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let notified = self.state.request_notification.notified();
                    let requests = self.state.requests.lock().await;
                    let matching: Vec<_> = requests
                        .iter()
                        .filter(|request| matches(request))
                        .cloned()
                        .collect();
                    if matching.len() >= count {
                        return matching;
                    }
                    drop(requests);
                    notified.await;
                }
            })
            .await
            .expect("mock should receive the expected requests")
        }

        async fn stop(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            self.task
                .await
                .expect("mock server task should finish cleanly");
        }
    }

    fn mock_router(state: Arc<MockState>) -> Router {
        Router::new()
            .route("/v1/agents/register", post(register))
            .route("/v1/agents/{agent_id}/heartbeat", post(heartbeat))
            .route(
                "/v1/agents/{agent_id}/assignments/next",
                get(poll_assignment),
            )
            .with_state(state)
    }

    async fn register(State(state): State<Arc<MockState>>, Json(body): Json<Value>) -> Response {
        record(&state, Method::POST, "/v1/agents/register", body).await;
        if state.registration_status == StatusCode::CREATED {
            response(
                state.registration_status,
                &agent_response_json("worker-test"),
            )
        } else {
            error_response(state.registration_status)
        }
    }

    async fn heartbeat(
        State(state): State<Arc<MockState>>,
        Path(agent_id): Path<String>,
    ) -> Response {
        record(
            &state,
            Method::POST,
            &format!("/v1/agents/{agent_id}/heartbeat"),
            Value::Null,
        )
        .await;
        let status = state
            .heartbeat_statuses
            .lock()
            .await
            .pop_front()
            .unwrap_or(StatusCode::OK);
        if status == StatusCode::OK {
            response(status, &agent_response_json("worker-test"))
        } else {
            error_response(status)
        }
    }

    async fn poll_assignment(
        State(state): State<Arc<MockState>>,
        Path(agent_id): Path<String>,
    ) -> Response {
        record(
            &state,
            Method::GET,
            &format!("/v1/agents/{agent_id}/assignments/next"),
            Value::Null,
        )
        .await;
        let reply = state
            .assignment_replies
            .lock()
            .await
            .pop_front()
            .unwrap_or(PollReply::Empty);
        match reply {
            PollReply::Empty => StatusCode::NO_CONTENT.into_response(),
            PollReply::Error => error_response(StatusCode::SERVICE_UNAVAILABLE),
            PollReply::Assignment(assignment) => Json(assignment).into_response(),
            PollReply::Stalled(release) => {
                release.notified().await;
                StatusCode::NO_CONTENT.into_response()
            }
        }
    }

    async fn record(state: &MockState, method: Method, path: &str, body: Value) {
        state.requests.lock().await.push(RecordedRequest {
            method,
            path: path.to_owned(),
            body,
            received_at: Instant::now(),
        });
        state.request_notification.notify_one();
    }

    fn response(status: StatusCode, body: &str) -> Response {
        (
            status,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            Body::from(body.to_owned()),
        )
            .into_response()
    }

    fn error_response(status: StatusCode) -> Response {
        response(
            status,
            r#"{"code":"private-code-worker-test","message":"reflected worker-test https://proxy.invalid/private"}"#,
        )
    }

    fn agent_response_json(address: &str) -> String {
        format!(
            r#"{{"id":"{AGENT_ID}","address":"{address}","last_heartbeat_at":123,"health":"healthy","availability":"idle"}}"#
        )
    }

    fn agent_config(server_addr: SocketAddr, heartbeat_interval: Duration) -> AgentConfig {
        AgentConfig {
            server_addr,
            agent_address: "worker-test".to_owned(),
            heartbeat_interval,
            poll_interval: Duration::from_secs(1),
            working_dir: PathBuf::from("."),
        }
    }

    #[tokio::test]
    async fn registers_with_configured_address_and_heartbeats_with_returned_id() {
        let mock = MockServer::start(StatusCode::CREATED, []).await;
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let agent = tokio::spawn(run_with_shutdown(
            agent_config(mock.address, Duration::from_millis(10)),
            async move {
                let _ = shutdown_receiver.await;
            },
        ));

        let requests = mock
            .wait_for_matching_requests(2, |r| r.method == Method::POST)
            .await;
        let _ = shutdown_sender.send(());
        agent
            .await
            .expect("agent task should finish cleanly")
            .expect("agent runtime should stop cleanly");

        assert_eq!(requests[0].method, Method::POST);
        assert_eq!(requests[0].path, "/v1/agents/register");
        assert_eq!(requests[0].body, json!({"address":"worker-test"}));
        assert_eq!(requests[1].method, Method::POST);
        assert_eq!(requests[1].path, format!("/v1/agents/{AGENT_ID}/heartbeat"));
        mock.stop().await;
    }

    #[tokio::test]
    async fn registration_failure_stops_before_heartbeat() {
        let mock = MockServer::start(StatusCode::SERVICE_UNAVAILABLE, []).await;
        let result = run_with_shutdown(
            agent_config(mock.address, Duration::from_millis(10)),
            pending::<()>(),
        )
        .await;

        let error = result.expect_err("registration error should stop agent startup");
        let error = error.to_string();
        assert!(error.contains("503"));
        assert!(!error.contains(ECHOED_ERROR_CODE));
        assert!(!error.contains(ECHOED_ERROR_MESSAGE));
        let requests = mock.wait_for_requests(1).await;
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/v1/agents/register");
        mock.stop().await;
    }

    #[tokio::test]
    async fn heartbeat_failure_does_not_stop_later_attempts() {
        let mock = MockServer::start(
            StatusCode::CREATED,
            [StatusCode::SERVICE_UNAVAILABLE, StatusCode::OK],
        )
        .await;
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let agent = tokio::spawn(run_with_shutdown(
            agent_config(mock.address, Duration::from_millis(10)),
            async move {
                let _ = shutdown_receiver.await;
            },
        ));

        let requests = mock
            .wait_for_matching_requests(3, |r| r.method == Method::POST)
            .await;
        let _ = shutdown_sender.send(());
        agent
            .await
            .expect("agent task should finish cleanly")
            .expect("temporary heartbeat error should not stop the loop");

        assert_eq!(requests.len(), 3);
        assert_eq!(requests[1].path, format!("/v1/agents/{AGENT_ID}/heartbeat"));
        assert_eq!(requests[2].path, format!("/v1/agents/{AGENT_ID}/heartbeat"));
        mock.stop().await;
    }

    #[tokio::test]
    async fn heartbeat_error_summary_does_not_echo_server_response() {
        let mock = MockServer::start(StatusCode::CREATED, [StatusCode::SERVICE_UNAVAILABLE]).await;
        let client = AgentHttpClient::new(mock.address, Duration::from_secs(1))
            .expect("client config should be valid");
        let agent_id = AGENT_ID
            .parse::<AgentId>()
            .expect("agent ID should be valid");

        let error = client
            .heartbeat(agent_id)
            .await
            .expect_err("mock heartbeat should fail with the configured status");
        let summary = SafeAgentClientError(&error).to_string();

        assert!(summary.contains("server_error"));
        assert!(summary.contains("503"));
        assert!(!summary.contains(ECHOED_ERROR_CODE));
        assert!(!summary.contains(ECHOED_ERROR_MESSAGE));
        mock.stop().await;
    }

    #[tokio::test]
    async fn shutdown_stops_heartbeat_loop() {
        let mock = MockServer::start(StatusCode::CREATED, []).await;
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let agent = tokio::spawn(run_with_shutdown(
            agent_config(mock.address, Duration::from_secs(30)),
            async move {
                let _ = shutdown_receiver.await;
            },
        ));

        let requests = mock.wait_for_requests(1).await;
        let _ = shutdown_sender.send(());
        tokio::time::timeout(Duration::from_secs(1), agent)
            .await
            .expect("shutdown should interrupt the heartbeat wait")
            .expect("agent task should finish cleanly")
            .expect("agent runtime should stop cleanly");

        assert_eq!(requests.len(), 1);
        mock.stop().await;
    }

    const JOB_ID: &str = "550e8400-e29b-41d4-a716-446655440002";
    const ALLOCATION_ID: &str = "550e8400-e29b-41d4-a716-446655440003";
    const SECOND_ALLOCATION_ID: &str = "550e8400-e29b-41d4-a716-446655440004";

    fn assignment(allocation_id: &str) -> AssignmentResponse {
        serde_json::from_value(json!({
            "job": {
                "id": JOB_ID,
                "spec": {"command": "private-command", "args": ["private-argument"], "env_keys": ["PRIVATE_KEY"], "working_dir": null},
                "status": "assigned", "submitted_at": 1, "updated_at": 2,
                "started_at": null, "finished_at": null, "terminal_result": null
            },
            "allocation": {
                "id": allocation_id, "job_id": JOB_ID, "agent_id": AGENT_ID,
                "status": "assigned", "attempt": 1, "assigned_at": 2,
                "started_at": null, "finished_at": null
            }
        })).expect("test assignment should match API v1")
    }

    fn polling_config(address: SocketAddr, interval: Duration) -> AgentConfig {
        let mut config = agent_config(address, Duration::from_millis(10));
        config.poll_interval = interval;
        config
    }

    async fn receive_assignment(
        receiver: &mut mpsc::UnboundedReceiver<AssignmentResponse>,
    ) -> AssignmentResponse {
        tokio::time::timeout(Duration::from_secs(2), receiver.recv())
            .await
            .expect("assignment should arrive")
            .expect("handler should remain available")
    }

    async fn stop_agent(
        sender: oneshot::Sender<()>,
        agent: JoinHandle<Result<(), super::AgentRuntimeError>>,
    ) {
        let _ = sender.send(());
        tokio::time::timeout(Duration::from_millis(200), agent)
            .await
            .expect("shutdown should cancel in-flight work promptly")
            .expect("agent task should finish")
            .expect("agent should stop cleanly");
    }

    #[tokio::test]
    async fn polling_waits_between_empty_and_failed_responses_then_hands_off_assignment() {
        let mock = MockServer::start(StatusCode::CREATED, []).await;
        let expected = assignment(ALLOCATION_ID);
        mock.enqueue_assignments([
            PollReply::Empty,
            PollReply::Error,
            PollReply::Assignment(Box::new(expected.clone())),
        ])
        .await;
        let interval = Duration::from_millis(30);
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let agent = tokio::spawn(run_with_handler(
            polling_config(mock.address, interval),
            async move {
                let _ = shutdown_receiver.await;
            },
            move |assignment: AssignmentResponse| {
                sender
                    .send(assignment)
                    .expect("handoff observer should be open");
                pending::<()>()
            },
        ));

        assert_eq!(receive_assignment(&mut receiver).await, expected);
        let polls = mock.wait_for_polls(3).await;
        for pair in polls.windows(2) {
            assert!(pair[1].received_at.duration_since(pair[0].received_at) >= interval);
        }
        assert!(
            polls
                .iter()
                .all(|r| r.path == format!("/v1/agents/{AGENT_ID}/assignments/next"))
        );
        stop_agent(shutdown_sender, agent).await;
        mock.stop().await;
    }

    #[tokio::test]
    async fn assignment_handoff_blocks_further_polling_but_keeps_heartbeat_alive() {
        let mock = MockServer::start(StatusCode::CREATED, []).await;
        mock.enqueue_assignments([
            PollReply::Assignment(Box::new(assignment(ALLOCATION_ID))),
            PollReply::Assignment(Box::new(assignment(SECOND_ALLOCATION_ID))),
        ])
        .await;
        let release = Arc::new(Notify::new());
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let handler_release = Arc::clone(&release);
        let agent = tokio::spawn(run_with_handler(
            polling_config(mock.address, Duration::from_millis(5)),
            async move {
                let _ = shutdown_receiver.await;
            },
            move |assignment: AssignmentResponse| {
                let release = Arc::clone(&handler_release);
                sender
                    .send(assignment)
                    .expect("handoff observer should be open");
                async move {
                    release.notified().await;
                }
            },
        ));

        assert_eq!(
            receive_assignment(&mut receiver).await.allocation.id,
            ALLOCATION_ID.parse().unwrap()
        );
        mock.wait_for_matching_requests(4, |r| r.path.ends_with("/heartbeat"))
            .await;
        assert_eq!(
            mock.state
                .requests
                .lock()
                .await
                .iter()
                .filter(|r| r.method == Method::GET)
                .count(),
            1
        );
        release.notify_one();
        assert_eq!(
            receive_assignment(&mut receiver).await.allocation.id,
            SECOND_ALLOCATION_ID.parse().unwrap()
        );
        stop_agent(shutdown_sender, agent).await;
        mock.stop().await;
    }

    #[tokio::test]
    async fn completed_handoff_does_not_readmit_a_duplicate_allocation() {
        let mock = MockServer::start(StatusCode::CREATED, []).await;
        mock.enqueue_assignments([
            PollReply::Assignment(Box::new(assignment(ALLOCATION_ID))),
            PollReply::Assignment(Box::new(assignment(ALLOCATION_ID))),
            PollReply::Assignment(Box::new(assignment(SECOND_ALLOCATION_ID))),
        ])
        .await;
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let agent = tokio::spawn(run_with_handler(
            polling_config(mock.address, Duration::from_millis(5)),
            async move {
                let _ = shutdown_receiver.await;
            },
            move |assignment: AssignmentResponse| {
                sender
                    .send(assignment)
                    .expect("handoff observer should be open");
                async {}
            },
        ));

        assert_eq!(
            receive_assignment(&mut receiver).await.allocation.id,
            ALLOCATION_ID.parse().unwrap()
        );
        assert_eq!(
            receive_assignment(&mut receiver).await.allocation.id,
            SECOND_ALLOCATION_ID.parse().unwrap()
        );
        assert!(receiver.try_recv().is_err());
        stop_agent(shutdown_sender, agent).await;
        mock.stop().await;
    }

    #[tokio::test]
    async fn invalid_assignment_snapshots_are_skipped_without_consuming_the_claim() {
        use crate::domain::Timestamp;
        use crate::transport::v1::{AllocationStatusV1, JobStatusV1};
        let mock = MockServer::start(StatusCode::CREATED, []).await;
        let valid = assignment(ALLOCATION_ID);
        let mut invalid = Vec::new();
        let mut value = valid.clone();
        value.allocation.agent_id = JOB_ID.parse().unwrap();
        invalid.push(value);
        let mut value = valid.clone();
        value.allocation.job_id = AGENT_ID.parse().unwrap();
        invalid.push(value);
        let mut value = valid.clone();
        value.job.status = JobStatusV1::Running;
        invalid.push(value);
        let mut value = valid.clone();
        value.allocation.status = AllocationStatusV1::Running;
        invalid.push(value);
        let mut value = valid.clone();
        value.allocation.attempt = 0;
        invalid.push(value);
        let mut value = valid.clone();
        value.job.started_at = Some(Timestamp::from_unix_millis(3));
        invalid.push(value);
        let mut value = valid.clone();
        value.allocation.finished_at = Some(Timestamp::from_unix_millis(3));
        invalid.push(value);
        mock.enqueue_assignments(
            invalid
                .into_iter()
                .chain([valid.clone()])
                .map(|a| PollReply::Assignment(Box::new(a))),
        )
        .await;
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let agent = tokio::spawn(run_with_handler(
            polling_config(mock.address, Duration::from_millis(3)),
            async move {
                let _ = shutdown_receiver.await;
            },
            move |assignment: AssignmentResponse| {
                sender
                    .send(assignment)
                    .expect("handoff observer should be open");
                pending::<()>()
            },
        ));

        assert_eq!(receive_assignment(&mut receiver).await, valid);
        assert_eq!(mock.wait_for_polls(8).await.len(), 8);
        assert!(receiver.try_recv().is_err());
        stop_agent(shutdown_sender, agent).await;
        mock.stop().await;
    }

    #[tokio::test]
    async fn default_runtime_defers_one_assignment_and_continues_heartbeat_until_shutdown() {
        let mock = MockServer::start(StatusCode::CREATED, []).await;
        mock.enqueue_assignments([PollReply::Assignment(Box::new(assignment(ALLOCATION_ID)))])
            .await;
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let agent = tokio::spawn(run_with_shutdown(
            polling_config(mock.address, Duration::from_millis(5)),
            async move {
                let _ = shutdown_receiver.await;
            },
        ));

        mock.wait_for_polls(1).await;
        mock.wait_for_matching_requests(4, |r| r.path.ends_with("/heartbeat"))
            .await;
        assert_eq!(
            mock.state
                .requests
                .lock()
                .await
                .iter()
                .filter(|r| r.method == Method::GET)
                .count(),
            1
        );
        stop_agent(shutdown_sender, agent).await;
        mock.stop().await;
    }

    #[tokio::test]
    async fn pending_poll_does_not_block_heartbeat_or_shutdown() {
        let mock = MockServer::start(StatusCode::CREATED, []).await;
        let release = Arc::new(Notify::new());
        mock.enqueue_assignments([PollReply::Stalled(Arc::clone(&release))])
            .await;
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let agent = tokio::spawn(run_with_shutdown(
            polling_config(mock.address, Duration::from_millis(5)),
            async move {
                let _ = shutdown_receiver.await;
            },
        ));

        mock.wait_for_polls(1).await;
        mock.wait_for_matching_requests(2, |r| r.path.ends_with("/heartbeat"))
            .await;
        stop_agent(shutdown_sender, agent).await;
        release.notify_one();
        mock.stop().await;
    }

    #[tokio::test]
    async fn poll_error_summary_does_not_echo_server_response() {
        let mock = MockServer::start(StatusCode::CREATED, []).await;
        mock.enqueue_assignments([PollReply::Error]).await;
        let client = AgentHttpClient::new(mock.address, Duration::from_secs(1)).unwrap();
        let error = client
            .next_assignment(AGENT_ID.parse().unwrap())
            .await
            .unwrap_err();
        let summary = SafeAgentClientError(&error).to_string();
        assert!(summary.contains("503"));
        assert!(!summary.contains(ECHOED_ERROR_CODE));
        assert!(!summary.contains(ECHOED_ERROR_MESSAGE));
        mock.stop().await;
    }

    #[tokio::test]
    async fn shutdown_cancels_poll_interval_without_issuing_a_poll() {
        let mock = MockServer::start(StatusCode::CREATED, []).await;
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let agent = tokio::spawn(run_with_shutdown(
            polling_config(mock.address, Duration::from_secs(60)),
            async move {
                let _ = shutdown_receiver.await;
            },
        ));
        mock.wait_for_requests(1).await;
        stop_agent(shutdown_sender, agent).await;
        assert_eq!(
            mock.state
                .requests
                .lock()
                .await
                .iter()
                .filter(|r| r.method == Method::GET)
                .count(),
            0
        );
        mock.stop().await;
    }
}
