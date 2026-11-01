use crate::{
    config::AgentConfig,
    domain::AgentId,
    worker::client::{AgentClientError, AgentHttpClient},
};
use std::{error::Error, fmt, future::Future, time::Duration};

const AGENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(crate) enum AgentRuntimeError {
    Client(AgentClientError),
    Logging(Box<dyn Error + Send + Sync>),
    Runtime(std::io::Error),
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

async fn run_with_shutdown<Shutdown>(
    config: AgentConfig,
    shutdown: Shutdown,
) -> Result<(), AgentRuntimeError>
where
    Shutdown: Future<Output = ()> + Send,
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
    agent
        .heartbeat_loop(config.heartbeat_interval, shutdown)
        .await;
    Ok(())
}

impl RegisteredAgent {
    async fn heartbeat_loop<Shutdown>(&self, interval: Duration, shutdown: Shutdown)
    where
        Shutdown: Future<Output = ()> + Send,
    {
        tokio::pin!(shutdown);
        let mut connection_state = HeartbeatConnectionState::Connected;
        loop {
            tokio::select! {
                () = &mut shutdown => {
                    tracing::info!(event = "agent_shutdown", agent_id = %self.id);
                    return;
                }
                () = tokio::time::sleep(interval) => {
                    match self.client.heartbeat(self.id).await {
                        Ok(_) => {
                            if connection_state.observe(true)
                                == Some(HeartbeatConnectionState::Connected)
                            {
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
    use super::{HeartbeatConnectionState, SafeAgentClientError, run_with_shutdown};
    use crate::{config::AgentConfig, domain::AgentId, worker::client::AgentHttpClient};
    use axum::{
        Json, Router,
        body::Body,
        extract::{Path, State},
        http::{Method, StatusCode},
        response::{IntoResponse, Response},
        routing::post,
    };
    use serde_json::{Value, json};
    use std::{
        collections::VecDeque, future::pending, net::SocketAddr, path::PathBuf, sync::Arc,
        time::Duration,
    };
    use tokio::{
        net::TcpListener,
        sync::{Mutex, Notify, oneshot},
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
    }

    struct MockState {
        requests: Mutex<Vec<RecordedRequest>>,
        request_notification: Notify,
        registration_status: StatusCode,
        heartbeat_statuses: Mutex<VecDeque<StatusCode>>,
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

        async fn wait_for_requests(&self, count: usize) -> Vec<RecordedRequest> {
            tokio::time::timeout(Duration::from_secs(2), async {
                loop {
                    let notified = self.state.request_notification.notified();
                    let requests = self.state.requests.lock().await;
                    if requests.len() >= count {
                        return requests.clone();
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

    async fn record(state: &MockState, method: Method, path: &str, body: Value) {
        state.requests.lock().await.push(RecordedRequest {
            method,
            path: path.to_owned(),
            body,
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

        let requests = mock.wait_for_requests(2).await;
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

        let requests = mock.wait_for_requests(3).await;
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
}
