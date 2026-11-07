use crate::{
    domain::{AgentId, AllocationId},
    transport::v1::{
        AgentResponse, AllocationEventRequest, AssignmentResponse, ErrorResponse,
        EventReportResponse, RegisterAgentRequest, StopCommandResponse,
    },
};
use serde::de::DeserializeOwned;
use std::{error::Error, fmt, net::SocketAddr, time::Duration};

#[derive(Clone)]
pub(crate) struct AgentHttpClient {
    client: reqwest::Client,
    base_url: String,
}

pub(crate) enum AgentClientError {
    InvalidTimeout,
    ClientBuild(reqwest::Error),
    Transport(reqwest::Error),
    Server {
        status: reqwest::StatusCode,
        error: ErrorResponse,
    },
    InvalidResponse {
        status: reqwest::StatusCode,
        source: reqwest::Error,
    },
    UnexpectedStatus {
        status: reqwest::StatusCode,
    },
}

impl fmt::Debug for AgentClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTimeout => formatter.write_str("InvalidTimeout"),
            Self::ClientBuild(_) => formatter.write_str("ClientBuild"),
            Self::Transport(error) => formatter
                .debug_tuple("Transport")
                .field(&reqwest_error_kind(error))
                .finish(),
            Self::Server { status, .. } => formatter
                .debug_struct("Server")
                .field("status", status)
                .finish(),
            Self::InvalidResponse { status, source } => formatter
                .debug_struct("InvalidResponse")
                .field("status", status)
                .field("cause", &reqwest_error_kind(source))
                .finish(),
            Self::UnexpectedStatus { status } => formatter
                .debug_struct("UnexpectedStatus")
                .field("status", status)
                .finish(),
        }
    }
}

fn reqwest_error_kind(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_decode() {
        "decode"
    } else {
        "request"
    }
}

impl AgentHttpClient {
    pub(crate) fn new(
        server_addr: SocketAddr,
        request_timeout: Duration,
    ) -> Result<Self, AgentClientError> {
        if request_timeout.is_zero() {
            return Err(AgentClientError::InvalidTimeout);
        }

        let client = reqwest::Client::builder()
            .timeout(request_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| AgentClientError::ClientBuild(error.without_url()))?;

        Ok(Self {
            client,
            base_url: format!("http://{server_addr}"),
        })
    }

    pub(crate) async fn register(&self, address: &str) -> Result<AgentResponse, AgentClientError> {
        let request = RegisterAgentRequest {
            address: address.to_owned(),
        };
        self.send_json(
            self.client
                .post(self.url("/v1/agents/register"))
                .json(&request),
            reqwest::StatusCode::CREATED,
        )
        .await
    }

    pub(crate) async fn heartbeat(
        &self,
        agent_id: AgentId,
    ) -> Result<AgentResponse, AgentClientError> {
        self.send_json(
            self.client
                .post(self.url(&format!("/v1/agents/{agent_id}/heartbeat"))),
            reqwest::StatusCode::OK,
        )
        .await
    }

    pub(crate) async fn next_assignment(
        &self,
        agent_id: AgentId,
    ) -> Result<Option<AssignmentResponse>, AgentClientError> {
        let response = self
            .client
            .get(self.url(&format!("/v1/agents/{agent_id}/assignments/next")))
            .send()
            .await
            .map_err(|error| AgentClientError::Transport(error.without_url()))?;

        match response.status() {
            reqwest::StatusCode::NO_CONTENT => Ok(None),
            reqwest::StatusCode::OK => Self::decode_json(response).await.map(Some),
            status if status.is_success() => Err(AgentClientError::UnexpectedStatus { status }),
            _ => Err(Self::decode_server_error(response).await),
        }
    }

    pub(crate) async fn poll_stop(
        &self,
        agent_id: AgentId,
        allocation_id: AllocationId,
    ) -> Result<Option<StopCommandResponse>, AgentClientError> {
        let response = self
            .client
            .get(self.url(&format!(
                "/v1/agents/{agent_id}/allocations/{allocation_id}/stop"
            )))
            .send()
            .await
            .map_err(|error| AgentClientError::Transport(error.without_url()))?;
        match response.status() {
            reqwest::StatusCode::NO_CONTENT => Ok(None),
            reqwest::StatusCode::OK => Self::decode_json(response).await.map(Some),
            status if status.is_success() => Err(AgentClientError::UnexpectedStatus { status }),
            _ => Err(Self::decode_server_error(response).await),
        }
    }

    pub(crate) async fn report_event(
        &self,
        allocation_id: AllocationId,
        event: AllocationEventRequest,
    ) -> Result<EventReportResponse, AgentClientError> {
        self.send_json(
            self.client
                .post(self.url(&format!("/v1/allocations/{allocation_id}/events")))
                .json(&event),
            reqwest::StatusCode::OK,
        )
        .await
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn send_json<T>(
        &self,
        request: reqwest::RequestBuilder,
        expected_status: reqwest::StatusCode,
    ) -> Result<T, AgentClientError>
    where
        T: DeserializeOwned,
    {
        let response = request
            .send()
            .await
            .map_err(|error| AgentClientError::Transport(error.without_url()))?;
        let status = response.status();
        if status == expected_status {
            Self::decode_json(response).await
        } else if status.is_success() {
            Err(AgentClientError::UnexpectedStatus { status })
        } else {
            Err(Self::decode_server_error(response).await)
        }
    }

    async fn decode_json<T>(response: reqwest::Response) -> Result<T, AgentClientError>
    where
        T: DeserializeOwned,
    {
        let status = response.status();
        response
            .json::<T>()
            .await
            .map_err(|source| AgentClientError::InvalidResponse {
                status,
                source: source.without_url(),
            })
    }

    async fn decode_server_error(response: reqwest::Response) -> AgentClientError {
        let status = response.status();
        match response.json::<ErrorResponse>().await {
            Ok(error) => AgentClientError::Server { status, error },
            Err(source) => AgentClientError::InvalidResponse {
                status,
                source: source.without_url(),
            },
        }
    }
}

impl fmt::Display for AgentClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidTimeout => formatter.write_str("agent request timeout must be positive"),
            Self::ClientBuild(_) => {
                formatter.write_str("agent HTTP client could not be initialized")
            }
            Self::Transport(error) if error.is_timeout() => {
                formatter.write_str("agent HTTP request timed out")
            }
            Self::Transport(_) => formatter.write_str("agent HTTP request failed"),
            Self::Server { status, error } => write!(
                formatter,
                "control plane returned {} ({}) with HTTP status {status}",
                error.message, error.code
            ),
            Self::InvalidResponse { status, .. } => {
                write!(
                    formatter,
                    "control plane returned an invalid API response with HTTP status {status}"
                )
            }
            Self::UnexpectedStatus { status } => {
                write!(
                    formatter,
                    "control plane returned unexpected success status {status}"
                )
            }
        }
    }
}

impl Error for AgentClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ClientBuild(source)
            | Self::Transport(source)
            | Self::InvalidResponse { source, .. } => Some(source),
            Self::InvalidTimeout | Self::Server { .. } | Self::UnexpectedStatus { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AgentClientError, AgentHttpClient};
    use crate::{
        domain::{AgentId, AllocationId, JobId, Timestamp},
        transport::v1::{
            AllocationEventRequest, EventReportResponse, ProcessResultV1, StopReasonV1,
        },
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
    use std::{net::SocketAddr, sync::Arc, time::Duration};
    use tokio::{
        net::TcpListener,
        sync::{Mutex, oneshot},
        task::JoinHandle,
    };

    const AGENT_ID: &str = "550e8400-e29b-41d4-a716-446655440001";
    const JOB_ID: &str = "550e8400-e29b-41d4-a716-446655440000";
    const ALLOCATION_ID: &str = "550e8400-e29b-41d4-a716-446655440002";

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedRequest {
        method: Method,
        path: String,
        body: Value,
    }

    #[derive(Clone)]
    struct MockConfig {
        register_status: StatusCode,
        register_body: String,
        register_delay: Duration,
        poll_status: StatusCode,
        poll_body: String,
    }

    impl Default for MockConfig {
        fn default() -> Self {
            Self {
                register_status: StatusCode::CREATED,
                register_body: agent_response_json(),
                register_delay: Duration::ZERO,
                poll_status: StatusCode::OK,
                poll_body: assignment_response_json(),
            }
        }
    }

    #[derive(Default)]
    struct MockState {
        requests: Mutex<Vec<RecordedRequest>>,
        config: MockConfig,
    }

    struct MockServer {
        address: SocketAddr,
        state: Arc<MockState>,
        shutdown: Option<oneshot::Sender<()>>,
        task: JoinHandle<()>,
    }

    impl MockServer {
        async fn start(config: MockConfig) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("mock server should bind a loopback port");
            let address = listener
                .local_addr()
                .expect("mock server should expose its local address");
            let state = Arc::new(MockState {
                requests: Mutex::default(),
                config,
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

        async fn requests(&self) -> Vec<RecordedRequest> {
            self.state.requests.lock().await.clone()
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
            .route("/v1/agents/{agent_id}/assignments/next", get(poll))
            .route("/v1/allocations/{allocation_id}/events", post(report_event))
            .with_state(state)
    }

    async fn register(State(state): State<Arc<MockState>>, Json(body): Json<Value>) -> Response {
        record(&state, Method::POST, "/v1/agents/register", body).await;
        tokio::time::sleep(state.config.register_delay).await;
        response(state.config.register_status, &state.config.register_body)
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
        response(StatusCode::OK, &agent_response_json())
    }

    async fn poll(State(state): State<Arc<MockState>>, Path(agent_id): Path<String>) -> Response {
        let path = format!("/v1/agents/{agent_id}/assignments/next");
        record(&state, Method::GET, &path, Value::Null).await;
        response(state.config.poll_status, &state.config.poll_body)
    }

    async fn report_event(
        State(state): State<Arc<MockState>>,
        Path(allocation_id): Path<String>,
        Json(body): Json<Value>,
    ) -> Response {
        record(
            &state,
            Method::POST,
            &format!("/v1/allocations/{allocation_id}/events"),
            body,
        )
        .await;
        response(StatusCode::OK, r#"{"outcome":"applied"}"#)
    }

    async fn record(state: &MockState, method: Method, path: &str, body: Value) {
        state.requests.lock().await.push(RecordedRequest {
            method,
            path: path.to_owned(),
            body,
        });
    }

    fn response(status: StatusCode, body: &str) -> Response {
        (
            status,
            [(axum::http::header::CONTENT_TYPE, "application/json")],
            Body::from(body.to_owned()),
        )
            .into_response()
    }

    fn agent_response_json() -> String {
        format!(
            r#"{{"id":"{AGENT_ID}","address":"worker.example:9000","last_heartbeat_at":123,"health":"healthy","availability":"idle"}}"#
        )
    }

    fn assignment_response_json() -> String {
        format!(
            r#"{{"job":{{"id":"{JOB_ID}","spec":{{"command":"echo","args":["hello"],"env_keys":null,"working_dir":null}},"status":"assigned","submitted_at":100,"updated_at":110,"started_at":null,"finished_at":null,"terminal_result":null}},"allocation":{{"id":"{ALLOCATION_ID}","job_id":"{JOB_ID}","agent_id":"{AGENT_ID}","status":"assigned","attempt":1,"assigned_at":110,"started_at":null,"finished_at":null}}}}"#
        )
    }

    fn agent_id() -> AgentId {
        AGENT_ID.parse().expect("fixture agent ID should parse")
    }

    fn job_id() -> JobId {
        JOB_ID.parse().expect("fixture job ID should parse")
    }

    fn allocation_id() -> AllocationId {
        ALLOCATION_ID
            .parse()
            .expect("fixture allocation ID should parse")
    }

    #[tokio::test]
    async fn client_uses_v1_routes_and_json_contract() {
        let mock = MockServer::start(MockConfig::default()).await;
        let client = AgentHttpClient::new(mock.address, Duration::from_secs(1))
            .expect("valid address and timeout should build a client");

        let agent = client
            .register("worker.example:9000")
            .await
            .expect("registration should succeed");
        assert_eq!(agent.id, agent_id());
        let heartbeat = client
            .heartbeat(agent.id)
            .await
            .expect("heartbeat should succeed");
        assert_eq!(heartbeat.id, agent.id);

        let assignment = client
            .next_assignment(agent.id)
            .await
            .expect("assignment poll should succeed")
            .expect("mock poll should return an assignment");
        assert_eq!(assignment.allocation.id, allocation_id());
        assert_eq!(assignment.job.id, job_id());

        let event = AllocationEventRequest::Started {
            job_id: job_id(),
            agent_id: agent.id,
            occurred_at: Timestamp::from_unix_millis(120),
        };
        let report = client
            .report_event(allocation_id(), event)
            .await
            .expect("event report should succeed");
        assert_eq!(
            report,
            EventReportResponse {
                outcome: crate::transport::v1::EventOutcomeV1::Applied
            }
        );

        assert_eq!(
            mock.requests().await,
            vec![
                RecordedRequest {
                    method: Method::POST,
                    path: "/v1/agents/register".into(),
                    body: json!({"address": "worker.example:9000"}),
                },
                RecordedRequest {
                    method: Method::POST,
                    path: format!("/v1/agents/{AGENT_ID}/heartbeat"),
                    body: Value::Null,
                },
                RecordedRequest {
                    method: Method::GET,
                    path: format!("/v1/agents/{AGENT_ID}/assignments/next"),
                    body: Value::Null,
                },
                RecordedRequest {
                    method: Method::POST,
                    path: format!("/v1/allocations/{ALLOCATION_ID}/events"),
                    body: json!({
                        "type": "started",
                        "job_id": JOB_ID,
                        "agent_id": AGENT_ID,
                        "occurred_at": 120
                    }),
                },
            ]
        );
        mock.stop().await;
    }

    #[tokio::test]
    async fn finished_and_stopped_events_serialize_nested_fields() {
        let mock = MockServer::start(MockConfig::default()).await;
        let client = AgentHttpClient::new(mock.address, Duration::from_secs(1))
            .expect("valid address and timeout should build a client");

        for event in [
            AllocationEventRequest::Finished {
                job_id: job_id(),
                agent_id: agent_id(),
                occurred_at: Timestamp::from_unix_millis(130),
                result: ProcessResultV1::Exited { exit_code: 7 },
            },
            AllocationEventRequest::Finished {
                job_id: job_id(),
                agent_id: agent_id(),
                occurred_at: Timestamp::from_unix_millis(131),
                result: ProcessResultV1::SpawnFailed,
            },
            AllocationEventRequest::Stopped {
                job_id: job_id(),
                agent_id: agent_id(),
                occurred_at: Timestamp::from_unix_millis(132),
                reason: StopReasonV1::UserRequested,
            },
        ] {
            client
                .report_event(allocation_id(), event)
                .await
                .expect("event report should serialize and succeed");
        }

        let requests = mock.requests().await;
        let bodies = requests
            .iter()
            .map(|request| request.body.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            bodies,
            vec![
                json!({
                    "type": "finished",
                    "job_id": JOB_ID,
                    "agent_id": AGENT_ID,
                    "occurred_at": 130,
                    "result": {"type": "exited", "exit_code": 7}
                }),
                json!({
                    "type": "finished",
                    "job_id": JOB_ID,
                    "agent_id": AGENT_ID,
                    "occurred_at": 131,
                    "result": {"type": "spawn_failed"}
                }),
                json!({
                    "type": "stopped",
                    "job_id": JOB_ID,
                    "agent_id": AGENT_ID,
                    "occurred_at": 132,
                    "reason": "user_requested"
                }),
            ]
        );

        mock.stop().await;
    }

    #[tokio::test]
    async fn assignment_poll_maps_no_content_to_none() {
        let mock = MockServer::start(MockConfig {
            poll_status: StatusCode::NO_CONTENT,
            poll_body: String::new(),
            ..MockConfig::default()
        })
        .await;
        let client = AgentHttpClient::new(mock.address, Duration::from_secs(1))
            .expect("valid address and timeout should build a client");

        assert_eq!(
            client
                .next_assignment(agent_id())
                .await
                .expect("empty poll should succeed"),
            None
        );

        mock.stop().await;
    }

    #[tokio::test]
    async fn server_error_response_is_distinct_from_transport_errors() {
        let mock = MockServer::start(MockConfig {
            register_status: StatusCode::CONFLICT,
            register_body:
                r#"{"code":"conflict","message":"request conflicts with current state"}"#.into(),
            ..MockConfig::default()
        })
        .await;
        let client = AgentHttpClient::new(mock.address, Duration::from_secs(1))
            .expect("valid address and timeout should build a client");

        let error = client
            .register("worker.example:9000")
            .await
            .expect_err("409 should be returned as a server error");
        assert!(matches!(error, AgentClientError::Server { .. }));
        assert!(
            error
                .to_string()
                .contains("request conflicts with current state")
        );
        assert!(!error.to_string().contains("worker.example"));

        mock.stop().await;
    }

    #[tokio::test]
    async fn malformed_error_body_is_a_protocol_error() {
        let mock = MockServer::start(MockConfig {
            register_status: StatusCode::INTERNAL_SERVER_ERROR,
            register_body: "not json".into(),
            ..MockConfig::default()
        })
        .await;
        let client = AgentHttpClient::new(mock.address, Duration::from_secs(1))
            .expect("valid address and timeout should build a client");

        assert!(matches!(
            client.register("worker.example:9000").await,
            Err(AgentClientError::InvalidResponse { .. })
        ));

        mock.stop().await;
    }

    #[tokio::test]
    async fn unexpected_success_status_is_reported_without_retry() {
        let mock = MockServer::start(MockConfig {
            register_status: StatusCode::ACCEPTED,
            ..MockConfig::default()
        })
        .await;
        let client = AgentHttpClient::new(mock.address, Duration::from_secs(1))
            .expect("valid address and timeout should build a client");

        assert!(matches!(
            client.register("worker.example:9000").await,
            Err(AgentClientError::UnexpectedStatus { status })
                if status == StatusCode::ACCEPTED
        ));
        assert_eq!(mock.requests().await.len(), 1);

        mock.stop().await;
    }

    #[tokio::test]
    async fn connection_failure_is_reported_as_transport_error() {
        let client = AgentHttpClient::new(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            Duration::from_secs(1),
        )
        .expect("valid address and timeout should build a client");

        let error = client
            .register("worker.example:9000")
            .await
            .expect_err("missing server should be a transport failure");
        assert!(matches!(error, AgentClientError::Transport(_)));
        assert!(
            !format!("{error:?}").contains("127.0.0.1:0"),
            "request error debug should omit the URL: {error:?}"
        );
    }

    #[test]
    fn zero_request_timeout_is_rejected() {
        assert!(matches!(
            AgentHttpClient::new(SocketAddr::from(([127, 0, 0, 1], 8080)), Duration::ZERO),
            Err(AgentClientError::InvalidTimeout)
        ));
    }

    #[tokio::test]
    async fn request_timeout_is_reported_as_transport_error() {
        let mock = MockServer::start(MockConfig {
            register_delay: Duration::from_millis(100),
            ..MockConfig::default()
        })
        .await;
        let client = AgentHttpClient::new(mock.address, Duration::from_millis(10))
            .expect("valid address and timeout should build a client");

        let error = client
            .register("worker.example:9000")
            .await
            .expect_err("configured request timeout should be a transport failure");
        assert!(matches!(
            &error,
            AgentClientError::Transport(source) if source.is_timeout()
        ));
        assert!(!format!("{error:?}").contains(&format!("http://{}", mock.address)));
        assert!(!error.to_string().contains("/v1/agents/register"));

        mock.stop().await;
    }

    #[tokio::test]
    async fn malformed_success_body_is_a_protocol_error() {
        let mock = MockServer::start(MockConfig {
            register_body: "not json".into(),
            ..MockConfig::default()
        })
        .await;
        let client = AgentHttpClient::new(mock.address, Duration::from_secs(1))
            .expect("valid address and timeout should build a client");

        assert!(matches!(
            client.register("worker.example:9000").await,
            Err(AgentClientError::InvalidResponse { .. })
        ));

        mock.stop().await;
    }
}
