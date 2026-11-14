//! HTTP client for the user-facing job commands.

use crate::{
    domain::JobId,
    transport::v1::{
        CreateJobRequest, ErrorResponse, JobListResponse, JobResponse, StopJobResponse,
    },
};
use serde::de::DeserializeOwned;
use std::{error::Error, fmt, net::SocketAddr, time::Duration};

const CLI_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub(crate) struct CliHttpClient {
    client: reqwest::Client,
    base_url: String,
}

pub(crate) enum CliClientError {
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

impl fmt::Debug for CliHttpClient {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CliHttpClient")
            .finish_non_exhaustive()
    }
}

impl fmt::Debug for CliClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientBuild(_) => formatter.write_str("ClientBuild"),
            Self::Transport(error) => formatter
                .debug_tuple("Transport")
                .field(&reqwest_error_kind(error))
                .finish(),
            Self::Server { status, error } => formatter
                .debug_struct("Server")
                .field("status", status)
                .field("code", &error.code)
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

impl CliHttpClient {
    pub(crate) fn new(server_addr: SocketAddr) -> Result<Self, CliClientError> {
        let client = reqwest::Client::builder()
            .timeout(CLI_REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| CliClientError::ClientBuild(error.without_url()))?;

        Ok(Self {
            client,
            base_url: format!("http://{server_addr}"),
        })
    }

    pub(crate) async fn create_job(
        &self,
        request: &CreateJobRequest,
    ) -> Result<JobResponse, CliClientError> {
        self.send_json(
            self.client.post(self.url("/v1/jobs")).json(request),
            &[reqwest::StatusCode::CREATED],
        )
        .await
    }

    pub(crate) async fn list_jobs(&self) -> Result<JobListResponse, CliClientError> {
        self.send_json(
            self.client.get(self.url("/v1/jobs")),
            &[reqwest::StatusCode::OK],
        )
        .await
    }

    pub(crate) async fn get_job(&self, job_id: JobId) -> Result<JobResponse, CliClientError> {
        self.send_json(
            self.client.get(self.url(&format!("/v1/jobs/{job_id}"))),
            &[reqwest::StatusCode::OK],
        )
        .await
    }

    pub(crate) async fn stop_job(&self, job_id: JobId) -> Result<StopJobResponse, CliClientError> {
        self.send_json(
            self.client
                .post(self.url(&format!("/v1/jobs/{job_id}/stop"))),
            &[reqwest::StatusCode::OK, reqwest::StatusCode::ACCEPTED],
        )
        .await
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    async fn send_json<T>(
        &self,
        request: reqwest::RequestBuilder,
        expected_statuses: &[reqwest::StatusCode],
    ) -> Result<T, CliClientError>
    where
        T: DeserializeOwned,
    {
        let response = request
            .send()
            .await
            .map_err(|error| CliClientError::Transport(error.without_url()))?;
        let status = response.status();
        if expected_statuses.contains(&status) {
            Self::decode_json(response).await
        } else if status.is_success() {
            Err(CliClientError::UnexpectedStatus { status })
        } else {
            Err(Self::decode_server_error(response).await)
        }
    }

    async fn decode_json<T>(response: reqwest::Response) -> Result<T, CliClientError>
    where
        T: DeserializeOwned,
    {
        let status = response.status();
        response
            .json::<T>()
            .await
            .map_err(|source| CliClientError::InvalidResponse {
                status,
                source: source.without_url(),
            })
    }

    async fn decode_server_error(response: reqwest::Response) -> CliClientError {
        let status = response.status();
        match response.json::<ErrorResponse>().await {
            Ok(error) => CliClientError::Server { status, error },
            Err(source) => CliClientError::InvalidResponse {
                status,
                source: source.without_url(),
            },
        }
    }
}

impl fmt::Display for CliClientError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ClientBuild(_) => formatter.write_str("could not initialize CLI HTTP client"),
            Self::Transport(error) if error.is_timeout() => {
                formatter.write_str("request to control plane timed out")
            }
            Self::Transport(_) => formatter.write_str("request to control plane failed"),
            Self::Server { status, error } => write!(
                formatter,
                "control plane rejected the request: {} ({}) [HTTP {status}]",
                error.message, error.code
            ),
            Self::InvalidResponse { status, .. } => write!(
                formatter,
                "control plane returned an invalid API response [HTTP {status}]"
            ),
            Self::UnexpectedStatus { status } => write!(
                formatter,
                "control plane returned an unexpected success status [HTTP {status}]"
            ),
        }
    }
}

impl Error for CliClientError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ClientBuild(source)
            | Self::Transport(source)
            | Self::InvalidResponse { source, .. } => Some(source),
            Self::Server { .. } | Self::UnexpectedStatus { .. } => None,
        }
    }
}
#[cfg(test)]
mod tests {
    use super::{CliClientError, CliHttpClient};
    use crate::{
        domain::JobId,
        transport::v1::{CreateJobRequest, JobStatusV1, StopJobResponse},
    };
    use axum::{
        Router,
        body::{Body, to_bytes},
        extract::State,
        http::{Method, Request, StatusCode},
        response::{IntoResponse, Response},
    };
    use serde_json::{Value, json};
    use std::{
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

    const JOB_ID: &str = "550e8400-e29b-41d4-a716-446655440000";
    const ALLOCATION_ID: &str = "550e8400-e29b-41d4-a716-446655440002";
    const AGENT_ID: &str = "550e8400-e29b-41d4-a716-446655440001";

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct RecordedRequest {
        method: Method,
        path: String,
        body: Value,
    }

    #[derive(Clone)]
    struct MockState {
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        create_reply: Option<(StatusCode, String)>,
        stop_status: StatusCode,
    }

    struct MockServer {
        address: SocketAddr,
        requests: Arc<Mutex<Vec<RecordedRequest>>>,
        shutdown: Option<oneshot::Sender<()>>,
        task: JoinHandle<()>,
    }

    impl MockServer {
        async fn start(
            create_reply: Option<(StatusCode, String)>,
            stop_status: StatusCode,
        ) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let requests = Arc::new(Mutex::new(Vec::new()));
            let state = MockState {
                requests: Arc::clone(&requests),
                create_reply,
                stop_status,
            };
            let (shutdown, receiver) = oneshot::channel();
            let task = tokio::spawn(async move {
                axum::serve(listener, Router::new().fallback(handle).with_state(state))
                    .with_graceful_shutdown(async {
                        let _ = receiver.await;
                    })
                    .await
                    .unwrap();
            });
            Self {
                address,
                requests,
                shutdown: Some(shutdown),
                task,
            }
        }

        fn requests(&self) -> Vec<RecordedRequest> {
            self.requests.lock().unwrap().clone()
        }

        async fn stop(mut self) {
            if let Some(shutdown) = self.shutdown.take() {
                let _ = shutdown.send(());
            }
            self.task.await.unwrap();
        }
    }

    async fn handle(State(state): State<MockState>, request: Request<Body>) -> Response {
        let method = request.method().clone();
        let path = request.uri().path().to_owned();
        let body = to_bytes(request.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        state.requests.lock().unwrap().push(RecordedRequest {
            method: method.clone(),
            path: path.clone(),
            body,
        });

        if method == Method::POST && path == "/v1/jobs" {
            if let Some((status, body)) = &state.create_reply {
                return (*status, body.clone()).into_response();
            }
            return (StatusCode::CREATED, JsonBody(job_json())).into_response();
        }
        if method == Method::GET && path == "/v1/jobs" {
            return (StatusCode::OK, JsonBody(json!({"items": [job_json()]}))).into_response();
        }
        if method == Method::GET && path == format!("/v1/jobs/{JOB_ID}") {
            return (StatusCode::OK, JsonBody(job_json())).into_response();
        }
        if method == Method::POST && path == format!("/v1/jobs/{JOB_ID}/stop") {
            let body = if state.stop_status == StatusCode::ACCEPTED {
                json!({
                    "outcome": "requested",
                    "job_id": JOB_ID,
                    "allocation_id": ALLOCATION_ID,
                    "agent_id": AGENT_ID
                })
            } else {
                json!({"outcome": "completed", "job": job_json()})
            };
            return (state.stop_status, JsonBody(body)).into_response();
        }
        (StatusCode::NOT_FOUND, "not found").into_response()
    }

    struct JsonBody(Value);

    impl IntoResponse for JsonBody {
        fn into_response(self) -> Response {
            axum::Json(self.0).into_response()
        }
    }

    fn job_json() -> Value {
        json!({
            "id": JOB_ID,
            "spec": {
                "command": "echo",
                "args": ["hello"],
                "env_keys": null,
                "working_dir": null
            },
            "status": "pending",
            "submitted_at": 100,
            "updated_at": 100,
            "started_at": null,
            "finished_at": null,
            "terminal_result": null,
            "allocation": null
        })
    }

    fn job_id() -> JobId {
        JOB_ID.parse().unwrap()
    }

    fn create_request() -> CreateJobRequest {
        CreateJobRequest {
            command: "echo".into(),
            args: vec!["hello".into()],
            env: None,
            working_dir: None,
        }
    }

    #[tokio::test]
    async fn typed_client_uses_v1_job_routes_and_json_contract() {
        let server = MockServer::start(None, StatusCode::ACCEPTED).await;
        let client = CliHttpClient::new(server.address).unwrap();

        let created = client.create_job(&create_request()).await.unwrap();
        assert_eq!(created.job.id, job_id());
        assert_eq!(created.job.status, JobStatusV1::Pending);

        let listed = client.list_jobs().await.unwrap();
        assert_eq!(listed.items.len(), 1);
        assert_eq!(listed.items[0].job.id, job_id());

        let fetched = client.get_job(job_id()).await.unwrap();
        assert_eq!(fetched.job.id, job_id());

        let stopped = client.stop_job(job_id()).await.unwrap();
        assert!(matches!(stopped, StopJobResponse::Requested { .. }));

        let requests = server.requests();
        assert_eq!(requests.len(), 4);
        assert_eq!(requests[0].method, Method::POST);
        assert_eq!(requests[0].path, "/v1/jobs");
        assert_eq!(
            requests[0].body,
            json!({
                "command": "echo",
                "args": ["hello"],
                "env": null,
                "working_dir": null
            })
        );
        assert_eq!(requests[1].method, Method::GET);
        assert_eq!(requests[1].path, "/v1/jobs");
        assert_eq!(requests[2].path, format!("/v1/jobs/{JOB_ID}"));
        assert_eq!(requests[3].method, Method::POST);
        assert_eq!(requests[3].path, format!("/v1/jobs/{JOB_ID}/stop"));

        server.stop().await;
    }

    #[tokio::test]
    async fn stop_job_accepts_completed_response() {
        let server = MockServer::start(None, StatusCode::OK).await;
        let client = CliHttpClient::new(server.address).unwrap();

        let stopped = client.stop_job(job_id()).await.unwrap();
        assert!(matches!(stopped, StopJobResponse::Completed { .. }));

        server.stop().await;
    }

    #[tokio::test]
    async fn api_error_display_is_useful_and_omits_untrusted_details() {
        let secret = "internal-detail-must-not-be-printed";
        let body = json!({
            "code": "invalid_input",
            "message": "request contains invalid input",
            "details": null,
            "internal_details": secret
        })
        .to_string();
        let server = MockServer::start(Some((StatusCode::BAD_REQUEST, body)), StatusCode::OK).await;
        let client = CliHttpClient::new(server.address).unwrap();

        let error = client.create_job(&create_request()).await.unwrap_err();
        assert!(matches!(error, CliClientError::Server { .. }));
        let display = error.to_string();
        assert!(display.contains("request contains invalid input"));
        assert!(display.contains("invalid_input"));
        assert!(display.contains("400"));
        assert!(!display.contains(secret));
        let debug = format!("{error:?}");
        assert!(!debug.contains(secret));
        assert!(!debug.contains(&server.address.to_string()));

        server.stop().await;
    }

    #[tokio::test]
    async fn malformed_success_response_is_reported_without_body_or_url() {
        let secret = "raw-response-secret";
        let server = MockServer::start(
            Some((StatusCode::CREATED, format!("{{\"secret\":\"{secret}"))),
            StatusCode::OK,
        )
        .await;
        let client = CliHttpClient::new(server.address).unwrap();

        let error = client.create_job(&create_request()).await.unwrap_err();
        assert!(matches!(error, CliClientError::InvalidResponse { .. }));
        let display = error.to_string();
        assert!(!display.contains(secret));
        assert!(!display.contains(&server.address.to_string()));
        assert!(!format!("{error:?}").contains(secret));

        server.stop().await;
    }

    #[tokio::test]
    async fn malformed_server_error_response_is_reported_without_body_or_url() {
        let secret = "raw-error-response-secret";
        let server = MockServer::start(
            Some((StatusCode::BAD_GATEWAY, format!("{{\"secret\":\"{secret}"))),
            StatusCode::OK,
        )
        .await;
        let client = CliHttpClient::new(server.address).unwrap();

        let error = client.create_job(&create_request()).await.unwrap_err();
        assert!(matches!(error, CliClientError::InvalidResponse { .. }));
        let display = error.to_string();
        assert!(display.contains("502"));
        assert!(!display.contains(secret));
        assert!(!display.contains(&server.address.to_string()));
        assert!(!format!("{error:?}").contains(secret));

        server.stop().await;
    }

    #[tokio::test]
    async fn unexpected_success_status_is_an_error() {
        let server = MockServer::start(
            Some((StatusCode::ACCEPTED, job_json().to_string())),
            StatusCode::OK,
        )
        .await;
        let client = CliHttpClient::new(server.address).unwrap();

        let error = client.create_job(&create_request()).await.unwrap_err();
        assert!(
            matches!(error, CliClientError::UnexpectedStatus { status } if status == StatusCode::ACCEPTED)
        );
        assert!(!error.to_string().contains(&server.address.to_string()));

        server.stop().await;
    }

    #[tokio::test]
    async fn connection_failure_is_safe_and_retains_a_source() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let client = CliHttpClient::new(address).unwrap();

        let error = client.create_job(&create_request()).await.unwrap_err();
        assert!(matches!(error, CliClientError::Transport(_)));
        assert!(!error.to_string().contains(&address.to_string()));
        assert!(!format!("{error:?}").contains(&address.to_string()));
        assert!(std::error::Error::source(&error).is_some());
    }

    #[test]
    fn client_request_timeout_is_finite() {
        assert_eq!(super::CLI_REQUEST_TIMEOUT, Duration::from_secs(5));
    }
}
