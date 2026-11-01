use crate::{
    application::{AgentsService, JobDetails, JobsService, StopOutcome},
    config::ServerConfig,
    domain::Timestamp,
    error::{AppError, ErrorKind},
    scheduler::{Scheduler, SchedulerError},
    scheduler_loop::{SchedulerLoop, SchedulerLoopError},
    state::store::StateStore,
    transport::v1::{
        CreateJobRequest, ErrorResponse, HealthResponse, JobListResponse, JobResponse,
        StopJobResponse,
    },
};
use axum::{
    Json, Router,
    extract::{
        Path, State,
        rejection::{JsonRejection, PathRejection},
    },
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use std::{
    error::Error,
    fmt,
    future::Future,
    io,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::net::TcpListener;

pub(crate) enum ServerError {
    Runtime(io::Error),
    Bind(io::Error),
    Serve(io::Error),
    SchedulerLoop(SchedulerLoopError),
    Logging(Box<dyn Error + Send + Sync>),
}

impl fmt::Debug for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, formatter)
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Runtime(_) => formatter.write_str("server runtime could not be created"),
            Self::Bind(_) => formatter.write_str("server could not bind to the configured address"),
            Self::Serve(_) => formatter.write_str("HTTP server stopped with an I/O error"),
            Self::SchedulerLoop(error) => {
                write!(formatter, "scheduler could not start or stop ({error})")
            }
            Self::Logging(_) => formatter.write_str("server logging could not be initialized"),
        }
    }
}

impl Error for ServerError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Runtime(source) | Self::Bind(source) | Self::Serve(source) => Some(source),
            Self::SchedulerLoop(source) => Some(source),
            Self::Logging(source) => Some(source.as_ref()),
        }
    }
}

#[expect(
    dead_code,
    reason = "M5.4 agent handlers will use the remaining shared application service"
)]
struct ServerState {
    jobs: JobsService,
    agents: AgentsService,
    assignment_timeout: Duration,
}

pub(crate) fn run(config: ServerConfig) -> Result<(), ServerError> {
    initialize_logging()?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(ServerError::Runtime)?;
    runtime.block_on(async move {
        let listener = TcpListener::bind(config.bind_addr)
            .await
            .map_err(ServerError::Bind)?;
        run_with_listener(config, listener, shutdown_signal()).await
    })
}

fn initialize_logging() -> Result<(), ServerError> {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()
        .map_err(ServerError::Logging)
}

async fn run_with_listener<Shutdown>(
    config: ServerConfig,
    listener: TcpListener,
    shutdown: Shutdown,
) -> Result<(), ServerError>
where
    Shutdown: Future<Output = ()> + Send + 'static,
{
    let bind_addr = listener.local_addr().map_err(ServerError::Bind)?;
    let store = StateStore::default();
    let state = server_state(store.clone(), config.assignment_timeout);
    let mut scheduler_loop =
        SchedulerLoop::new(config.scheduler_interval).map_err(ServerError::SchedulerLoop)?;
    scheduler_loop
        .start(
            scheduler_tick_with_clock(
                Scheduler::new(store),
                state.assignment_timeout,
                current_timestamp,
            ),
            |error| tracing::error!(event = "scheduler_tick_failed", error = %error),
        )
        .map_err(ServerError::SchedulerLoop)?;

    tracing::info!(event = "server_started", bind_addr = %bind_addr);
    let serve_result = axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await;
    tracing::info!(event = "server_stopped", bind_addr = %bind_addr);
    let scheduler_result = scheduler_loop.stop();

    serve_result.map_err(ServerError::Serve)?;
    scheduler_result.map_err(ServerError::SchedulerLoop)?;
    Ok(())
}

fn server_state(store: StateStore, assignment_timeout: Duration) -> Arc<ServerState> {
    Arc::new(ServerState {
        jobs: JobsService::new(store.clone()),
        agents: AgentsService::new(store),
        assignment_timeout,
    })
}

fn router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/v1/jobs", get(list_jobs).post(create_job))
        .route("/v1/jobs/{job_id}", get(get_job))
        .route("/v1/jobs/{job_id}/stop", post(stop_job))
        .with_state(state)
}

async fn health(State(_state): State<Arc<ServerState>>) -> Json<HealthResponse> {
    Json(HealthResponse::ok())
}

async fn create_job(
    State(state): State<Arc<ServerState>>,
    request: Result<Json<CreateJobRequest>, JsonRejection>,
) -> Response {
    let Json(request) = match request {
        Ok(request) => request,
        Err(rejection) => return json_rejection_response(rejection),
    };
    let submitted_at = match current_timestamp() {
        Ok(timestamp) => timestamp,
        Err(reason) => return clock_error_response(reason),
    };
    match state
        .jobs
        .submit_job(request.into_submission(), submitted_at)
    {
        Ok(job) => (
            StatusCode::CREATED,
            Json(JobResponse::from_parts(&job, None)),
        )
            .into_response(),
        Err(error) => app_error_response(error),
    }
}

async fn list_jobs(State(state): State<Arc<ServerState>>) -> Response {
    match state.jobs.list_job_details() {
        Ok(jobs) => {
            let items = jobs.iter().map(job_response).collect();
            (StatusCode::OK, Json(JobListResponse { items })).into_response()
        }
        Err(error) => app_error_response(error),
    }
}

async fn get_job(
    State(state): State<Arc<ServerState>>,
    path: Result<Path<String>, PathRejection>,
) -> Response {
    let Path(id) = match path {
        Ok(path) => path,
        Err(_) => return invalid_job_id_response(),
    };
    let id = match id.parse() {
        Ok(id) => id,
        Err(_) => return invalid_job_id_response(),
    };
    match state.jobs.get_job_details(id) {
        Ok(job) => (StatusCode::OK, Json(job_response(&job))).into_response(),
        Err(error) => app_error_response(error),
    }
}

async fn stop_job(
    State(state): State<Arc<ServerState>>,
    path: Result<Path<String>, PathRejection>,
) -> Response {
    let Path(id) = match path {
        Ok(path) => path,
        Err(_) => return invalid_job_id_response(),
    };
    let id = match id.parse() {
        Ok(id) => id,
        Err(_) => return invalid_job_id_response(),
    };
    let stopped_at = match current_timestamp() {
        Ok(timestamp) => timestamp,
        Err(reason) => return clock_error_response(reason),
    };
    match state.jobs.stop_job(id, stopped_at) {
        Ok(outcome) => {
            let status = match outcome {
                StopOutcome::Completed(_) => StatusCode::OK,
                StopOutcome::Requested(_) => StatusCode::ACCEPTED,
            };
            (status, Json(StopJobResponse::from(outcome))).into_response()
        }
        Err(error) => app_error_response(error),
    }
}

fn job_response(details: &JobDetails) -> JobResponse {
    JobResponse::from_parts(&details.job, details.allocation.as_ref())
}

fn invalid_job_id_response() -> Response {
    app_error_response(AppError::invalid_input("job ID path is invalid"))
}

fn json_rejection_response(rejection: JsonRejection) -> Response {
    let status = rejection.status();
    let error = if status.is_server_error() {
        AppError::internal(
            "request JSON could not be decoded",
            io::Error::other("JSON extractor failed"),
        )
    } else {
        AppError::invalid_input("request JSON is invalid")
    };
    error_response(status, &error)
}

fn clock_error_response(reason: &'static str) -> Response {
    app_error_response(AppError::internal(
        "server clock is unavailable",
        io::Error::other(reason),
    ))
}

fn app_error_response(error: AppError) -> Response {
    let status = match error.kind() {
        ErrorKind::InvalidInput => StatusCode::BAD_REQUEST,
        ErrorKind::NotFound => StatusCode::NOT_FOUND,
        ErrorKind::Conflict => StatusCode::CONFLICT,
        ErrorKind::Internal | ErrorKind::Transport | ErrorKind::Process => {
            StatusCode::INTERNAL_SERVER_ERROR
        }
    };
    error_response(status, &error)
}

fn error_response(status: StatusCode, error: &AppError) -> Response {
    (status, Json(ErrorResponse::from(error))).into_response()
}

fn current_timestamp() -> Result<Timestamp, &'static str> {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "system clock is before the Unix epoch")?;
    let millis = u64::try_from(elapsed.as_millis())
        .map_err(|_| "system timestamp exceeds the supported range")?;
    Ok(Timestamp::from_unix_millis(millis))
}

fn scheduler_tick_with_clock<Clock>(
    scheduler: Scheduler,
    assignment_timeout: Duration,
    mut clock: Clock,
) -> impl FnMut() -> Result<(), SchedulerError> + Send + 'static
where
    Clock: FnMut() -> Result<Timestamp, &'static str> + Send + 'static,
{
    move || {
        let now = match clock() {
            Ok(now) => now,
            Err(reason) => {
                tracing::error!(event = "scheduler_clock_failed", reason);
                return Ok(());
            }
        };
        scheduler.tick(now, assignment_timeout).map(|_| ())
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let terminate = signal(SignalKind::terminate());
        let terminate_signal = async move {
            match terminate {
                Ok(mut stream) => {
                    stream.recv().await;
                }
                Err(error) => {
                    tracing::error!(
                        event = "shutdown_signal_registration_failed",
                        signal = "sigterm",
                        error = %error
                    );
                    std::future::pending::<()>().await;
                }
            }
        };
        tokio::select! {
            result = tokio::signal::ctrl_c() => {
                if let Err(error) = result {
                    tracing::error!(
                        event = "shutdown_signal_wait_failed",
                        signal = "ctrl_c",
                        error = %error
                    );
                }
            }
            () = terminate_signal => {}
        }
    }
    #[cfg(not(unix))]
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
    use crate::{
        config::ServerConfig,
        domain::{
            Agent, AgentId, Allocation, AllocationId, Job, JobId, JobSpec, JobStatus, Timestamp,
        },
        scheduler::Scheduler,
        state::store::StateStore,
    };
    use serde_json::{Value, json};
    use std::{
        io::{Read, Write},
        net::SocketAddr,
        num::NonZeroU32,
        time::Duration,
    };
    use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

    fn config() -> ServerConfig {
        ServerConfig {
            bind_addr: "127.0.0.1:0".parse().expect("fixture address is valid"),
            liveness_timeout: Duration::from_secs(30),
            assignment_timeout: Duration::from_secs(1),
            scheduler_interval: Duration::from_secs(60),
        }
    }

    fn assigned_store() -> (StateStore, JobId) {
        let store = StateStore::default();
        let job_id = "00000000-0000-4000-8000-000000000001"
            .parse::<JobId>()
            .expect("fixture job id is valid");
        let agent_id = "00000000-0000-4000-8001-000000000001"
            .parse::<AgentId>()
            .expect("fixture agent id is valid");
        let allocation_id = "00000000-0000-4000-8002-000000000001"
            .parse::<AllocationId>()
            .expect("fixture allocation id is valid");
        let assigned_at = Timestamp::from_unix_millis(1_000);
        store
            .create_job(Job::new(
                job_id,
                JobSpec::new("echo", Vec::new(), None, None).expect("fixture job spec is valid"),
                assigned_at,
            ))
            .expect("fixture job should be stored");
        store
            .register_agent(Agent::new(
                agent_id,
                "worker.example:9000".into(),
                assigned_at,
            ))
            .expect("fixture agent should be stored");
        store
            .assign_allocation(Allocation::new(
                allocation_id,
                job_id,
                agent_id,
                NonZeroU32::MIN,
                assigned_at,
            ))
            .expect("fixture allocation should be stored");
        (store, job_id)
    }

    async fn start_test_router(
        store: StateStore,
    ) -> (
        SocketAddr,
        oneshot::Sender<()>,
        JoinHandle<Result<(), std::io::Error>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("test listener should bind");
        let address = listener.local_addr().expect("listener has an address");
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                super::router(super::server_state(store, Duration::from_secs(30))),
            )
            .with_graceful_shutdown(async move {
                let _ = shutdown_receiver.await;
            })
            .await
        });
        (address, shutdown_sender, server)
    }

    async fn http_request(address: SocketAddr, method: &str, path: &str, body: &str) -> String {
        let method = method.to_owned();
        let path = path.to_owned();
        let body = body.to_owned();
        tokio::task::spawn_blocking(move || {
            let mut stream = std::net::TcpStream::connect(address)
                .expect("test server should accept the request");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("read timeout should be set");
            write!(
                stream,
                "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            )
            .expect("request should be written");
            let mut response = String::new();
            stream
                .read_to_string(&mut response)
                .expect("response should finish");
            response
        })
        .await
        .expect("blocking HTTP client should complete")
    }

    fn response_status(response: &str) -> u16 {
        response
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|status| status.parse().ok())
            .expect("response should have an HTTP status")
    }

    fn response_json(response: &str) -> Value {
        let (_, body) = response
            .split_once("\r\n\r\n")
            .expect("response should contain headers and body");
        serde_json::from_str(body).expect("response should contain JSON")
    }

    #[tokio::test]
    async fn serves_health_and_gracefully_stops_the_scheduler() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ephemeral listener should bind");
        let address: SocketAddr = listener.local_addr().expect("listener has an address");
        let (shutdown_sender, shutdown_receiver) = oneshot::channel();
        let server = tokio::spawn(super::run_with_listener(config(), listener, async move {
            let _ = shutdown_receiver.await;
        }));

        let response = tokio::task::spawn_blocking(move || {
            let mut stream = std::net::TcpStream::connect(address)
                .expect("server should accept the health request");
            stream
                .set_read_timeout(Some(Duration::from_secs(2)))
                .expect("read timeout should be set");
            stream
                .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
                .expect("health request should be written");
            let mut response = String::new();
            stream
                .read_to_string(&mut response)
                .expect("health response should finish");
            response
        })
        .await
        .expect("blocking HTTP client should complete");

        let (headers, body) = response
            .split_once("\r\n\r\n")
            .expect("HTTP response should contain headers and body");
        assert!(headers.starts_with("HTTP/1.1 200 OK"));
        assert_eq!(body.trim(), r#"{"status":"ok"}"#);

        shutdown_sender
            .send(())
            .expect("server should still be waiting for shutdown");
        server
            .await
            .expect("server task should finish")
            .expect("server should stop cleanly");
    }

    #[tokio::test]
    async fn jobs_http_routes_create_list_and_get_safe_job_responses() {
        let (address, shutdown_sender, server) = start_test_router(StateStore::default()).await;
        let create = http_request(
            address,
            "POST",
            "/v1/jobs",
            r#"{"command":"echo","args":["hello"],"env":{"TOKEN":"must-not-leak"}}"#,
        )
        .await;

        assert_eq!(response_status(&create), 201);
        let created = response_json(&create);
        let job_id = created["id"]
            .as_str()
            .expect("created response should include job ID");
        assert_eq!(created["status"], "pending");
        assert_eq!(created["spec"]["env_keys"], json!(["TOKEN"]));
        assert!(!create.contains("must-not-leak"));

        let list = http_request(address, "GET", "/v1/jobs", "").await;
        assert_eq!(response_status(&list), 200);
        assert_eq!(response_json(&list)["items"][0]["id"], job_id);

        let path = format!("/v1/jobs/{job_id}");
        let get = http_request(address, "GET", &path, "").await;
        assert_eq!(response_status(&get), 200);
        assert_eq!(response_json(&get)["id"], job_id);

        shutdown_sender
            .send(())
            .expect("test server should still be running");
        server
            .await
            .expect("test server task should finish")
            .expect("test router should stop cleanly");
    }

    #[tokio::test]
    async fn jobs_http_routes_return_safe_input_and_lookup_errors() {
        let (address, shutdown_sender, server) = start_test_router(StateStore::default()).await;
        let malformed = http_request(
            address,
            "POST",
            "/v1/jobs",
            r#"{"command":"private-marker", broken}"#,
        )
        .await;
        assert_eq!(response_status(&malformed), 400);
        assert_eq!(response_json(&malformed)["code"], "invalid_input");
        assert!(!malformed.contains("private-marker"));

        let invalid_spec = http_request(address, "POST", "/v1/jobs", r#"{"command":"  "}"#).await;
        assert_eq!(response_status(&invalid_spec), 400);
        assert_eq!(response_json(&invalid_spec)["code"], "invalid_input");

        let unknown_field = http_request(
            address,
            "POST",
            "/v1/jobs",
            r#"{"command":"echo","private_field":"must-not-leak"}"#,
        )
        .await;
        assert_eq!(response_status(&unknown_field), 422);
        assert_eq!(response_json(&unknown_field)["code"], "invalid_input");
        assert!(!unknown_field.contains("must-not-leak"));

        let invalid_id = http_request(address, "GET", "/v1/jobs/not-a-uuid", "").await;
        assert_eq!(response_status(&invalid_id), 400);
        assert_eq!(response_json(&invalid_id)["code"], "invalid_input");

        let unknown_id = http_request(
            address,
            "GET",
            "/v1/jobs/00000000-0000-4000-8000-000000000099",
            "",
        )
        .await;
        assert_eq!(response_status(&unknown_id), 404);
        assert_eq!(response_json(&unknown_id)["code"], "not_found");

        shutdown_sender
            .send(())
            .expect("test server should still be running");
        server
            .await
            .expect("test server task should finish")
            .expect("test router should stop cleanly");
    }

    #[tokio::test]
    async fn jobs_http_stop_maps_completed_requested_and_conflict_outcomes() {
        let (address, shutdown_sender, server) = start_test_router(StateStore::default()).await;
        let create = http_request(address, "POST", "/v1/jobs", r#"{"command":"echo"}"#).await;
        let pending_job_id = response_json(&create)["id"]
            .as_str()
            .expect("created job should have an ID")
            .to_owned();

        let stop_path = format!("/v1/jobs/{pending_job_id}/stop");
        let completed = http_request(address, "POST", &stop_path, "").await;
        assert_eq!(response_status(&completed), 200);
        assert_eq!(response_json(&completed)["outcome"], "completed");
        assert_eq!(response_json(&completed)["job"]["status"], "stopped");

        let conflict = http_request(address, "POST", &stop_path, "").await;
        assert_eq!(response_status(&conflict), 409);
        assert_eq!(response_json(&conflict)["code"], "conflict");

        shutdown_sender
            .send(())
            .expect("test server should still be running");
        server
            .await
            .expect("test server task should finish")
            .expect("test router should stop cleanly");

        let (assigned_store, assigned_job_id) = assigned_store();
        let store_snapshot = assigned_store.clone();
        let (address, shutdown_sender, server) = start_test_router(assigned_store).await;
        let assigned_path = format!("/v1/jobs/{assigned_job_id}");
        let assigned = http_request(address, "GET", &assigned_path, "").await;
        assert_eq!(response_status(&assigned), 200);
        assert_eq!(response_json(&assigned)["status"], "assigned");
        assert_eq!(response_json(&assigned)["allocation"]["status"], "assigned");

        let stop_path = format!("/v1/jobs/{assigned_job_id}/stop");
        let requested = http_request(address, "POST", &stop_path, "").await;
        assert_eq!(response_status(&requested), 202);
        assert_eq!(response_json(&requested)["outcome"], "requested");
        assert_eq!(
            response_json(&requested)["job_id"],
            assigned_job_id.to_string()
        );
        assert_eq!(
            store_snapshot.get_job(assigned_job_id).unwrap().status(),
            JobStatus::Assigned
        );

        shutdown_sender
            .send(())
            .expect("test server should still be running");
        server
            .await
            .expect("test server task should finish")
            .expect("test router should stop cleanly");
    }

    #[test]
    fn scheduler_tick_uses_the_shared_assignment_timeout() {
        let config = config();
        let (store, job_id) = assigned_store();
        let state = super::server_state(store.clone(), config.assignment_timeout);
        let mut tick = super::scheduler_tick_with_clock(
            Scheduler::new(store.clone()),
            state.assignment_timeout,
            || Ok(Timestamp::from_unix_millis(2_000)),
        );

        tick().expect("scheduler tick should succeed");

        assert_eq!(store.get_job(job_id).unwrap().status(), JobStatus::Lost);
    }
}
