use crate::{
    application::{AgentsService, JobsService},
    config::ServerConfig,
    domain::Timestamp,
    scheduler::{Scheduler, SchedulerError},
    scheduler_loop::{SchedulerLoop, SchedulerLoopError},
    state::store::StateStore,
    transport::v1::HealthResponse,
};
use axum::{Json, Router, extract::State, routing::get};
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
    reason = "M5.3 and M5.4 handlers will use these shared application services"
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
        .with_state(state)
}

async fn health(State(_state): State<Arc<ServerState>>) -> Json<HealthResponse> {
    Json(HealthResponse::ok())
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
    use std::{
        io::{Read, Write},
        net::SocketAddr,
        num::NonZeroU32,
        time::Duration,
    };
    use tokio::{net::TcpListener, sync::oneshot};

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
