use std::{
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};

use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde_json::{Value, json};
use tokio::{net::TcpListener, sync::oneshot, task::JoinHandle};

const SUBMITTED_JOB_ID: &str = "550e8400-e29b-41d4-a716-446655440000";

static NEXT_TEMP_ID: AtomicUsize = AtomicUsize::new(0);

fn run_cli(args: &[&str]) -> Output {
    run_cli_with_env(args, &[])
}

fn run_cli_with_env(args: &[&str], env: &[(&str, &str)]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_maestro"));
    command.env_clear().args(args);
    for (key, value) in env {
        command.env(key, value);
    }
    command.output().expect("maestro binary should start")
}

fn unique_temp_path() -> PathBuf {
    let id = NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("maestro-config-test-{}-{id}", std::process::id()))
}

#[derive(Clone)]
struct SubmitMockState {
    requests: Arc<Mutex<Vec<Value>>>,
    response_status: StatusCode,
    response_body: Value,
}

struct SubmitMockServer {
    address: std::net::SocketAddr,
    requests: Arc<Mutex<Vec<Value>>>,
    shutdown: oneshot::Sender<()>,
    task: JoinHandle<()>,
}

impl SubmitMockServer {
    async fn start(response_status: StatusCode, response_body: Value) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("submit fixture should bind a loopback address");
        let address = listener
            .local_addr()
            .expect("submit fixture address should be available");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let state = SubmitMockState {
            requests: Arc::clone(&requests),
            response_status,
            response_body,
        };
        let app = Router::new()
            .route("/v1/jobs", post(submit_handler))
            .with_state(state);
        let (shutdown, shutdown_signal) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = shutdown_signal.await;
                })
                .await
                .expect("submit fixture should shut down cleanly");
        });

        Self {
            address,
            requests,
            shutdown,
            task,
        }
    }

    fn requests(&self) -> Vec<Value> {
        self.requests
            .lock()
            .expect("request list should not be poisoned")
            .clone()
    }

    async fn stop(self) {
        self.shutdown
            .send(())
            .expect("submit fixture should still be running");
        self.task
            .await
            .expect("submit fixture task should finish cleanly");
    }
}

async fn submit_handler(
    State(state): State<SubmitMockState>,
    Json(request): Json<Value>,
) -> (StatusCode, Json<Value>) {
    state
        .requests
        .lock()
        .expect("request list should not be poisoned")
        .push(request);
    (state.response_status, Json(state.response_body))
}

fn submitted_job_response() -> Value {
    json!({
        "id": SUBMITTED_JOB_ID,
        "spec": {
            "command": "/bin/echo",
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

fn run_cli_owned(args: Vec<String>) -> Output {
    let arg_refs = args.iter().map(String::as_str).collect::<Vec<_>>();
    run_cli(&arg_refs)
}

#[test]
fn help_lists_the_supported_commands() {
    let output = run_cli(&["--help"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    assert!(stdout.contains("Usage:"));
    for command in ["server", "agent", "submit", "status", "stop"] {
        assert!(stdout.contains(command), "help is missing {command}");
    }
}

#[test]
fn commands_run_as_placeholders() {
    for command in ["status", "stop"] {
        let output = run_cli(&[command]);
        let stdout = String::from_utf8_lossy(&output.stdout);

        assert!(output.status.success(), "{command} returned an error");
        assert!(stdout.contains(&format!("maestro {command} mode is not implemented yet")));
    }
}

#[test]
fn agent_startup_reports_server_unavailable_without_echoing_address() {
    let address = "private-agent-label:1234";
    let output = run_cli(&[
        "agent",
        "--server-addr",
        "127.0.0.1:0",
        "--agent-address",
        address,
    ]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("agent startup failed"));
    assert!(!stderr.contains(address));
    assert!(!stderr.contains("http://"));
}

#[test]
fn invalid_arguments_fail_without_echoing_the_input() {
    let secret = "do-not-print-this-value";

    for args in [
        &[secret][..],
        &["server", secret][..],
        &["--token", secret][..],
    ] {
        let output = run_cli(args);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(!output.status.success(), "invalid arguments were accepted");
        assert!(
            !stderr.contains(secret),
            "parser echoed user input: {stderr}"
        );
    }
}

#[test]
fn unknown_command_has_a_clear_safe_error() {
    let command = "not-a-real-command";
    let output = run_cli(&[command]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("unknown command"));
    assert!(!stderr.contains(command));
}

#[test]
fn agent_help_shows_config_options_and_defaults() {
    let output = run_cli(&["agent", "--help"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    for expected in [
        "--agent-address",
        "MAESTRO_AGENT_ADDRESS",
        "--server-addr",
        "--heartbeat-interval-secs",
        "--poll-interval-secs",
        "--working-dir",
        "127.0.0.1:8080",
        "5",
        "1",
    ] {
        assert!(stdout.contains(expected), "help is missing {expected}");
    }
}

#[test]
fn server_help_shows_scheduler_timeouts_and_defaults() {
    let output = run_cli(&["server", "--help"]);
    let stdout = String::from_utf8_lossy(&output.stdout);

    assert!(output.status.success());
    for expected in [
        "--assignment-timeout-secs",
        "MAESTRO_ASSIGNMENT_TIMEOUT_SECS",
        "30",
        "--scheduler-interval-secs",
        "MAESTRO_SCHEDULER_INTERVAL_SECS",
        "1",
    ] {
        assert!(stdout.contains(expected), "help is missing {expected}");
    }
}

#[test]
fn server_rejects_invalid_scheduler_timeout_environment_values_safely() {
    for (variable, value) in [
        ("MAESTRO_ASSIGNMENT_TIMEOUT_SECS", "not-a-duration"),
        ("MAESTRO_SCHEDULER_INTERVAL_SECS", "not-a-duration"),
    ] {
        let output = run_cli_with_env(&["server"], &[(variable, value)]);
        let stderr = String::from_utf8_lossy(&output.stderr);

        assert!(!output.status.success(), "{variable} was ignored");
        assert!(stderr.contains(&format!("invalid value for {variable}")));
        assert!(!stderr.contains(value));
    }
}

#[test]
fn server_cli_values_override_invalid_environment_values() {
    let mut command = Command::new(env!("CARGO_BIN_EXE_maestro"));
    command
        .env_clear()
        .env("MAESTRO_BIND_ADDR", "not-a-socket")
        .env("MAESTRO_ASSIGNMENT_TIMEOUT_SECS", "not-a-duration")
        .env("MAESTRO_SCHEDULER_INTERVAL_SECS", "not-a-duration")
        .args([
            "server",
            "--bind-addr",
            "127.0.0.1:0",
            "--assignment-timeout-secs",
            "2",
            "--scheduler-interval-secs",
            "1",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().expect("server process should start");
    let deadline = Instant::now() + Duration::from_secs(2);

    let stayed_running = loop {
        if child
            .try_wait()
            .expect("server process should be observable")
            .is_some()
        {
            break false;
        }
        if Instant::now() >= deadline {
            break true;
        }
        thread::sleep(Duration::from_millis(10));
    };

    if stayed_running {
        child.kill().expect("test server should be stoppable");
        child.wait().expect("test server should exit after kill");
    } else {
        let output = child
            .wait_with_output()
            .expect("early server exit output should be readable");
        panic!(
            "server exited before CLI overrides were applied: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    assert!(stayed_running);
}

#[test]
fn commands_accept_their_configuration_options() {
    for command in ["status", "stop"] {
        assert!(
            run_cli(&[command, "--server-addr", "127.0.0.1:9002"])
                .status
                .success()
        );
    }
}

#[tokio::test]
async fn submit_positional_command_posts_request_and_prints_job_id() {
    let server = SubmitMockServer::start(StatusCode::CREATED, submitted_job_response()).await;
    let address = server.address.to_string();
    let output = tokio::task::spawn_blocking(move || {
        run_cli_owned(vec![
            "submit".into(),
            "--server-addr".into(),
            address,
            "/bin/echo".into(),
            "--help".into(),
            "hello".into(),
            "two words".into(),
        ])
    })
    .await
    .expect("CLI subprocess should finish");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("job_id: {SUBMITTED_JOB_ID}\n")
    );
    assert!(output.stderr.is_empty());
    assert_eq!(
        server.requests(),
        vec![json!({
            "command": "/bin/echo",
            "args": ["--help", "hello", "two words"],
            "env": null,
            "working_dir": null
        })]
    );

    server.stop().await;
}

#[tokio::test]
async fn submit_spec_file_prints_json_job_id() {
    let server = SubmitMockServer::start(StatusCode::CREATED, submitted_job_response()).await;
    let spec_path = unique_temp_path();
    let spec_path_text = spec_path.to_string_lossy().into_owned();
    let spec = r#"{"command":"/bin/echo","args":["from file"],"env":{"TOKEN":"private-value"},"working_dir":"/tmp"}"#;
    std::fs::write(&spec_path, spec).expect("spec file should be writable");
    let address = server.address.to_string();
    let output = tokio::task::spawn_blocking(move || {
        run_cli_owned(vec![
            "submit".into(),
            "--server-addr".into(),
            address,
            "--spec".into(),
            spec_path_text,
            "--json".into(),
        ])
    })
    .await
    .expect("CLI subprocess should finish");

    assert_eq!(output.status.code(), Some(0));
    assert_eq!(output.stderr, b"");
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!({"job_id": SUBMITTED_JOB_ID})
    );
    assert_eq!(
        server.requests(),
        vec![json!({
            "command": "/bin/echo",
            "args": ["from file"],
            "env": {"TOKEN": "private-value"},
            "working_dir": "/tmp"
        })]
    );

    std::fs::remove_file(spec_path).expect("spec file should be removable");
    server.stop().await;
}

#[tokio::test]
async fn submit_api_error_returns_nonzero_and_safe_stderr() {
    let server = SubmitMockServer::start(
        StatusCode::BAD_REQUEST,
        json!({
            "code": "invalid_input",
            "message": "request contains invalid input",
            "details": null
        }),
    )
    .await;
    let address = server.address.to_string();
    let expected_address = address.clone();
    let output = tokio::task::spawn_blocking(move || {
        run_cli_owned(vec![
            "submit".into(),
            "--server-addr".into(),
            address,
            "--".into(),
            "/bin/echo".into(),
        ])
    })
    .await
    .expect("CLI subprocess should finish");
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(stderr.contains("request contains invalid input"));
    assert!(stderr.contains("invalid_input"));
    assert!(stderr.contains("400"));
    assert!(!stderr.contains(&expected_address));
    assert!(!stderr.contains("http://"));

    server.stop().await;
}

#[test]
fn submit_rejects_invalid_spec_file_without_echoing_input() {
    let spec_path = unique_temp_path();
    let secret = "private-spec-content";
    std::fs::write(&spec_path, format!(r#"{{"command":"{secret}","args":["#))
        .expect("invalid spec file should be writable");
    let path = spec_path.to_string_lossy().into_owned();
    let output = run_cli(&["submit", "--spec", &path]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr.contains("job specification file is invalid"));
    assert!(!stderr.contains(secret));
    assert!(!stderr.contains(&path));
    std::fs::remove_file(spec_path).expect("spec file should be removable");
}

#[test]
fn submit_rejects_missing_spec_file_without_echoing_path() {
    let spec_path = unique_temp_path();
    let path = spec_path.to_string_lossy().into_owned();
    let output = run_cli(&["submit", "--spec", &path]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr.contains("could not read job specification file"));
    assert!(!stderr.contains(&path));
}

#[test]
fn submit_rejects_oversized_spec_file_before_http_request() {
    let spec_path = unique_temp_path();
    let spec = format!(r#"{{"command":"echo"}}{}"#, " ".repeat(1024 * 1024 + 1));
    std::fs::write(&spec_path, spec).expect("oversized spec file should be writable");
    let path = spec_path.to_string_lossy().into_owned();
    let output = run_cli(&["submit", "--server-addr", "127.0.0.1:0", "--spec", &path]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr.contains("job specification file exceeds the 1 MiB limit"));
    assert!(!stderr.contains(&path));
    assert!(!stderr.contains("request to control plane"));
    std::fs::remove_file(spec_path).expect("spec file should be removable");
}

#[test]
fn submit_rejects_non_regular_spec_file_without_echoing_path() {
    let spec_path = unique_temp_path();
    std::fs::create_dir_all(&spec_path).expect("spec directory should be writable");
    let path = spec_path.to_string_lossy().into_owned();
    let output = run_cli(&["submit", "--spec", &path]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr.contains("job specification must be a regular file"));
    assert!(!stderr.contains(&path));
    std::fs::remove_dir_all(spec_path).expect("spec directory should be removable");
}

#[test]
fn submit_validates_command_before_http_request() {
    let output = run_cli(&["submit", "--server-addr", "127.0.0.1:0", "--", ""]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr.contains("command must not be empty"));
    assert!(!stderr.contains("request to control plane"));
}

#[test]
fn submit_rejects_spec_with_positional_command() {
    let spec_path = unique_temp_path();
    let path = spec_path.to_string_lossy().into_owned();
    let output = run_cli(&["submit", "--spec", &path, "--", "/bin/true"]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert_eq!(output.status.code(), Some(2));
    assert!(stderr.contains("invalid command-line arguments"));
    assert!(!stderr.contains(&path));
}

#[test]
fn environment_values_are_used_and_cli_values_take_precedence() {
    let invalid_environment = [("MAESTRO_SERVER_ADDR", "not-a-socket")];
    let output = run_cli_with_env(&["status"], &invalid_environment);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("invalid value for MAESTRO_SERVER_ADDR"));
    assert!(!stderr.contains("not-a-socket"));

    let output = run_cli_with_env(
        &["status", "--server-addr", "127.0.0.1:9003"],
        &invalid_environment,
    );
    assert!(output.status.success());
}

#[test]
fn rejects_zero_intervals_and_invalid_socket_addresses() {
    let zero_interval = run_cli(&["agent", "--heartbeat-interval-secs", "0"]);
    let zero_interval_error = String::from_utf8_lossy(&zero_interval.stderr);
    assert!(!zero_interval.status.success());
    assert!(zero_interval_error.contains("invalid configuration value"));

    let invalid_address = run_cli(&["server", "--bind-addr", "not-a-socket"]);
    let invalid_address_error = String::from_utf8_lossy(&invalid_address.stderr);
    assert!(!invalid_address.status.success());
    assert!(invalid_address_error.contains("invalid configuration value"));
    assert!(!invalid_address_error.contains("not-a-socket"));
}

#[test]
fn creates_a_missing_agent_working_directory() {
    let working_dir = unique_temp_path();
    let path = working_dir.to_string_lossy().into_owned();
    let output = run_cli(&[
        "agent",
        "--server-addr",
        "127.0.0.1:0",
        "--working-dir",
        &path,
    ]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("agent startup failed"));
    assert!(working_dir.is_dir());
    std::fs::remove_dir_all(working_dir).expect("test directory should be removable");
}

#[test]
fn rejects_a_file_as_agent_working_directory_without_echoing_its_path() {
    let file_path = unique_temp_path();
    std::fs::write(&file_path, "test").expect("test file should be writable");
    let path = file_path.to_string_lossy().into_owned();
    let output = run_cli(&["agent", "--working-dir", &path]);
    let stderr = String::from_utf8_lossy(&output.stderr);

    assert!(!output.status.success());
    assert!(stderr.contains("agent working directory must be a directory"));
    assert!(!stderr.contains(&path));
    std::fs::remove_file(file_path).expect("test file should be removable");
}
