use std::{
    path::PathBuf,
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    thread,
    time::{Duration, Instant},
};

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
    for command in ["agent", "submit", "status", "stop"] {
        let output = run_cli(&[command]);
        let stdout = String::from_utf8_lossy(&output.stdout);

        assert!(output.status.success(), "{command} returned an error");
        assert!(stdout.contains(&format!("maestro {command} mode is not implemented yet")));
    }
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
    let temp_dir = std::env::temp_dir().to_string_lossy().into_owned();
    let agent_args = [
        "agent",
        "--server-addr",
        "127.0.0.1:9000",
        "--heartbeat-interval-secs",
        "3",
        "--poll-interval-secs",
        "2",
        "--working-dir",
        temp_dir.as_str(),
    ];
    assert!(run_cli(&agent_args).status.success());
    for command in ["submit", "status", "stop"] {
        assert!(
            run_cli(&[command, "--server-addr", "127.0.0.1:9002"])
                .status
                .success()
        );
    }
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
    let output = run_cli(&["agent", "--working-dir", &path]);

    assert!(output.status.success());
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
