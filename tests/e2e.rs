#![cfg(unix)]

#[path = "support/e2e.rs"]
mod support;

use std::time::Duration;

use serde_json::json;
use support::Harness;

#[tokio::test]
async fn starts_isolated_stands_and_registers_agents() {
    let (first, second) = tokio::join!(
        Harness::start(Duration::from_secs(20)),
        Harness::start(Duration::from_secs(20))
    );
    let mut first = first.unwrap();
    let mut second = second.unwrap();
    let first_root = first.working_dir().to_owned();
    let second_root = second.working_dir().to_owned();

    assert_ne!(first.address(), second.address());
    assert_ne!(first_root, second_root);
    assert!(first_root.is_dir() && second_root.is_dir());

    let (first_agent, second_agent) = tokio::join!(first.start_agent(), second.start_agent());
    assert_ne!(first_agent.unwrap(), second_agent.unwrap());
    assert_eq!(
        first.cli_json(&["status", "--json"]).await.unwrap(),
        json!({"items": []})
    );

    drop(second);
    drop(first);
    assert!(!first_root.exists() && !second_root.exists());
}

const TEST_TIMEOUT: Duration = Duration::from_secs(25);
// The server's default scheduler interval is one second.
const SCHEDULER_WINDOW: Duration = Duration::from_millis(1_100);

async fn submit_fixture(harness: &mut Harness, name: &str, mode: &str) -> String {
    let spec_path = harness.working_dir().join(format!("{name}.json"));
    let mut spec = json!({
        "command": std::env::current_exe().unwrap().to_str().unwrap(),
        "args": ["--exact", "job_process_fixture", "--ignored"],
        "env": {"MAESTRO_E2E_JOB": name, "MAESTRO_E2E_MODE": mode}
    });
    if mode == "log" {
        spec["args"]
            .as_array_mut()
            .unwrap()
            .push(json!("--nocapture"));
        spec["env"]["MAESTRO_E2E_SECRET_TOKEN"] = json!("m81-secret-env-value");
    }
    std::fs::write(&spec_path, spec.to_string()).unwrap();
    let submitted = harness
        .cli_json(&["submit", "--json", "--spec", spec_path.to_str().unwrap()])
        .await
        .unwrap();
    let id = submitted["job_id"]
        .as_str()
        .expect("submit returns a job UUID");
    uuid::Uuid::parse_str(id).expect("job UUID is valid");
    id.to_owned()
}

#[test]
#[ignore = "invoked as a child by the Maestro e2e job tests"]
fn job_process_fixture() {
    use std::{fs, thread, time::Instant};

    let name = std::env::var("MAESTRO_E2E_JOB").expect("job fixture name");
    let mode = std::env::var("MAESTRO_E2E_MODE").expect("job fixture mode");
    let marker = std::env::current_dir()
        .unwrap()
        .join(format!("{name}.started"));
    if mode == "log" {
        println!("m81-private-stdout {}", "x".repeat(70_000));
        eprintln!("m81-private-stderr");
        assert_eq!(
            std::env::var("MAESTRO_E2E_SECRET_TOKEN").unwrap(),
            "m81-secret-env-value"
        );
    }
    fs::write(&marker, std::process::id().to_string()).unwrap();
    match mode.as_str() {
        "success" => {}
        "fail" => std::process::exit(7),
        "wait" | "log" => {
            let release = std::path::PathBuf::from(format!("{name}.release"));
            let deadline = Instant::now() + Duration::from_secs(30);
            // The harness removes the marker on teardown if the agent
            // exits before reaping this process.
            while marker.exists() && !release.exists() {
                assert!(Instant::now() < deadline, "e2e fixture was not released");
                thread::sleep(Duration::from_millis(20));
            }
        }
        _ => panic!("unknown job fixture mode"),
    }
}

fn assert_terminal_job(
    job: &serde_json::Value,
    id: &str,
    agent: uuid::Uuid,
    status: &str,
    result: serde_json::Value,
) {
    assert_eq!(job["id"], id);
    assert_eq!(job["status"], status);
    assert_eq!(job["terminal_result"], result);
    assert_eq!(job["allocation"]["job_id"], id);
    assert_eq!(job["allocation"]["agent_id"], agent.to_string());
    assert_eq!(job["allocation"]["status"], status);
    let started = job["started_at"].as_u64().expect("job start timestamp");
    let finished = job["finished_at"].as_u64().expect("job finish timestamp");
    assert!(finished >= started);
    assert_eq!(job["allocation"]["started_at"], started);
    assert_eq!(job["allocation"]["finished_at"], finished);
}

#[tokio::test]
async fn submit_assign_run_succeeds() {
    let mut harness = Harness::start(TEST_TIMEOUT).await.unwrap();
    let agent = harness.start_agent().await.unwrap();
    let id = submit_fixture(&mut harness, "success", "success").await;

    let job = harness.wait_for_status(&id, "succeeded").await.unwrap();
    assert_terminal_job(
        &job,
        &id,
        agent,
        "succeeded",
        json!({"type":"exited","exit_code":0}),
    );
    assert!(harness.working_dir().join("success.started").is_file());
}

#[tokio::test]
async fn nonzero_exit_fails_the_job() {
    let mut harness = Harness::start(TEST_TIMEOUT).await.unwrap();
    let agent = harness.start_agent().await.unwrap();
    let id = submit_fixture(&mut harness, "failure", "fail").await;

    let job = harness.wait_for_status(&id, "failed").await.unwrap();
    assert_terminal_job(
        &job,
        &id,
        agent,
        "failed",
        json!({"type":"exited","exit_code":7}),
    );
    assert!(harness.working_dir().join("failure.started").is_file());
    assert_eq!(harness.terminal_log_field("exit_code").await.unwrap(), "7");
}

#[tokio::test]
async fn cli_stop_stops_a_running_job() {
    let mut harness = Harness::start(TEST_TIMEOUT).await.unwrap();
    let agent = harness.start_agent().await.unwrap();
    let id = submit_fixture(&mut harness, "stop", "wait").await;
    let running = harness.wait_for_status(&id, "running").await.unwrap();

    let stop = harness.cli_json(&["stop", "--json", &id]).await.unwrap();
    assert_eq!(stop["outcome"], "requested");
    assert_eq!(stop["job_id"], id);
    assert_eq!(stop["agent_id"], agent.to_string());
    assert_eq!(stop["allocation_id"], running["allocation"]["id"]);

    let stopped = harness.wait_for_status(&id, "stopped").await.unwrap();
    assert_terminal_job(&stopped, &id, agent, "stopped", json!({"type":"stopped"}));
    assert_eq!(stopped["allocation"]["id"], running["allocation"]["id"]);
    assert_eq!(
        harness.terminal_log_field("reason").await.unwrap(),
        "\"user_requested\""
    );
}

#[tokio::test]
async fn expired_heartbeat_marks_a_running_job_lost() {
    let mut harness = Harness::start_with_args(TEST_TIMEOUT, &["--liveness-timeout-secs", "10"])
        .await
        .unwrap();
    // Skip heartbeats for the whole test, while keeping the real agent
    // alive so teardown can reap the fixture process normally.
    let agent = harness
        .start_agent_with_args(&["--heartbeat-interval-secs", "60"])
        .await
        .unwrap();
    let id = submit_fixture(&mut harness, "heartbeat", "wait").await;
    let running = harness.wait_for_status(&id, "running").await.unwrap();

    let job = harness.wait_for_status(&id, "lost").await.unwrap();
    assert_terminal_job(&job, &id, agent, "lost", json!({"type":"lost"}));
    assert_eq!(job["allocation"]["id"], running["allocation"]["id"]);
    assert_eq!(
        harness.terminal_log_field("reason").await.unwrap(),
        "\"heartbeat_expired\""
    );
}

async fn assert_pending_across_scheduler_interval(harness: &mut Harness, id: &str) {
    for delay in [Duration::ZERO, SCHEDULER_WINDOW] {
        if !delay.is_zero() {
            tokio::time::sleep(delay).await;
        }
        let job = harness.status(id).await.unwrap();
        assert_eq!(job["status"], "pending");
        assert_eq!(job["allocation"], serde_json::Value::Null);
        assert_eq!(job["started_at"], serde_json::Value::Null);
        assert_eq!(job["terminal_result"], serde_json::Value::Null);
    }
}

#[tokio::test]
async fn job_without_an_agent_stays_pending() {
    let mut harness = Harness::start(TEST_TIMEOUT).await.unwrap();
    let id = submit_fixture(&mut harness, "pending", "success").await;

    assert_pending_across_scheduler_interval(&mut harness, &id).await;
    assert!(!harness.working_dir().join("pending.started").exists());
}

#[tokio::test]
async fn jobs_receive_distinct_sequential_allocations() {
    let mut harness = Harness::start(TEST_TIMEOUT).await.unwrap();
    let agent = harness.start_agent().await.unwrap();
    let first_id = submit_fixture(&mut harness, "first", "wait").await;
    let running = harness.wait_for_status(&first_id, "running").await.unwrap();
    let second_id = submit_fixture(&mut harness, "second", "success").await;

    assert_pending_across_scheduler_interval(&mut harness, &second_id).await;
    assert!(!harness.working_dir().join("second.started").exists());
    assert_eq!(
        harness.status(&first_id).await.unwrap()["status"],
        "running"
    );

    std::fs::write(harness.working_dir().join("first.release"), b"").unwrap();
    let first = harness
        .wait_for_status(&first_id, "succeeded")
        .await
        .unwrap();
    let second = harness
        .wait_for_status(&second_id, "succeeded")
        .await
        .unwrap();
    for (job, id) in [(&first, &first_id), (&second, &second_id)] {
        assert_terminal_job(
            job,
            id,
            agent,
            "succeeded",
            json!({"type":"exited","exit_code":0}),
        );
    }
    assert_eq!(first["allocation"]["id"], running["allocation"]["id"]);
    assert_ne!(first["allocation"]["id"], second["allocation"]["id"]);
    assert!(harness.working_dir().join("second.started").is_file());
}
