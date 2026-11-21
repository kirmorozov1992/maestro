use super::*;

struct ReleaseProcess(PathBuf);

impl Drop for ReleaseProcess {
    fn drop(&mut self) {
        let _ = fs::write(&self.0, b"");
    }
}

#[tokio::test]
async fn server_loss_interrupts_heartbeat_and_terminal_reporting() {
    let mut harness = Harness::start(Duration::from_secs(25)).await.unwrap();
    let agent_id = harness
        .start_agent_with_args(&["--heartbeat-interval-secs", "1"])
        .await
        .unwrap();
    let job_id = crate::submit_fixture(&mut harness, "outage", "wait").await;
    let job = harness.wait_for_status(&job_id, "running").await.unwrap();
    let release = ReleaseProcess(harness.working_dir().join("outage.release"));
    let started = harness.working_dir().join("outage.started");
    while !started.is_file() {
        harness
            .server
            .check_deadline(harness.deadline, "outage fixture")
            .unwrap();
        pause_until(harness.deadline).await;
    }
    let before = harness.agent.as_ref().unwrap().stderr().unwrap();
    assert!(
        !before.contains(r#"event="agent_disconnected""#),
        "{before}"
    );

    harness.server.stop();
    assert!(harness.server.child.try_wait().unwrap().unwrap().success());

    let agent = harness.agent.as_mut().unwrap();
    assert_eq!(
        agent
            .wait_for_field(harness.deadline, "agent_disconnected", "agent_id")
            .await
            .unwrap(),
        agent_id.to_string()
    );

    fs::write(&release.0, b"").unwrap();
    assert_eq!(
        agent
            .wait_for_field(harness.deadline, "agent_lifecycle_report_failed", "job_id")
            .await
            .unwrap(),
        job_id
    );

    agent.stop();
    assert!(agent.child.try_wait().unwrap().unwrap().success());
    let stderr = agent.stderr().unwrap();
    let disconnected = stderr[before.len()..]
        .lines()
        .find(|line| line.contains(r#"event="agent_disconnected""#))
        .expect("heartbeat should detect the outage");
    assert!(disconnected.contains("error=transport"), "{disconnected}");
    let mut failures = stderr
        .lines()
        .filter(|line| line.contains(r#"event="agent_lifecycle_report_failed""#));
    let failure = failures.next().expect("terminal report must fail offline");
    assert!(failures.next().is_none(), "{stderr}");
    assert!(failure.contains(&format!(
        "allocation_id={}",
        job["allocation"]["id"].as_str().unwrap()
    )));
    assert!(failure.contains(&format!("agent_id={agent_id}")));
    assert!(failure.contains("error=Transport"));
    assert!(stderr.contains(r#"event="agent_shutdown""#));
}

#[tokio::test]
async fn disappearance_after_assignment_marks_unstarted_job_lost() {
    let mut harness =
        Harness::start_with_args(Duration::from_secs(25), &["--liveness-timeout-secs", "10"])
            .await
            .unwrap();
    let agent_id = harness
        .start_agent_with_args(&[
            "--heartbeat-interval-secs",
            "1",
            "--poll-interval-secs",
            "60",
        ])
        .await
        .unwrap();
    let job_id = crate::submit_fixture(&mut harness, "unstarted", "success").await;
    let assigned = harness.wait_for_status(&job_id, "assigned").await.unwrap();
    let allocation_id = &assigned["allocation"]["id"];
    assert!(allocation_id.as_str().is_some());
    assert_eq!(assigned["allocation"]["agent_id"], agent_id.to_string());

    let agent = harness.agent.as_mut().unwrap();
    agent.stop();
    assert!(agent.child.try_wait().unwrap().unwrap().success());

    let after_exit = harness.status(&job_id).await.unwrap();
    assert_eq!(after_exit["status"], "assigned");
    assert_eq!(after_exit["allocation"], assigned["allocation"]);

    let lost = harness.wait_for_status(&job_id, "lost").await.unwrap();
    assert_eq!(lost["terminal_result"], serde_json::json!({"type":"lost"}));
    assert_eq!(lost["allocation"]["id"], *allocation_id);
    assert_eq!(lost["allocation"]["status"], "lost");
    assert_eq!(lost["allocation"]["agent_id"], agent_id.to_string());
    assert!(lost["started_at"].is_null() && lost["allocation"]["started_at"].is_null());
    assert!(!harness.working_dir().join("unstarted.started").exists());
    assert_eq!(
        harness.terminal_log_field("reason").await.unwrap(),
        r#""heartbeat_expired""#
    );
}
