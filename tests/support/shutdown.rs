use super::*;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
};

#[tokio::test]
async fn server_signals_close_listener_and_drain_an_in_flight_request() {
    for (signal, name) in [
        (SignalKind::interrupt(), "sigint"),
        (SignalKind::terminate(), "sigterm"),
    ] {
        let mut harness = Harness::start_with_args(
            Duration::from_secs(20),
            &["--scheduler-interval-secs", "60"],
        )
        .await
        .unwrap();
        let address = harness.address();
        let root = harness.root.0.clone();
        let server_pid = harness.server.child.id().unwrap();

        timeout_at(harness.deadline, async {
            let mut connection = BufReader::new(TcpStream::connect(address).await.unwrap());
            let body = br#"{"command":"test-shutdown"}"#;
            let headers = format!(
                "POST /v1/jobs HTTP/1.1\r\nHost: {address}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nExpect: 100-continue\r\n\r\n",
                body.len()
            );
            connection
                .get_mut()
                .write_all(headers.as_bytes())
                .await
                .unwrap();

            let mut line = String::new();
            connection.read_line(&mut line).await.unwrap();
            assert_eq!(line, "HTTP/1.1 100 Continue\r\n");
            loop {
                line.clear();
                assert_ne!(connection.read_line(&mut line).await.unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
            }

            assert!(send_signal(server_pid, signal.as_raw_value()));
            loop {
                harness.server.check_deadline(harness.deadline, "server listener closure").unwrap();
                match TcpStream::connect(address).await {
                    Ok(_) => pause_until(harness.deadline).await,
                    Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => break,
                    Err(error) => panic!("unexpected listener error: {error}"),
                }
            }
            harness.server.check_running().unwrap();

            // Complete the accepted request after the listener has closed.
            connection.get_mut().write_all(body).await.unwrap();
            let mut response = String::new();
            connection.read_to_string(&mut response).await.unwrap();
            let (headers, json) = response.split_once("\r\n\r\n").unwrap();
            assert!(headers.starts_with("HTTP/1.1 201 Created"), "{response}");
            let job: Value = serde_json::from_str(json).unwrap();
            assert_eq!(job["status"], "pending");
            assert_eq!(job["spec"]["command"], "test-shutdown");
            Uuid::parse_str(job["id"].as_str().unwrap()).unwrap();

            assert!(harness.server.child.wait().await.unwrap().success());
        })
        .await
        .unwrap();

        let stderr = harness.server.stderr().unwrap();
        assert!(
            stderr.lines().any(|line| line.contains(&format!(
                r#"event="shutdown_signal_received" signal="{name}""#
            ))),
            "{stderr}"
        );
        assert!(stderr.contains(r#"event="server_stopped""#), "{stderr}");
        assert!(!send_signal(server_pid, 0));
        drop(harness);
        assert!(!root.exists());
    }
}

#[tokio::test]
async fn agent_sigterm_reaps_active_job_and_leaves_queued_job_unstarted() {
    let mut harness = Harness::start(Duration::from_secs(25)).await.unwrap();
    let agent_id = harness.start_agent().await.unwrap();
    let root = harness.root.0.clone();
    let server_pid = harness.server.child.id().unwrap();
    let releases = (
        ReleaseProcess(harness.working_dir().join("shutdown-active.release")),
        ReleaseProcess(harness.working_dir().join("shutdown-queued.release")),
    );
    let active_id = crate::submit_fixture(&mut harness, "shutdown-active", "wait").await;
    let running = harness
        .wait_for_status(&active_id, "running")
        .await
        .unwrap();
    let active_marker = harness.working_dir().join("shutdown-active.started");
    let child_pid = timeout_at(harness.deadline, async {
        loop {
            harness
                .server
                .check_deadline(harness.deadline, "active job PID")
                .unwrap();
            if let Some(pid) = fs::read_to_string(&active_marker)
                .ok()
                .and_then(|text| text.parse::<u32>().ok())
                .filter(|pid| *pid > 0)
            {
                break pid;
            }
            pause_until(harness.deadline).await;
        }
    })
    .await
    .unwrap();
    let queued_id = crate::submit_fixture(&mut harness, "shutdown-queued", "wait").await;
    assert_eq!(
        harness.status(&queued_id).await.unwrap()["status"],
        "pending"
    );

    let agent = harness.agent.as_mut().unwrap();
    assert_eq!(
        agent
            .wait_for_field(harness.deadline, "agent_start_acknowledged", "job_id")
            .await
            .unwrap(),
        active_id
    );
    let agent_pid = agent.child.id().unwrap();
    assert!(send_signal(
        agent_pid,
        SignalKind::terminate().as_raw_value()
    ));
    let status = timeout_at(harness.deadline, agent.child.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(status.success(), "{status}");
    assert!(!send_signal(agent_pid, 0));
    // Check before releasing the fixtures so fallback cannot hide a missed reap.
    assert!(!send_signal(child_pid, 0));

    let stopped = harness.status(&active_id).await.unwrap();
    crate::assert_terminal_job(
        &stopped,
        &active_id,
        agent_id,
        "stopped",
        serde_json::json!({"type":"stopped"}),
    );
    assert_eq!(stopped["allocation"]["id"], running["allocation"]["id"]);
    let queued = harness.status(&queued_id).await.unwrap();
    assert!(matches!(
        queued["status"].as_str(),
        Some("pending" | "assigned")
    ));
    assert!(queued["started_at"].is_null());
    assert!(
        !harness
            .working_dir()
            .join("shutdown-queued.started")
            .exists()
    );

    let stderr = harness.server.stderr().unwrap();
    let job_field = format!("job_id={active_id}");
    let mut terminal_events = stderr.lines().filter(|line| {
        line.contains(r#"event="job_terminal""#)
            && line.split_whitespace().any(|field| field == job_field)
    });
    let terminal = terminal_events.next().unwrap();
    assert!(terminal.contains(r#"reason="agent_shutdown""#));
    assert!(terminal_events.next().is_none());

    drop(releases);
    drop(harness);
    assert!(!send_signal(server_pid, 0));
    assert!(!root.exists());
}
