use super::*;
use crate::{config::AgentConfig, worker::client::AgentHttpClient};

#[tokio::test]
async fn stop_route_delivers_sticky_target_and_handles_statuses_and_ownership() {
    let (store, job, agent, allocation) = agent_assignment_store();
    let foreign = AgentId::new();
    store
        .register_agent(Agent::new(
            foreign,
            "other".into(),
            Timestamp::from_unix_millis(1000),
        ))
        .unwrap();
    let (address, stop, server) =
        start_test_router_with_timeout(store, Duration::from_millis(u64::MAX)).await;
    let client = AgentHttpClient::new(address, Duration::from_secs(2)).unwrap();
    assert_eq!(client.poll_stop(agent, allocation).await.unwrap(), None);
    let response = http_request(address, "POST", &format!("/v1/jobs/{job}/stop"), "").await;
    assert_eq!(response_status(&response), 202);
    let command = client.poll_stop(agent, allocation).await.unwrap().unwrap();
    assert_eq!(
        (command.job_id, command.agent_id, command.allocation_id),
        (job, agent, allocation)
    );
    assert_eq!(
        client.poll_stop(agent, allocation).await.unwrap(),
        Some(command)
    );
    for (path, status) in [
        (format!("/v1/agents/bad/allocations/{allocation}/stop"), 400),
        (format!("/v1/agents/{agent}/allocations/bad/stop"), 400),
        (
            format!(
                "/v1/agents/{}/allocations/{allocation}/stop",
                AgentId::new()
            ),
            404,
        ),
        (
            format!(
                "/v1/agents/{agent}/allocations/{}/stop",
                AllocationId::new()
            ),
            404,
        ),
        (
            format!("/v1/agents/{foreign}/allocations/{allocation}/stop"),
            409,
        ),
    ] {
        assert_eq!(
            response_status(&http_request(address, "GET", &path, "").await),
            status
        );
    }
    stop.send(()).unwrap();
    server.await.unwrap().unwrap();
}

#[tokio::test]
async fn completed_allocation_does_not_deliver_old_stop() {
    let (store, job, agent, allocation) = agent_assignment_store();
    store
        .append_event(&crate::domain::JobEvent::Started {
            job_id: job,
            agent_id: agent,
            allocation_id: allocation,
            occurred_at: Timestamp::from_unix_millis(2000),
        })
        .unwrap();
    let (address, stop, server) =
        start_test_router_with_timeout(store.clone(), Duration::from_secs(10)).await;
    assert_eq!(
        response_status(&http_request(address, "POST", &format!("/v1/jobs/{job}/stop"), "").await),
        202
    );
    let client = AgentHttpClient::new(address, Duration::from_secs(2)).unwrap();
    assert!(client.poll_stop(agent, allocation).await.unwrap().is_some());

    let finished = json!({
        "type":"finished", "job_id":job, "agent_id":agent, "occurred_at":3000,
        "result":{"type":"exited", "exit_code":0},
    });
    let response = http_request(
        address,
        "POST",
        &format!("/v1/allocations/{allocation}/events"),
        &finished.to_string(),
    )
    .await;
    assert_eq!(response_status(&response), 200);
    assert_eq!(response_json(&response)["outcome"], "applied");
    assert_eq!(client.poll_stop(agent, allocation).await.unwrap(), None);
    assert_eq!(store.get_job(job).unwrap().status(), JobStatus::Succeeded);
    let late_stop = http_request(address, "POST", &format!("/v1/jobs/{job}/stop"), "").await;
    assert_eq!(response_status(&late_stop), 409);
    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn concurrent_http_stop_and_completion_keep_the_recorded_result() {
    let (store, job, agent, allocation) = agent_assignment_store();
    let (address, stop, server) =
        start_test_router_with_timeout(store.clone(), Duration::from_millis(u64::MAX)).await;
    let event_path = format!("/v1/allocations/{allocation}/events");
    let job_stop_path = format!("/v1/jobs/{job}/stop");
    let started = json!({"type":"started", "job_id":job, "agent_id":agent, "occurred_at":2000});
    assert_eq!(
        response_status(&http_request(address, "POST", &event_path, &started.to_string()).await),
        200
    );
    let finished = json!({
        "type":"finished", "job_id":job, "agent_id":agent, "occurred_at":3000,
        "result":{"type":"exited", "exit_code":0},
    })
    .to_string();
    let (stop_response, finished_response) = tokio::join!(
        http_request(address, "POST", &job_stop_path, ""),
        http_request(address, "POST", &event_path, &finished),
    );
    match response_status(&stop_response) {
        202 => assert_eq!(
            response_json(&stop_response),
            json!({"outcome":"requested", "job_id":job, "agent_id":agent, "allocation_id":allocation})
        ),
        409 => assert_eq!(response_json(&stop_response)["code"], "conflict"),
        actual => panic!("unexpected stop status {actual}: {stop_response}"),
    }
    assert_eq!(response_status(&finished_response), 200);
    assert_eq!(response_json(&finished_response)["outcome"], "applied");

    let job_after = store.get_job(job).unwrap();
    let allocation_after = store.get_allocation(allocation).unwrap();
    assert_eq!(job_after.status(), JobStatus::Succeeded);
    assert_eq!(
        job_after.terminal_result(),
        Some(crate::domain::TerminalResult::Exited { exit_code: 0 })
    );
    assert_eq!(
        allocation_after.status(),
        crate::domain::AllocationStatus::Succeeded
    );
    assert_eq!(store.poll_stop(agent, allocation).unwrap(), None);
    let agent_after = store.get_agent(agent).unwrap();
    assert_eq!(
        agent_after.availability(),
        crate::domain::AgentAvailability::Idle
    );

    let late_stop = http_request(address, "POST", &job_stop_path, "").await;
    assert_eq!(response_status(&late_stop), 409);
    assert_eq!(response_json(&late_stop)["code"], "conflict");
    let duplicate = http_request(address, "POST", &event_path, &finished).await;
    assert_eq!(response_status(&duplicate), 200);
    assert_eq!(response_json(&duplicate)["outcome"], "duplicate");
    assert_eq!(store.get_job(job).unwrap(), job_after);
    assert_eq!(store.get_allocation(allocation).unwrap(), allocation_after);
    assert_eq!(store.get_agent(agent).unwrap(), agent_after);
    assert_eq!(store.poll_stop(agent, allocation).unwrap(), None);

    stop.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

pub(super) async fn submit_wait(
    address: SocketAddr,
    directory: &std::path::Path,
    mode: &str,
) -> JobId {
    let request = json!({"command":std::env::current_exe().unwrap(),"args":["--exact","worker::lifecycle::tests::process_fixture","--ignored","--nocapture"],"env":{"MAESTRO_M67_MODE":mode,"MAESTRO_M67_CODE":"0","MAESTRO_M67_READY":directory.join("ready"),"MAESTRO_M67_RELEASE":directory.join("release")}});
    let response = reqwest::Client::new()
        .post(format!("http://{address}/v1/jobs"))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::CREATED);
    response.json::<Value>().await.unwrap()["id"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap()
}
pub(super) async fn await_status(store: &StateStore, id: JobId, status: JobStatus) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while store.get_job(id).unwrap().status() != status {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn stops_only_requested_job_while_other_child_finishes_normally_and_agent_is_reusable() {
    let store = StateStore::default();
    let (address, server_stop, server) =
        start_test_router_with_timeout(store.clone(), Duration::from_secs(10)).await;
    let root = std::env::temp_dir().join(format!("maestro-m68-{}", JobId::new()));
    std::fs::create_dir(&root).unwrap();
    let mut stops = Vec::new();
    let mut agents = Vec::new();
    for name in ["first", "second"] {
        std::fs::create_dir(root.join(name)).unwrap();
        let (stop, shutdown) = oneshot::channel();
        stops.push(stop);
        agents.push(tokio::spawn(crate::worker::agent::run_with_shutdown(
            AgentConfig {
                server_addr: address,
                agent_address: format!("m68-{name}"),
                heartbeat_interval: Duration::from_millis(10),
                poll_interval: Duration::from_millis(5),
                working_dir: root.join(name),
            },
            async {
                let _ = shutdown.await;
            },
        )));
    }
    let scenario = async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while store.list_agents().unwrap().len() != 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let first = submit_wait(address, &root.join("first"), "wait").await;
        let second = submit_wait(address, &root.join("second"), "wait").await;
        let scheduler = Scheduler::new(store.clone());
        for _ in 0..2 {
            scheduler
                .tick(
                    super::super::current_timestamp().unwrap(),
                    Duration::from_secs(10),
                )
                .unwrap();
        }
        await_status(&store, first, JobStatus::Running).await;
        await_status(&store, second, JobStatus::Running).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while !root.join("first/ready").exists() || !root.join("second/ready").exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let before = store.list_agents().unwrap();
        assert_eq!(
            response_status(
                &http_request(address, "POST", &format!("/v1/jobs/{first}/stop"), "").await
            ),
            202
        );
        await_status(&store, first, JobStatus::Stopped).await;
        assert_eq!(store.get_job(second).unwrap().status(), JobStatus::Running);
        // A still-live second child must observe release and exit 0. A stray
        // kill would produce failure, so this checks actual process isolation.
        std::fs::write(root.join("second/release"), "release").unwrap();
        await_status(&store, second, JobStatus::Succeeded).await;
        for old in before {
            assert!(
                store.get_agent(old.id()).unwrap().last_heartbeat_at() >= old.last_heartbeat_at()
            );
        }
        let third = submit_wait(address, &root, "exit").await;
        scheduler
            .tick(
                super::super::current_timestamp().unwrap(),
                Duration::from_secs(10),
            )
            .unwrap();
        await_status(&store, third, JobStatus::Succeeded).await;
    };
    let result = tokio::time::timeout(Duration::from_secs(15), scenario).await;
    for stop in stops {
        let _ = stop.send(());
    }
    for agent in agents {
        tokio::time::timeout(Duration::from_secs(5), agent)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
    server_stop.send(()).unwrap();
    server.await.unwrap().unwrap();
    std::fs::remove_dir_all(root).unwrap();
    result.unwrap();
}
