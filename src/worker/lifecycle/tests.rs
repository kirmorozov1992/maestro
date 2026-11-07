use super::*;
use crate::transport::v1::ProcessResultV1;
use crate::worker::{client::AgentHttpClient, executor::Executor};
use crate::{
    domain::{AllocationId, Timestamp},
    transport::v1::AssignmentResponse,
};
use axum::{Json, Router, extract::State, routing::post};
use serde_json::{Value, json};
use std::{collections::VecDeque, path::PathBuf, sync::Arc, time::Duration};
use tokio::sync::{Mutex, Notify, oneshot};

const AGENT: &str = "550e8400-e29b-41d4-a716-446655440001";
const JOB: &str = "550e8400-e29b-41d4-a716-446655440000";
const ALLOCATION: &str = "550e8400-e29b-41d4-a716-446655440002";

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!("maestro-m67-{}", AllocationId::new()));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[derive(Default)]
struct Recorder {
    events: Mutex<Vec<Value>>,
    replies: Mutex<VecDeque<u16>>,
    notified: Notify,
}
struct Server {
    state: Arc<Recorder>,
    client: AgentHttpClient,
    stop: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    async fn new(replies: &[u16]) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let state = Arc::new(Recorder::default());
        state.replies.lock().await.extend(replies);
        let client =
            AgentHttpClient::new(listener.local_addr().unwrap(), Duration::from_millis(150))
                .unwrap();
        let router = Router::new()
            .route("/v1/allocations/{id}/events", post(record))
            .with_state(state.clone());
        let (stop, rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            axum::serve(listener, router)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await
                .unwrap();
        });
        Self {
            state,
            client,
            stop,
            task,
        }
    }
    async fn events(&self, n: usize) -> Vec<Value> {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let notified = self.state.notified.notified();
                let events = self.state.events.lock().await.clone();
                if events.len() >= n {
                    return events;
                }
                notified.await;
            }
        })
        .await
        .unwrap()
    }
    async fn close(self) {
        self.stop.send(()).unwrap();
        self.task.await.unwrap();
    }
}
async fn record(
    State(state): State<Arc<Recorder>>,
    Json(body): Json<Value>,
) -> (axum::http::StatusCode, Json<Value>) {
    state.events.lock().await.push(body);
    state.notified.notify_one();
    let status = state.replies.lock().await.pop_front().unwrap_or(200);
    if status == 0 {
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    (
        axum::http::StatusCode::from_u16(if status == 0 { 200 } else { status }).unwrap(),
        Json(if status == 200 || status == 0 {
            json!({"outcome":"applied"})
        } else {
            json!({"code":"conflict","message":"private-server-error"})
        }),
    )
}
fn assignment(dir: &TempDir, mode: &str, code: i32) -> AssignmentResponse {
    let exe = std::env::current_exe()
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let args = vec![
        "--exact",
        "worker::lifecycle::tests::process_fixture",
        "--ignored",
        "--nocapture",
    ];
    let env = json!({"MAESTRO_M67_MODE":mode,"MAESTRO_M67_CODE":code.to_string(),"MAESTRO_M67_READY":dir.0.join("ready").to_str().unwrap(),"MAESTRO_M67_RELEASE":dir.0.join("release").to_str().unwrap()});
    serde_json::from_value(json!({
        "job":{"id":JOB,"spec":{"command":exe,"args":args,"env_keys":["MAESTRO_M67_CODE","MAESTRO_M67_MODE","MAESTRO_M67_READY","MAESTRO_M67_RELEASE"],"working_dir":null},"status":"assigned","submitted_at":90,"updated_at":100,"started_at":null,"finished_at":null,"terminal_result":null},
        "allocation":{"id":ALLOCATION,"job_id":JOB,"agent_id":AGENT,"status":"assigned","attempt":1,"assigned_at":100,"started_at":null,"finished_at":null},
        "execution":{"command":exe,"args":args,"env":env,"working_dir":null},"server_time":120
    })).unwrap()
}
#[test]
#[ignore = "child fixture; explicitly executed by lifecycle tests"]
fn process_fixture() {
    let mode = std::env::var("MAESTRO_M67_MODE").unwrap();
    if mode == "wait" {
        // More than a pipe can buffer: reaching ready proves the parent is
        // driving wait/drain, which happens only after accepting started ack.
        use std::io::Write;
        let chunk = [b'o'; 8192];
        let mut stdout = std::io::stdout().lock();
        for _ in 0..256 {
            stdout.write_all(&chunk).unwrap();
        }
        stdout.flush().unwrap();
        drop(stdout);
        std::fs::write(
            std::env::var("MAESTRO_M67_READY").unwrap(),
            std::process::id().to_string(),
        )
        .unwrap();
        let release = PathBuf::from(std::env::var("MAESTRO_M67_RELEASE").unwrap());
        while !release.exists() {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    std::process::exit(std::env::var("MAESTRO_M67_CODE").unwrap().parse().unwrap());
}
async fn run(runner: &mut LifecycleRunner, client: &AgentHttpClient, a: AssignmentResponse) {
    tokio::time::timeout(Duration::from_secs(3), runner.handle(client, a))
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn reports_started_then_real_zero_or_nonzero_exit_once() {
    for code in [0, 7] {
        let dir = TempDir::new();
        let server = Server::new(&[]).await;
        let mut runner = LifecycleRunner::new(Executor::new(&dir.0).unwrap());
        run(&mut runner, &server.client, assignment(&dir, "exit", code)).await;
        runner.shutdown(&server.client).await.unwrap();
        let events = server.events(2).await;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["type"], "started");
        assert_eq!(
            events[1]["result"],
            json!({"type":"exited","exit_code":code})
        );
        assert_eq!(events[0]["job_id"], JOB);
        assert_eq!(events[1]["agent_id"], AGENT);
        assert!(
            events[1]["occurred_at"].as_u64().unwrap()
                >= events[0]["occurred_at"].as_u64().unwrap()
        );
        assert!(events[0]["occurred_at"].as_u64().unwrap() >= 120);
        assert!(
            !serde_json::to_string(&events)
                .unwrap()
                .contains("MAESTRO_M67")
        );
        server.close().await;
    }
}
#[tokio::test]
async fn spawn_failure_reports_only_finished_without_started() {
    let dir = TempDir::new();
    let server = Server::new(&[]).await;
    let mut a = assignment(&dir, "exit", 0);
    let missing = dir.0.join("missing").to_str().unwrap().to_owned();
    a.job.spec.command = missing.clone();
    a.execution.as_mut().unwrap().command = missing;
    let mut runner = LifecycleRunner::new(Executor::new(&dir.0).unwrap());
    run(&mut runner, &server.client, a).await;
    runner.shutdown(&server.client).await.unwrap();
    let events = server.events(1).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0]["type"], "finished");
    assert_eq!(events[0]["result"], json!({"type":"spawn_failed"}));
    server.close().await;
}
#[tokio::test]
async fn shutdown_during_running_child_reaps_and_reports_stopped_once() {
    let dir = TempDir::new();
    let server = Server::new(&[]).await;
    let mut runner = LifecycleRunner::new(Executor::new(&dir.0).unwrap());
    let a = assignment(&dir, "wait", 0);
    {
        let handling = runner.handle(&server.client, a);
        tokio::pin!(handling);
        tokio::select! {r=&mut handling=>panic!("unexpected completion {r:?}"),()=async {server.events(1).await;while !dir.0.join("ready").exists(){tokio::task::yield_now().await;}}=>{}}
    }
    runner.shutdown(&server.client).await.unwrap();
    runner.shutdown(&server.client).await.unwrap();
    assert!(runner.active.is_none());
    let events = server.events(2).await;
    assert_eq!(events.len(), 2);
    assert_eq!(events[1]["type"], "stopped");
    assert_eq!(events[1]["reason"], "agent_shutdown");
    server.close().await;
}
#[tokio::test]
async fn rejected_or_ambiguous_start_reaps_child_and_holds_slot_without_terminal() {
    for status in [409, 0] {
        let dir = TempDir::new();
        let server = Server::new(&[status]).await;
        let mut runner = LifecycleRunner::new(Executor::new(&dir.0).unwrap());
        assert!(
            tokio::time::timeout(
                Duration::from_millis(350),
                runner.handle(&server.client, assignment(&dir, "wait", 0))
            )
            .await
            .is_err()
        );
        assert!(runner.active.as_ref().unwrap().process.is_none());
        runner.shutdown(&server.client).await.unwrap();
        assert_eq!(server.events(1).await.len(), 1);
        server.close().await;
    }
}
#[tokio::test]
async fn ambiguous_terminal_does_not_free_slot_or_send_competing_stop() {
    let dir = TempDir::new();
    let server = Server::new(&[200, 0]).await;
    let mut runner = LifecycleRunner::new(Executor::new(&dir.0).unwrap());
    assert!(
        tokio::time::timeout(
            Duration::from_millis(350),
            runner.handle(&server.client, assignment(&dir, "exit", 0))
        )
        .await
        .is_err()
    );
    assert!(runner.active.as_ref().unwrap().terminal_attempted);
    runner.shutdown(&server.client).await.unwrap();
    assert_eq!(server.events(2).await.len(), 2);
    server.close().await;
}
#[tokio::test]
async fn missing_or_inconsistent_execution_never_launches_or_reports() {
    for case in 0..3 {
        let dir = TempDir::new();
        let server = Server::new(&[]).await;
        let mut runner = LifecycleRunner::new(Executor::new(&dir.0).unwrap());
        let mut a = assignment(&dir, "wait", 0);
        match case {
            0 => a.execution = None,
            1 => a.server_time = None,
            _ => a
                .execution
                .as_mut()
                .unwrap()
                .args
                .push("private-mismatch".into()),
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(20), runner.handle(&server.client, a))
                .await
                .is_err()
        );
        runner.shutdown(&server.client).await.unwrap();
        assert!(!dir.0.join("ready").exists());
        assert!(server.state.events.lock().await.is_empty());
        server.close().await;
    }
}
#[tokio::test]
async fn event_clock_uses_server_anchor_and_saturates_without_wall_clock() {
    let clock = EventClock::new(Timestamp::from_unix_millis(u64::MAX));
    tokio::task::yield_now().await;
    assert_eq!(clock.now(), Timestamp::from_unix_millis(u64::MAX));
    let clock = EventClock::new(Timestamp::from_unix_millis(500));
    let first = clock.now();
    assert!(first >= Timestamp::from_unix_millis(500));
    assert!(clock.now() >= first);
}
#[cfg(unix)]
#[test]
fn signal_outcome_is_failure_without_fabricated_exit_code() {
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(
        process_result(std::process::ExitStatus::from_raw(9)),
        ProcessResultV1::ExecutionFailed
    );
}

#[tokio::test]
async fn cancellation_during_start_or_terminal_http_does_not_emit_second_terminal() {
    for during_start in [true, false] {
        let dir = TempDir::new();
        let replies = if during_start { vec![0] } else { vec![200, 0] };
        let server = Server::new(&replies).await;
        let mut runner = LifecycleRunner::new(Executor::new(&dir.0).unwrap());
        let expected = if during_start { 1 } else { 2 };
        {
            let future = runner.handle(
                &server.client,
                assignment(&dir, if during_start { "wait" } else { "exit" }, 0),
            );
            tokio::pin!(future);
            tokio::select! {r=&mut future=>panic!("completed before cancellation: {r:?}"),_=server.events(expected)=>{}}
        }
        runner.shutdown(&server.client).await.unwrap();
        assert!(runner.active.is_none());
        assert_eq!(server.events(expected).await.len(), expected);
        server.close().await;
    }
}
