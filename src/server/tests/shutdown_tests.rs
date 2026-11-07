use super::*;
use crate::worker::agent::test_support::{
    FixtureRelease, cleanup_signal_agent, process_is_absent, reap_signal_agent, send_signal,
    spawn_signal_agent,
};
use tokio::signal::unix::SignalKind;

async fn server_that_schedules_before_event_ack(
    store: StateStore,
) -> (
    SocketAddr,
    oneshot::Sender<()>,
    JoinHandle<Result<(), std::io::Error>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = super::super::server_state(store.clone(), Duration::from_secs(10));
    let router = super::super::router(state).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let store = store.clone();
            async move {
                let event = request.uri().path().ends_with("/events");
                let response = next.run(request).await;
                if event && response.status().is_success() {
                    // Terminal commit freed the reservation; make the next
                    // assignment available before the shutdown report ack.
                    Scheduler::new(store)
                        .tick(
                            super::super::current_timestamp().unwrap(),
                            Duration::from_secs(10),
                        )
                        .unwrap();
                }
                response
            }
        },
    ));
    let (stop, shutdown) = oneshot::channel();
    let task = tokio::spawn(async {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown.await;
            })
            .await
    });
    (address, stop, task)
}

#[tokio::test]
async fn unix_signals_reap_active_child_report_stopped_and_never_launch_queued_job() {
    for signal in [SignalKind::interrupt(), SignalKind::terminate()] {
        let store = StateStore::default();
        let (address, server_stop, server) =
            server_that_schedules_before_event_ack(store.clone()).await;
        let root = std::env::temp_dir().join(format!("maestro-m69-signal-{}", JobId::new()));
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(root.join("queued")).unwrap();
        let mut agent = spawn_signal_agent(address, &root);
        let release = FixtureRelease::new(vec![root.join("release"), root.join("queued/release")]);
        tokio::time::timeout(Duration::from_secs(4), async {
            while store.list_agents().unwrap().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let running = super::stop_tests::submit_wait(address, &root, "wait").await;
        let queued = super::stop_tests::submit_wait(address, &root.join("queued"), "wait").await;
        let scheduler = Scheduler::new(store.clone());
        scheduler
            .tick(
                super::super::current_timestamp().unwrap(),
                Duration::from_secs(10),
            )
            .unwrap();
        super::stop_tests::await_status(&store, running, JobStatus::Running).await;
        tokio::time::timeout(Duration::from_secs(4), async {
            while !root.join("ready").exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let child_pid: u32 = std::fs::read_to_string(root.join("ready"))
            .unwrap()
            .parse()
            .unwrap();
        send_signal(&agent, signal);
        let status = reap_signal_agent(&mut agent).await;
        // Observe BEFORE fallback release: fallback cannot hide a missed reap.
        let absent_before_fallback = process_is_absent(child_pid);
        release.release();
        cleanup_signal_agent(&mut agent).await;
        server_stop.send(()).unwrap();
        server.await.unwrap().unwrap();
        let status = status.unwrap();
        assert!(status.success(), "agent should handle OS signal: {status}");
        assert!(
            absent_before_fallback,
            "direct child must already be absent when agent exits"
        );
        assert!(agent.id().is_none());
        assert_eq!(store.get_job(running).unwrap().status(), JobStatus::Stopped);
        assert_eq!(store.get_job(queued).unwrap().status(), JobStatus::Assigned);
        assert!(!root.join("queued/ready").exists());
        assert_eq!(store.get_job(queued).unwrap().started_at(), None);
        assert_eq!(
            store
                .list_queued_assignments(store.list_agents().unwrap()[0].id())
                .unwrap()
                .len(),
            1
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
