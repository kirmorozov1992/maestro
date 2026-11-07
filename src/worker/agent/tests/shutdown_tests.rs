use super::*;
#[cfg(unix)]
use crate::worker::agent::test_support::{
    cleanup_signal_agent, reap_signal_agent, send_signal, spawn_signal_agent,
};

#[cfg(unix)]
#[tokio::test]
async fn real_signals_cancel_stalled_registration_with_clean_process_exit() {
    use tokio::signal::unix::SignalKind;
    for signal in [SignalKind::interrupt(), SignalKind::terminate()] {
        let mock = MockServer::start(StatusCode::CREATED, []).await;
        let gate = Arc::new(Notify::new());
        *mock.state.registration_gate.lock().await = Some(gate.clone());
        let mut agent = spawn_signal_agent(mock.address, &std::env::temp_dir());
        mock.wait_for_requests(1).await;
        send_signal(&agent, signal);
        let status = reap_signal_agent(&mut agent).await;
        gate.notify_one();
        cleanup_signal_agent(&mut agent).await;
        assert_eq!(mock.state.requests.lock().await.len(), 1);
        mock.stop().await;
        let status = status.unwrap();
        assert!(
            status.success(),
            "agent must handle signal during registration: {status}"
        );
        assert!(agent.id().is_none());
    }
}

#[tokio::test]
async fn already_ready_shutdown_does_not_register_or_start_loops() {
    let mock = MockServer::start(StatusCode::CREATED, []).await;
    run_with_shutdown(
        agent_config(mock.address, Duration::from_millis(5)),
        async {},
    )
    .await
    .unwrap();
    assert!(mock.state.requests.lock().await.is_empty());
    mock.stop().await;
}

#[tokio::test]
async fn listener_failure_cleans_handler_and_returns_safe_error_with_source() {
    use crate::worker::{
        executor::ExecutorError, lifecycle::AssignmentHandler, shutdown::ShutdownError,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Handler {
        started: Arc<Notify>,
        cleaned: Arc<AtomicUsize>,
    }
    impl AssignmentHandler for Handler {
        async fn handle(
            &mut self,
            _: &AgentHttpClient,
            _: AssignmentResponse,
        ) -> Result<(), ExecutorError> {
            self.started.notify_one();
            pending().await
        }
        async fn shutdown(&mut self, _: &AgentHttpClient) -> Result<(), ExecutorError> {
            self.cleaned.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    let mock = MockServer::start(StatusCode::CREATED, []).await;
    mock.enqueue_assignments([PollReply::Assignment(Box::new(assignment(ALLOCATION_ID)))])
        .await;
    let started = Arc::new(Notify::new());
    let cleaned = Arc::new(AtomicUsize::new(0));
    let (send, receive) = oneshot::channel();
    let agent = tokio::spawn(super::super::run_until_shutdown(
        polling_config(mock.address, Duration::from_millis(5)),
        async {
            let _ = receive.await;
            Err(ShutdownError::new(std::io::Error::other(
                "private-listener-source",
            )))
        },
        Handler {
            started: started.clone(),
            cleaned: cleaned.clone(),
        },
    ));
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    send.send(()).unwrap();
    let error = agent.await.unwrap().unwrap_err();
    assert_eq!(cleaned.load(Ordering::SeqCst), 1);
    assert!(matches!(
        error,
        super::super::AgentRuntimeError::Shutdown(_)
    ));
    assert!(!error.to_string().contains("private-listener-source"));
    assert!(!format!("{error:?}").contains("private-listener-source"));
    let source = std::error::Error::source(&error).unwrap();
    assert!(
        std::error::Error::source(source)
            .unwrap()
            .to_string()
            .contains("private-listener-source")
    );
    mock.stop().await;
}

#[cfg(unix)]
#[tokio::test]
async fn signal_watchdog_returns_control_so_caller_can_finish_cleanup() {
    let mock = MockServer::start(StatusCode::CREATED, []).await;
    let gate = Arc::new(Notify::new());
    *mock.state.registration_gate.lock().await = Some(gate.clone());
    let mut child = spawn_signal_agent(mock.address, &std::env::temp_dir());
    mock.wait_for_requests(1).await;
    // No signal: registration stays gated and forces the test watchdog path.
    let waiter = tokio::spawn(async move {
        let result = reap_signal_agent(&mut child).await;
        if child.id().is_some() {
            child.kill().await.unwrap();
        }
        result
    });
    let result = waiter.await;
    gate.notify_one();
    mock.stop().await;
    assert!(
        result.is_ok(),
        "watchdog must return without bypassing caller cleanup"
    );
}
