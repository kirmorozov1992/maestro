//! Unix signal delivery is isolated to owned subprocesses, never the test runner.
use crate::config::AgentConfig;
use std::{net::SocketAddr, path::Path, process::Stdio, time::Duration};
use tokio::{
    process::{Child, Command},
    signal::unix::SignalKind,
};

pub(crate) fn spawn_signal_agent(address: SocketAddr, root: &Path) -> Child {
    Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "worker::agent::test_support::signal_agent_fixture",
            "--ignored",
            "--nocapture",
        ])
        .env("TOKIO_WORKER_THREADS", "2")
        .env("MAESTRO_M69_SERVER", address.to_string())
        .env("MAESTRO_M69_ROOT", root)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}

pub(crate) fn send_signal(child: &Child, signal: SignalKind) {
    unsafe extern "C" {
        #[link_name = "kill"]
        fn posix_kill(pid: std::ffi::c_int, signal: std::ffi::c_int) -> std::ffi::c_int;
    }
    let pid = i32::try_from(child.id().expect("owned child must not have been reaped")).unwrap();
    assert!(pid > 0);
    // SAFETY: kill takes integer values, not pointers. PID is a positive live
    // owned child's PID, retained unreaped; no process group/main test target.
    let result = unsafe { posix_kill(pid, signal.as_raw_value()) };
    assert_eq!(
        result,
        0,
        "signal delivery failed: {}",
        std::io::Error::last_os_error()
    );
}

pub(crate) async fn reap_signal_agent(
    child: &mut Child,
) -> std::io::Result<std::process::ExitStatus> {
    tokio::time::timeout(Duration::from_secs(4), child.wait())
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "signal agent watchdog expired",
            )
        })?
}

pub(crate) async fn cleanup_signal_agent(child: &mut Child) {
    if child.id().is_some() {
        match tokio::time::timeout(Duration::from_secs(1), child.wait()).await {
            Ok(Ok(_)) => {}
            _ => child.kill().await.unwrap(),
        }
    }
}

/// Declared after the agent so panic unwinding releases fixtures before its
/// kill_on_drop fallback. Files stay present until explicit successful cleanup.
pub(crate) struct FixtureRelease {
    files: Vec<std::path::PathBuf>,
}
impl FixtureRelease {
    pub(crate) fn new(files: Vec<std::path::PathBuf>) -> Self {
        Self { files }
    }
    pub(crate) fn release(&self) {
        for path in &self.files {
            let _ = std::fs::write(path, "release");
        }
    }
}
impl Drop for FixtureRelease {
    fn drop(&mut self) {
        self.release();
    }
}

#[test]
#[ignore = "production agent signal fixture; invoked only in owned subprocess"]
fn signal_agent_fixture() {
    let config = AgentConfig {
        server_addr: std::env::var("MAESTRO_M69_SERVER")
            .unwrap()
            .parse()
            .unwrap(),
        agent_address: format!("signal-agent-{}", std::process::id()),
        working_dir: std::env::var_os("MAESTRO_M69_ROOT").unwrap().into(),
        heartbeat_interval: Duration::from_millis(10),
        poll_interval: Duration::from_millis(5),
    };
    super::run(config).unwrap();
}

pub(crate) fn process_is_absent(pid: u32) -> bool {
    unsafe extern "C" {
        #[link_name = "kill"]
        fn posix_kill(pid: std::ffi::c_int, signal: std::ffi::c_int) -> std::ffi::c_int;
    }
    let pid = i32::try_from(pid).unwrap();
    assert!(pid > 0);
    // SAFETY: signal zero only probes existence of our fixture descendant's
    // positive PID. It sends no signal and uses no pointers.
    unsafe { posix_kill(pid, 0) != 0 }
}
