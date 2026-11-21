use std::{
    fs::{self, File},
    io,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
    thread,
    time::Duration,
};

use serde_json::Value;
use tokio::{
    process::{Child, Command},
    signal::unix::SignalKind,
    time::{Instant, sleep_until, timeout_at},
};
use uuid::Uuid;

const POLL_INTERVAL: Duration = Duration::from_millis(20);
const SERVER_SHUTDOWN: Duration = Duration::from_secs(2);
const AGENT_SHUTDOWN: Duration = Duration::from_secs(8);
const FORCED_REAP: Duration = Duration::from_millis(250);

pub(super) struct Harness {
    // Drop the agent first so it can reap its job and report to the server.
    agent: Option<Process>,
    server: Process,
    root: TempDir,
    working_dir: PathBuf,
    deadline: Instant,
    address: SocketAddr,
}

impl Harness {
    pub(super) async fn start(timeout: Duration) -> io::Result<Self> {
        Self::start_with_args(timeout, &[]).await
    }

    pub(super) async fn start_with_args(timeout: Duration, args: &[&str]) -> io::Result<Self> {
        let deadline = Instant::now() + timeout;
        let root = TempDir::new()?;
        let working_dir = root.0.join("work");
        fs::create_dir(&working_dir)?;
        let mut server = Process::spawn(
            &root.0.join("server.log"),
            maestro()
                .args(["server", "--bind-addr", "127.0.0.1:0"])
                .args(args),
            Some(SERVER_SHUTDOWN),
        )?;
        let address = server
            .wait_for_field(deadline, "server_started", "bind_addr")
            .await?
            .parse::<SocketAddr>()
            .map_err(|error| server.failure(format_args!("invalid server address: {error}")))?;

        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_millis(250))
            .build()
            .map_err(io::Error::other)?;
        let url = format!("http://{address}/health");
        loop {
            server.check_deadline(deadline, "HTTP health")?;
            server.check_running()?;
            let response = timeout_at(deadline, async {
                client
                    .get(&url)
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Value>()
                    .await
            })
            .await;
            if matches!(response, Ok(Ok(ref body)) if body["status"] == "ok") {
                break;
            }
            pause_until(deadline).await;
        }

        Ok(Self {
            agent: None,
            server,
            root,
            working_dir,
            deadline,
            address,
        })
    }

    pub(super) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(super) fn working_dir(&self) -> &Path {
        &self.working_dir
    }

    pub(super) async fn start_agent(&mut self) -> io::Result<Uuid> {
        self.start_agent_with_args(&[]).await
    }

    pub(super) async fn start_agent_with_args(&mut self, args: &[&str]) -> io::Result<Uuid> {
        if self.agent.is_some() {
            return Err(io::Error::other("e2e agent already started"));
        }
        self.server.check_deadline(self.deadline, "agent start")?;
        self.server.check_running()?;
        let mut agent = Process::spawn(
            &self.root.0.join("agent.log"),
            maestro()
                .args(["agent", "--server-addr", &self.address.to_string()])
                .arg("--working-dir")
                .arg(self.working_dir())
                .args(args),
            Some(AGENT_SHUTDOWN),
        )?;
        let id = agent
            .wait_for_field(self.deadline, "agent_registered", "agent_id")
            .await?
            .parse::<Uuid>()
            .map_err(|error| agent.failure(format_args!("invalid agent ID: {error}")))?;
        self.agent = Some(agent);
        Ok(id)
    }

    pub(super) async fn status(&mut self, id: &str) -> io::Result<Value> {
        self.cli_json(&["status", "--json", id]).await
    }

    pub(super) async fn wait_for_status(&mut self, id: &str, expected: &str) -> io::Result<Value> {
        let mut last_status = "unknown".to_owned();
        loop {
            let job = self.status(id).await.map_err(|error| {
                io::Error::new(
                    error.kind(),
                    format!("waiting for job {id} to reach {expected}, last status {last_status}: {error}"),
                )
            })?;
            let status = job["status"]
                .as_str()
                .ok_or_else(|| io::Error::other(format!("job {id} omitted its status")))?;
            if status == expected {
                return Ok(job);
            }
            if matches!(status, "succeeded" | "failed" | "stopped" | "lost") {
                return Err(io::Error::other(format!(
                    "job {id} reached {status} while waiting for {expected}: {job}"
                )));
            }
            last_status = status.to_owned();
            sleep_until(
                self.deadline
                    .min(Instant::now() + Duration::from_millis(100)),
            )
            .await;
        }
    }

    pub(super) async fn cli_json(&mut self, args: &[&str]) -> io::Result<Value> {
        self.server.check_deadline(self.deadline, "CLI start")?;
        self.server.check_running()?;
        let mut cli = Process::spawn(
            &self.root.0.join("cli.log"),
            maestro()
                .env("MAESTRO_SERVER_ADDR", self.address.to_string())
                .args(args),
            None,
        )?;
        let status = timeout_at(self.deadline, cli.child.wait())
            .await
            .map_err(|_| cli.timed_out("CLI exit"))??;
        if !status.success() {
            return Err(cli.failure(format_args!("CLI exited with {status}")));
        }
        serde_json::from_str(&cli.log()?)
            .map_err(|error| cli.failure(format_args!("invalid JSON from CLI: {error}")))
    }
}

async fn pause_until(deadline: Instant) {
    sleep_until(deadline.min(Instant::now() + POLL_INTERVAL)).await;
}

fn maestro() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_maestro"));
    command
        .env_clear()
        .env("NO_COLOR", "1")
        .env("RUST_LOG", "info")
        .env("TOKIO_WORKER_THREADS", "2")
        .stdin(Stdio::null());
    command
}

struct TempDir(PathBuf);

impl TempDir {
    fn new() -> io::Result<Self> {
        let path = std::env::temp_dir().join(format!("maestro-e2e-{}", Uuid::new_v4()));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            eprintln!("could not remove e2e directory {:?}: {error}", self.0);
        }
    }
}

struct Process {
    child: Child,
    log_path: PathBuf,
    stderr_path: PathBuf,
    graceful_timeout: Option<Duration>,
}

impl Process {
    fn spawn(
        path: &Path,
        command: &mut Command,
        graceful_timeout: Option<Duration>,
    ) -> io::Result<Self> {
        // Files can be read while a process is running and cannot fill like a pipe.
        let stderr_path = path.with_extension("stderr");
        let child = command
            .stdout(File::create(path)?)
            .stderr(File::create(&stderr_path)?)
            .kill_on_drop(true)
            .spawn()?;
        Ok(Self {
            child,
            log_path: path.to_owned(),
            stderr_path,
            graceful_timeout,
        })
    }

    fn log(&self) -> io::Result<String> {
        fs::read_to_string(&self.log_path)
    }

    fn failure(&self, reason: impl std::fmt::Display) -> io::Error {
        io::Error::other(format!(
            "{reason} ({:?})\nstdout:\n{}\nstderr:\n{}",
            self.log_path,
            self.log()
                .unwrap_or_else(|error| format!("could not read stdout: {error}")),
            fs::read_to_string(&self.stderr_path)
                .unwrap_or_else(|error| format!("could not read stderr: {error}")),
        ))
    }

    fn check_running(&mut self) -> io::Result<()> {
        match self.child.try_wait()? {
            Some(status) => Err(self.failure(format_args!("process exited with {status}"))),
            None => Ok(()),
        }
    }

    async fn wait_for_field(
        &mut self,
        deadline: Instant,
        event: &str,
        field: &str,
    ) -> io::Result<String> {
        let event = format!("event=\"{event}\"");
        let field = format!("{field}=");
        loop {
            self.check_deadline(deadline, &event)?;
            self.check_running()?;
            if let Some(value) = self
                .log()?
                .split_inclusive('\n')
                .filter(|line| line.ends_with('\n'))
                .find(|line| line.split_whitespace().any(|part| part == event))
                .and_then(|line| {
                    line.split_whitespace()
                        .find_map(|part| part.strip_prefix(&field))
                })
            {
                return Ok(value.to_owned());
            }
            pause_until(deadline).await;
        }
    }

    fn check_deadline(&self, deadline: Instant, waiting_for: &str) -> io::Result<()> {
        if Instant::now() >= deadline {
            return Err(self.timed_out(waiting_for));
        }
        Ok(())
    }

    fn timed_out(&self, waiting_for: &str) -> io::Error {
        io::Error::new(
            io::ErrorKind::TimedOut,
            self.failure(format_args!(
                "e2e deadline exceeded waiting for {waiting_for}"
            )),
        )
    }

    fn wait_for_shutdown(
        &mut self,
        deadline: std::time::Instant,
    ) -> io::Result<Option<ExitStatus>> {
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return Ok(Some(status)),
                Ok(None) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            thread::sleep(remaining.min(POLL_INTERVAL));
        }
    }

    fn stop(&mut self) {
        let started = std::time::Instant::now();
        if matches!(self.wait_for_shutdown(started), Ok(Some(_))) {
            return;
        }
        let grace = self.graceful_timeout.unwrap_or_default();
        if !grace.is_zero()
            && self
                .child
                .id()
                .is_some_and(|pid| send_signal(pid, SignalKind::terminate().as_raw_value()))
            && matches!(self.wait_for_shutdown(started + grace), Ok(Some(_)))
        {
            return;
        }
        if let Err(error) = self.child.start_kill() {
            eprintln!(
                "{}",
                self.failure(format_args!("e2e forced stop failed: {error}"))
            );
        }
        if !matches!(
            self.wait_for_shutdown(started + grace + FORCED_REAP),
            Ok(Some(_))
        ) {
            eprintln!(
                "{}",
                self.failure("e2e child still unreaped at teardown deadline")
            );
        }
        // Tokio kills on drop and retains unreaped children for its Unix reaper.
    }
}

fn send_signal(pid: u32, signal: std::ffi::c_int) -> bool {
    unsafe extern "C" {
        #[link_name = "kill"]
        fn posix_kill(pid: std::ffi::c_int, signal: std::ffi::c_int) -> std::ffi::c_int;
    }
    let Ok(pid) = std::ffi::c_int::try_from(pid) else {
        return false;
    };
    // SAFETY: A positive PID targets one process. Nonzero signals are sent
    // only to an owned, unreaped child. Signal zero only probes its PID.
    pid > 0 && unsafe { posix_kill(pid, signal) == 0 }
}

impl Drop for Process {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn startup_wait_reports_an_exited_process() {
        let root = TempDir::new().unwrap();
        let mut child = Process::spawn(
            &root.0.join("invalid.log"),
            maestro().arg("invalid-subcommand"),
            None,
        )
        .unwrap();
        let error = child
            .wait_for_field(
                Instant::now() + Duration::from_secs(5),
                "server_started",
                "bind_addr",
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("process exited"));
        assert!(error.to_string().contains("unknown command"));
    }

    #[tokio::test]
    async fn unwinding_reaps_agent_and_server() {
        let mut harness = Harness::start(Duration::from_secs(20)).await.unwrap();
        harness.start_agent().await.unwrap();
        let server_pid = harness.server.child.id().unwrap();
        let agent_pid = harness.agent.as_ref().unwrap().child.id().unwrap();
        let root = harness.root.0.clone();

        // The closure consumes the harness, so it cannot be used after unwinding.
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
            let _harness = harness;
            panic!("exercise e2e cleanup");
        }));

        assert!(result.is_err());
        // Signal zero probes the PID without delivering a signal. A zombie
        // would still have a PID, so this also checks that the children were reaped.
        assert!(!send_signal(agent_pid, 0));
        assert!(!send_signal(server_pid, 0));
        assert!(!root.exists());
    }

    #[tokio::test]
    async fn expired_deadline_prevents_starting_agent_or_cli() {
        let mut harness = Harness::start(Duration::from_secs(20)).await.unwrap();
        harness.deadline = Instant::now();

        assert_eq!(
            harness.start_agent().await.unwrap_err().kind(),
            io::ErrorKind::TimedOut
        );
        assert_eq!(
            harness
                .cli_json(&["status", "--json"])
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::TimedOut
        );
        assert!(!harness.root.0.join("agent.log").exists());
        assert!(!harness.root.0.join("cli.log").exists());
    }
}
