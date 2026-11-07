//! Local process ownership. Runtime assignment/reporting wiring follows in M6.7.

use crate::{
    domain::JobSpec,
    worker::working_directory::{WorkingDirectoryError, resolve_job_working_directory},
};
use std::{
    error::Error,
    fmt, io,
    path::{Path, PathBuf},
    process::{ExitStatus, Stdio},
};
use tokio::{
    process::{Child, Command},
    runtime::{Handle, TryCurrentError},
};

pub(crate) enum ExecutorError {
    WorkingDirectory(WorkingDirectoryError),
    InvalidCommand,
    InvalidArgument,
    InvalidEnvironment,
    RuntimeUnavailable(TryCurrentError),
    Spawn(io::Error),
    Wait(io::Error),
    Terminate(io::Error),
}

impl ExecutorError {
    pub(crate) fn code(&self) -> &'static str {
        match self {
            Self::WorkingDirectory(error) => error.code(),
            Self::InvalidCommand => "invalid_process_command",
            Self::InvalidArgument => "invalid_process_argument",
            Self::InvalidEnvironment => "invalid_process_environment",
            Self::RuntimeUnavailable(_) => "process_runtime_unavailable",
            Self::Spawn(error) => match error.kind() {
                io::ErrorKind::NotFound => "process_spawn_not_found",
                io::ErrorKind::PermissionDenied => "process_spawn_permission_denied",
                io::ErrorKind::InvalidInput => "invalid_process_input",
                _ => "process_spawn_failed",
            },
            Self::Wait(_) => "process_wait_failed",
            Self::Terminate(_) => "process_terminate_failed",
        }
    }
}

impl fmt::Display for ExecutorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::WorkingDirectory(error) => return fmt::Display::fmt(error, formatter),
            Self::InvalidCommand => "process command contains invalid data",
            Self::InvalidArgument => "process argument contains invalid data",
            Self::InvalidEnvironment => "process environment contains invalid data",
            Self::RuntimeUnavailable(_) => "process execution requires an active Tokio runtime",
            Self::Spawn(_) => "process could not be started",
            Self::Wait(_) => "process completion could not be observed",
            Self::Terminate(_) => "process could not be terminated",
        };
        formatter.write_str(message)
    }
}

impl fmt::Debug for ExecutorError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // OS error messages can include user-controlled paths and arguments.
        formatter
            .debug_struct("ExecutorError")
            .field("code", &self.code())
            .finish_non_exhaustive()
    }
}

impl Error for ExecutorError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::WorkingDirectory(error) => Some(error),
            Self::RuntimeUnavailable(error) => Some(error),
            Self::Spawn(error) | Self::Wait(error) | Self::Terminate(error) => Some(error),
            Self::InvalidCommand | Self::InvalidArgument | Self::InvalidEnvironment => None,
        }
    }
}

pub(crate) struct Executor {
    agent_root: PathBuf,
}

impl fmt::Debug for Executor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_struct("Executor").finish_non_exhaustive()
    }
}

impl Executor {
    pub(crate) fn new(agent_root: impl AsRef<Path>) -> Result<Self, ExecutorError> {
        let agent_root = resolve_job_working_directory(agent_root.as_ref(), None)
            .map_err(ExecutorError::WorkingDirectory)?;
        Ok(Self { agent_root })
    }

    /// Must be called inside a Tokio runtime with its I/O driver enabled.
    pub(crate) fn spawn(&self, spec: &JobSpec) -> Result<RunningProcess, ExecutorError> {
        validate_process_input(spec)?;
        let working_directory =
            resolve_job_working_directory(&self.agent_root, spec.working_dir().map(Path::new))
                .map_err(ExecutorError::WorkingDirectory)?;
        Handle::try_current().map_err(ExecutorError::RuntimeUnavailable)?;

        let program = Path::new(spec.command());
        // Relative executable paths with a directory are anchored explicitly to
        // the job cwd. Bare names retain the platform's normal PATH lookup.
        let program = if !program.is_absolute()
            && program
                .parent()
                .is_some_and(|parent| !parent.as_os_str().is_empty())
        {
            working_directory.join(program)
        } else {
            program.to_path_buf()
        };
        let mut command = Command::new(program);
        command
            .args(spec.args())
            .current_dir(working_directory)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        // Preserve normal inheritance; override only explicitly provided keys.
        if let Some(environment) = spec.env() {
            command.envs(environment);
        }
        command
            .spawn()
            .map(|child| RunningProcess { child })
            .map_err(ExecutorError::Spawn)
    }
}

fn validate_process_input(spec: &JobSpec) -> Result<(), ExecutorError> {
    if spec.command().contains('\0') {
        return Err(ExecutorError::InvalidCommand);
    }
    if spec.args().iter().any(|argument| argument.contains('\0')) {
        return Err(ExecutorError::InvalidArgument);
    }
    if spec.env().is_some_and(|environment| {
        environment
            .iter()
            .any(|(key, value)| key.is_empty() || key.contains(['=', '\0']) || value.contains('\0'))
    }) {
        return Err(ExecutorError::InvalidEnvironment);
    }
    Ok(())
}

/// The caller must await completion or explicit cleanup before dropping this
/// owner. kill_on_drop is a fallback, not a guarantee of immediate reaping or
/// termination of descendants. Cancelling wait retains ownership in this value.
pub(crate) struct RunningProcess {
    child: Child,
}

impl fmt::Debug for RunningProcess {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RunningProcess")
            .field("pid", &self.id())
            .finish_non_exhaustive()
    }
}

impl RunningProcess {
    pub(crate) fn id(&self) -> Option<u32> {
        self.child.id()
    }

    pub(crate) async fn wait(&mut self) -> Result<ExitStatus, ExecutorError> {
        self.child.wait().await.map_err(ExecutorError::Wait)
    }

    /// Force termination and reap the direct child, without implementing job
    /// stop delivery or graceful process-tree shutdown.
    pub(crate) async fn terminate_and_wait(&mut self) -> Result<ExitStatus, ExecutorError> {
        self.child.kill().await.map_err(ExecutorError::Terminate)?;
        self.wait().await
    }
}

#[cfg(test)]
mod tests {
    use super::Executor;
    use crate::domain::JobSpec;
    use serde_json::{Value, json};
    use std::{
        collections::BTreeMap,
        error::Error,
        fs,
        io::{self, Read, Write},
        path::{Path, PathBuf},
        process::{Command, Stdio},
        time::Duration,
    };

    const FIXTURE: &str = "worker::executor::tests::process_fixture";
    const MODE: &str = "MAESTRO_EXECUTOR_TEST_MODE";
    const OUTPUT: &str = "MAESTRO_EXECUTOR_TEST_OUTPUT";
    const INHERITED: &str = "MAESTRO_EXECUTOR_TEST_INHERITED";
    const OVERRIDE: &str = "MAESTRO_EXECUTOR_TEST_OVERRIDE";
    const WATCHDOG: Duration = Duration::from_secs(5);

    struct TestRoot(PathBuf);

    impl TestRoot {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("maestro executor {}", uuid::Uuid::new_v4()));
            fs::create_dir(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestRoot {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.0)
                .expect("test root should be removable after children finish");
        }
    }

    fn executable() -> String {
        std::env::current_exe()
            .unwrap()
            .into_os_string()
            .into_string()
            .unwrap()
    }

    fn fixture_args(extra: &[&str]) -> Vec<String> {
        let mut args = vec![
            FIXTURE.to_owned(),
            "--exact".into(),
            "--ignored".into(),
            "--nocapture".into(),
        ];
        if !extra.is_empty() {
            args.push("--".into());
            args.extend(extra.iter().map(|value| (*value).to_owned()));
        }
        args
    }

    fn fixture_env(mode: &str, output: &Path) -> BTreeMap<String, String> {
        BTreeMap::from([
            (MODE.to_owned(), mode.to_owned()),
            (OUTPUT.to_owned(), output.to_str().unwrap().to_owned()),
        ])
    }

    fn spec(mode: &str, output: &Path, args: &[&str], working_dir: Option<String>) -> JobSpec {
        JobSpec::new(
            executable(),
            fixture_args(args),
            Some(fixture_env(mode, output)),
            working_dir,
        )
        .unwrap()
    }

    async fn wait_for_file(path: &Path) {
        tokio::time::timeout(WATCHDOG, async {
            while !path.exists() {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("fixture should announce readiness");
    }

    // This function is invoked only in a child test executable. It never changes
    // the main test process's environment/cwd or requires a shell/system utility.
    #[test]
    #[ignore = "subprocess fixture invoked by executor tests"]
    fn process_fixture() {
        let mode = std::env::var(MODE).expect("fixture mode is required");
        let mode = if mode == "environment_parent" && std::env::args().any(|a| a == "inner-probe") {
            "inspect".to_owned()
        } else {
            mode
        };
        let output =
            PathBuf::from(std::env::var_os(OUTPUT).expect("fixture output path is required"));
        match mode.as_str() {
            "exit" => std::process::exit(
                std::env::var("MAESTRO_EXECUTOR_EXIT")
                    .unwrap()
                    .parse()
                    .unwrap(),
            ),
            "inspect" => {
                let mut input = Vec::new();
                io::stdin().read_to_end(&mut input).unwrap();
                // Null streams must not accumulate output or require a reader.
                let output_bytes = vec![b'x'; 128 * 1024];
                io::stdout().write_all(&output_bytes).unwrap();
                io::stderr().write_all(&output_bytes).unwrap();
                fs::write(
                    &output,
                    serde_json::to_vec(&json!({
                        "cwd": std::env::current_dir().unwrap(),
                        "args": std::env::args().collect::<Vec<_>>(),
                        "inherited": std::env::var(INHERITED).ok(),
                        "overridden": std::env::var(OVERRIDE).ok(),
                        "stdin_bytes": input.len(),
                    }))
                    .unwrap(),
                )
                .unwrap();
            }
            "wait" => {
                fs::write(&output, b"ready").unwrap();
                let release = output.with_extension("release");
                while !release.exists() {
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
            "environment_parent" => {
                let case = std::env::var("MAESTRO_EXECUTOR_ENV_CASE").unwrap();
                let env = match case.as_str() {
                    "none" => None,
                    "empty" => Some(BTreeMap::new()),
                    "override" => Some(BTreeMap::from([(
                        OVERRIDE.to_owned(),
                        "job-value".to_owned(),
                    )])),
                    _ => panic!("unknown env fixture case"),
                };
                // The inner-probe argument selects inspect without changing any
                // inherited environment value or touching the test runner.
                let child_spec =
                    JobSpec::new(executable(), fixture_args(&["inner-probe"]), env, None).unwrap();
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    let executor = Executor::new(output.parent().unwrap()).unwrap();
                    let mut child = executor.spawn(&child_spec).unwrap();
                    let status = tokio::time::timeout(WATCHDOG, child.wait())
                        .await
                        .unwrap()
                        .unwrap();
                    assert!(status.success());
                });
            }
            _ => panic!("unknown process fixture mode"),
        }
        std::process::exit(0);
    }

    #[tokio::test]
    async fn returns_zero_and_nonzero_exit_codes_without_treating_failure_as_success() {
        let root = TestRoot::new();
        let executor = Executor::new(&root.0).unwrap();
        for code in [0, 23] {
            let mut env = fixture_env("exit", &root.0.join("unused"));
            env.insert("MAESTRO_EXECUTOR_EXIT".into(), code.to_string());
            let spec = JobSpec::new(executable(), fixture_args(&[]), Some(env), None).unwrap();
            let mut child = executor.spawn(&spec).unwrap();
            assert!(child.id().is_some());
            let status = tokio::time::timeout(WATCHDOG, child.wait())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(status.code(), Some(code));
            assert_eq!(status.success(), code == 0);
            assert_eq!(child.id(), None);
            assert_eq!(child.wait().await.unwrap(), status);
            assert_eq!(child.terminate_and_wait().await.unwrap(), status);
        }
    }

    #[tokio::test]
    async fn passes_literal_arguments_and_launches_executable_with_spaces() {
        let root = TestRoot::new();
        let program = root.0.join("literal program ; name");
        fs::copy(executable(), &program).unwrap();
        let output = root.0.join("observed.json");
        let literal = [
            "",
            "a b",
            "$(not-a-command)",
            "; not-a-command",
            "'quoted'",
            "line\nbreak",
        ];
        let spec = JobSpec::new(
            program.to_str().unwrap(),
            fixture_args(&literal),
            Some(fixture_env("inspect", &output)),
            None,
        )
        .unwrap();
        let mut child = Executor::new(&root.0).unwrap().spawn(&spec).unwrap();
        assert!(
            tokio::time::timeout(WATCHDOG, child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        let observed: Value = serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
        let args = observed["args"].as_array().unwrap();
        assert_eq!(
            &args[args.len() - literal.len()..],
            literal.map(Value::from)
        );
        assert_eq!(observed["stdin_bytes"], 0);
    }

    #[tokio::test]
    async fn resolves_relative_executable_from_validated_job_working_directory() {
        let root = TestRoot::new();
        let job_dir = root.0.join("job directory");
        fs::create_dir(&job_dir).unwrap();
        let program = job_dir.join("fixture program");
        fs::copy(executable(), &program).unwrap();
        let output = root.0.join("cwd.json");
        let spec = JobSpec::new(
            "./fixture program",
            fixture_args(&[]),
            Some(fixture_env("inspect", &output)),
            Some("job directory".into()),
        )
        .unwrap();
        let mut child = Executor::new(&root.0).unwrap().spawn(&spec).unwrap();
        assert!(
            tokio::time::timeout(WATCHDOG, child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        let observed: Value = serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
        assert_eq!(observed["cwd"], json!(job_dir.canonicalize().unwrap()));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn searches_unqualified_program_name_using_child_path_override() {
        let root = TestRoot::new();
        let program = root.0.join("fixture-command");
        fs::copy(executable(), &program).unwrap();
        let output = root.0.join("path.json");
        let mut env = fixture_env("inspect", &output);
        env.insert("PATH".into(), root.0.to_str().unwrap().into());
        let spec = JobSpec::new("fixture-command", fixture_args(&[]), Some(env), None).unwrap();
        let mut child = Executor::new(&root.0).unwrap().spawn(&spec).unwrap();
        assert!(
            tokio::time::timeout(WATCHDOG, child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        assert!(output.exists());
    }

    #[test]
    fn inherits_environment_for_none_and_empty_and_overrides_only_supplied_keys() {
        let root = TestRoot::new();
        for case in ["none", "empty", "override"] {
            let output = root.0.join(format!("{case}.json"));
            // Start a controlled parent with known inherited values. The fixture
            // itself then uses Executor to start its inspect child.
            let status = Command::new(executable())
                .args(fixture_args(&[]))
                .env(MODE, "environment_parent")
                .env(OUTPUT, &output)
                .env(INHERITED, "parent-only")
                .env(OVERRIDE, "parent-value")
                .env("MAESTRO_EXECUTOR_ENV_CASE", case)
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap();
            assert!(
                status.success(),
                "environment parent should succeed for {case}"
            );
            let observed: Value = serde_json::from_slice(&fs::read(output).unwrap()).unwrap();
            assert_eq!(observed["inherited"], "parent-only");
            assert_eq!(
                observed["overridden"],
                if case == "override" {
                    "job-value"
                } else {
                    "parent-value"
                }
            );
        }
    }

    #[test]
    fn spawning_requires_a_tokio_runtime_without_panicking() {
        let root = TestRoot::new();
        let spec = spec("inspect", &root.0.join("unused"), &[], None);
        let error = Executor::new(&root.0).unwrap().spawn(&spec).unwrap_err();
        assert_eq!(error.code(), "process_runtime_unavailable");
        assert!(error.source().is_some());
    }

    #[test]
    fn rejects_invalid_environment_without_exposing_keys_or_values() {
        let root = TestRoot::new();
        for (key, value) in [
            ("", "private-value"),
            ("private=key", "value"),
            ("private\0key", "value"),
            ("KEY", "private\0value"),
        ] {
            let env = BTreeMap::from([(key.to_owned(), value.to_owned())]);
            let spec = JobSpec::new(executable(), vec![], Some(env), None).unwrap();
            let error = Executor::new(&root.0).unwrap().spawn(&spec).unwrap_err();
            assert_eq!(error.code(), "invalid_process_environment");
            assert!(!format!("{error} {error:?}").contains("private"));
        }
    }

    #[test]
    fn rejects_nul_command_and_argument_before_spawning() {
        let root = TestRoot::new();
        let executor = Executor::new(&root.0).unwrap();
        for (command, args, code) in [
            (
                "private\0command".to_owned(),
                vec![],
                "invalid_process_command",
            ),
            (
                executable(),
                vec!["private\0argument".to_owned()],
                "invalid_process_argument",
            ),
        ] {
            let spec = JobSpec::new(command, args, None, None).unwrap();
            let error = executor.spawn(&spec).unwrap_err();
            assert_eq!(error.code(), code);
            assert!(!format!("{error} {error:?}").contains("private"));
        }
    }

    #[test]
    fn validates_missing_and_non_directory_paths_before_spawning() {
        let root = TestRoot::new();
        let file = root.0.join("private-file");
        fs::write(&file, b"data").unwrap();
        for (path, code) in [
            ("private-missing", "working_directory_not_found"),
            ("private-file", "working_directory_not_directory"),
        ] {
            let spec = spec("exit", &root.0.join("unused"), &[], Some(path.into()));
            let error = Executor::new(&root.0).unwrap().spawn(&spec).unwrap_err();
            assert_eq!(error.code(), code);
            assert!(!format!("{error} {error:?}").contains("private"));
        }
        let error = Executor::new(root.0.join("private-root")).unwrap_err();
        assert_eq!(error.code(), "working_directory_not_found");
    }

    #[tokio::test]
    async fn classifies_spawn_not_found_and_preserves_the_io_source_safely() {
        let root = TestRoot::new();
        let spec = JobSpec::new(
            root.0.join("private-missing-program").to_str().unwrap(),
            vec!["private-arg".into()],
            None,
            None,
        )
        .unwrap();
        let error = Executor::new(&root.0).unwrap().spawn(&spec).unwrap_err();
        assert_eq!(error.code(), "process_spawn_not_found");
        assert_eq!(
            error
                .source()
                .unwrap()
                .downcast_ref::<io::Error>()
                .unwrap()
                .kind(),
            io::ErrorKind::NotFound
        );
        assert!(!format!("{error} {error:?}").contains("private"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn classifies_non_executable_file_as_permission_denied() {
        use std::os::unix::fs::PermissionsExt;
        let root = TestRoot::new();
        let program = root.0.join("private-non-executable");
        fs::write(&program, b"data").unwrap();
        fs::set_permissions(&program, fs::Permissions::from_mode(0o600)).unwrap();
        let spec = JobSpec::new(program.to_str().unwrap(), vec![], None, None).unwrap();
        let error = Executor::new(&root.0).unwrap().spawn(&spec).unwrap_err();
        assert_eq!(error.code(), "process_spawn_permission_denied");
        assert!(!format!("{error} {error:?}").contains("private"));
    }

    #[tokio::test]
    async fn cancelled_wait_keeps_child_owned_for_explicit_termination_and_reaping() {
        let root = TestRoot::new();
        let ready = root.0.join("ready");
        let mut child = Executor::new(&root.0)
            .unwrap()
            .spawn(&spec("wait", &ready, &[], None))
            .unwrap();
        wait_for_file(&ready).await;
        let pid = child.id();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), child.wait())
                .await
                .is_err()
        );
        assert_eq!(child.id(), pid);
        let status = tokio::time::timeout(WATCHDOG, child.terminate_and_wait())
            .await
            .unwrap()
            .unwrap();
        assert!(!status.success());
        assert_eq!(child.id(), None);
        assert_eq!(child.wait().await.unwrap(), status);
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(status.code(), None);
            assert_eq!(status.signal(), Some(9));
        }
    }

    #[tokio::test]
    async fn can_resume_a_cancelled_wait_and_observe_normal_completion() {
        let root = TestRoot::new();
        let ready = root.0.join("ready");
        let mut child = Executor::new(&root.0)
            .unwrap()
            .spawn(&spec("wait", &ready, &[], None))
            .unwrap();
        wait_for_file(&ready).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(10), child.wait())
                .await
                .is_err()
        );
        fs::write(ready.with_extension("release"), b"finish").unwrap();
        assert_eq!(
            tokio::time::timeout(WATCHDOG, child.wait())
                .await
                .unwrap()
                .unwrap()
                .code(),
            Some(0)
        );
    }
}
