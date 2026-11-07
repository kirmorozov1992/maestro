//! Owned pipe injection for deterministic executor/control race tests.
use super::{BoundedCapture, RunningProcess};
pub(crate) fn replace_stdout(process: &mut RunningProcess, stdout: tokio::process::ChildStdout) {
    process.stdout = BoundedCapture::new(Some(stdout));
}
