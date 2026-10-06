use std::{
    error::Error,
    fmt, fs, io,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub(crate) enum WorkingDirectoryError {
    EmptyPath,
    NotFound,
    NotDirectory,
    InvalidPath,
    Io { source: io::Error },
}

impl WorkingDirectoryError {
    pub(crate) const fn code(&self) -> &'static str {
        match self {
            Self::EmptyPath => "empty_working_directory",
            Self::NotFound => "working_directory_not_found",
            Self::NotDirectory => "working_directory_not_directory",
            Self::InvalidPath => "invalid_working_directory",
            Self::Io { .. } => "working_directory_io_error",
        }
    }
}

impl fmt::Display for WorkingDirectoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::EmptyPath => "job working directory must not be empty",
            Self::NotFound => "job working directory does not exist",
            Self::NotDirectory => "job working directory must be a directory",
            Self::InvalidPath => "job working directory is invalid",
            Self::Io { .. } => "job working directory could not be accessed",
        };
        formatter.write_str(message)
    }
}

impl Error for WorkingDirectoryError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source } => Some(source),
            Self::EmptyPath | Self::NotFound | Self::NotDirectory | Self::InvalidPath => None,
        }
    }
}

pub(crate) fn resolve_job_working_directory(
    agent_root: &Path,
    requested: Option<&Path>,
) -> Result<PathBuf, WorkingDirectoryError> {
    let candidate = match requested {
        None => agent_root.to_path_buf(),
        Some(path) if path.as_os_str().is_empty() => {
            return Err(WorkingDirectoryError::EmptyPath);
        }
        Some(path) if path.is_absolute() => path.to_path_buf(),
        Some(path) => agent_root.join(path),
    };

    let metadata = fs::metadata(&candidate).map_err(map_path_error)?;
    if !metadata.is_dir() {
        return Err(WorkingDirectoryError::NotDirectory);
    }

    fs::canonicalize(candidate).map_err(map_path_error)
}

fn map_path_error(source: io::Error) -> WorkingDirectoryError {
    match source.kind() {
        io::ErrorKind::NotFound => WorkingDirectoryError::NotFound,
        io::ErrorKind::NotADirectory => WorkingDirectoryError::NotDirectory,
        io::ErrorKind::InvalidInput => WorkingDirectoryError::InvalidPath,
        _ => WorkingDirectoryError::Io { source },
    }
}

#[cfg(test)]
mod tests {
    use super::{WorkingDirectoryError, resolve_job_working_directory};
    use std::{
        error::Error,
        fs, io,
        path::{Path, PathBuf},
    };

    fn temporary_root() -> PathBuf {
        std::env::temp_dir().join(format!("maestro-m25-{}", uuid::Uuid::new_v4()))
    }

    #[test]
    fn defaults_to_agent_root_and_resolves_relative_paths_from_it() {
        let parent = temporary_root();
        let agent_root = parent.join("agent");
        fs::create_dir_all(agent_root.join("jobs/one")).unwrap();

        assert_eq!(
            resolve_job_working_directory(&agent_root, None).unwrap(),
            agent_root.canonicalize().unwrap()
        );
        assert_eq!(
            resolve_job_working_directory(&agent_root, Some(Path::new("jobs/one"))).unwrap(),
            agent_root.join("jobs/one").canonicalize().unwrap()
        );

        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn accepts_absolute_paths_outside_agent_root() {
        let parent = temporary_root();
        let agent_root = parent.join("agent");
        let shared_directory = parent.join("shared");
        fs::create_dir_all(&agent_root).unwrap();
        fs::create_dir_all(&shared_directory).unwrap();

        assert_eq!(
            resolve_job_working_directory(&agent_root, Some(&shared_directory)).unwrap(),
            shared_directory.canonicalize().unwrap()
        );

        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn rejects_missing_and_non_directory_paths_without_echoing_paths() {
        let parent = temporary_root();
        let agent_root = parent.join("agent");
        fs::create_dir_all(&agent_root).unwrap();
        let secret_path = Path::new("private-do-not-echo");

        let missing = resolve_job_working_directory(&agent_root, Some(secret_path)).unwrap_err();
        assert_eq!(missing.code(), "working_directory_not_found");
        assert!(!missing.to_string().contains("private-do-not-echo"));

        let file = agent_root.join("file");
        fs::write(&file, "data").unwrap();
        let not_directory = resolve_job_working_directory(&agent_root, Some(&file)).unwrap_err();
        assert_eq!(not_directory.code(), "working_directory_not_directory");

        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn rejects_empty_and_invalid_paths() {
        let parent = temporary_root();
        fs::create_dir_all(&parent).unwrap();

        let empty = resolve_job_working_directory(&parent, Some(Path::new(""))).unwrap_err();
        assert_eq!(empty.code(), "empty_working_directory");

        #[cfg(unix)]
        {
            let invalid =
                resolve_job_working_directory(&parent, Some(Path::new("bad\0path"))).unwrap_err();
            assert_eq!(invalid.code(), "invalid_working_directory");
        }

        fs::remove_dir_all(parent).unwrap();
    }

    #[test]
    fn working_directory_errors_have_stable_codes_and_safe_display() {
        let errors = [
            (WorkingDirectoryError::EmptyPath, "empty_working_directory"),
            (
                WorkingDirectoryError::NotFound,
                "working_directory_not_found",
            ),
            (
                WorkingDirectoryError::NotDirectory,
                "working_directory_not_directory",
            ),
            (
                WorkingDirectoryError::InvalidPath,
                "invalid_working_directory",
            ),
            (
                WorkingDirectoryError::Io {
                    source: io::Error::other("private path detail"),
                },
                "working_directory_io_error",
            ),
        ];

        for (error, expected_code) in errors {
            assert_eq!(error.code(), expected_code);
            assert!(!error.to_string().contains("private path detail"));
            if expected_code == "working_directory_io_error" {
                assert!(error.source().is_some());
            }
        }
    }
}
