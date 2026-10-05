//! Application error boundary.

use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

pub type BoxError = Box<dyn Error + Send + Sync + 'static>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    InvalidInput,
    NotFound,
    Conflict,
    Internal,
    Transport,
    Process,
}

impl ErrorKind {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidInput => "invalid_input",
            Self::NotFound => "not_found",
            Self::Conflict => "conflict",
            Self::Internal => "internal_error",
            Self::Transport => "transport_error",
            Self::Process => "process_error",
        }
    }

    const fn public_message(self) -> &'static str {
        match self {
            Self::InvalidInput => "request contains invalid input",
            Self::NotFound => "requested resource was not found",
            Self::Conflict => "request conflicts with current state",
            Self::Internal => "internal server error",
            Self::Transport => "service communication failed",
            Self::Process => "process execution failed",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicError {
    pub code: &'static str,
    pub message: &'static str,
}

impl Display for PublicError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

#[derive(Debug)]
pub enum AppError {
    InvalidInput { message: String },
    NotFound { resource: &'static str, id: String },
    Conflict { message: String },
    Internal { context: String, source: BoxError },
    Transport { context: String, source: BoxError },
    Process { context: String, source: BoxError },
}

impl AppError {
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::InvalidInput {
            message: message.into(),
        }
    }

    pub fn not_found(resource: &'static str, id: impl Into<String>) -> Self {
        Self::NotFound {
            resource,
            id: id.into(),
        }
    }

    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict {
            message: message.into(),
        }
    }

    pub fn internal<E>(context: impl Into<String>, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Internal {
            context: context.into(),
            source: Box::new(source),
        }
    }

    pub fn transport<E>(context: impl Into<String>, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Transport {
            context: context.into(),
            source: Box::new(source),
        }
    }

    pub fn process<E>(context: impl Into<String>, source: E) -> Self
    where
        E: Error + Send + Sync + 'static,
    {
        Self::Process {
            context: context.into(),
            source: Box::new(source),
        }
    }

    pub const fn kind(&self) -> ErrorKind {
        match self {
            Self::InvalidInput { .. } => ErrorKind::InvalidInput,
            Self::NotFound { .. } => ErrorKind::NotFound,
            Self::Conflict { .. } => ErrorKind::Conflict,
            Self::Internal { .. } => ErrorKind::Internal,
            Self::Transport { .. } => ErrorKind::Transport,
            Self::Process { .. } => ErrorKind::Process,
        }
    }

    pub fn public_error(&self) -> PublicError {
        let kind = self.kind();

        PublicError {
            code: kind.code(),
            message: kind.public_message(),
        }
    }
}

impl Display for AppError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput { message } => write!(formatter, "invalid input: {message}"),
            Self::NotFound { resource, id } => write!(formatter, "{resource} `{id}` not found"),
            Self::Conflict { message } => write!(formatter, "conflict: {message}"),
            Self::Internal { context, source }
            | Self::Transport { context, source }
            | Self::Process { context, source } => write!(formatter, "{context}: {source}"),
        }
    }
}

impl Error for AppError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Internal { source, .. }
            | Self::Transport { source, .. }
            | Self::Process { source, .. } => Some(source.as_ref()),
            Self::InvalidInput { .. } | Self::NotFound { .. } | Self::Conflict { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{AppError, ErrorKind};
    use std::{error::Error, fmt};

    #[derive(Debug)]
    struct TestSource;

    impl fmt::Display for TestSource {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("source failure")
        }
    }

    impl Error for TestSource {}

    #[test]
    fn maps_all_kinds_to_stable_public_codes() {
        let errors = [
            (AppError::invalid_input("bad command"), "invalid_input"),
            (AppError::not_found("job", "job-1"), "not_found"),
            (AppError::conflict("job is already running"), "conflict"),
            (
                AppError::internal("store read failed", TestSource),
                "internal_error",
            ),
            (
                AppError::transport("heartbeat failed", TestSource),
                "transport_error",
            ),
            (
                AppError::process("spawn failed", TestSource),
                "process_error",
            ),
        ];

        for (error, expected_code) in errors {
            assert_eq!(error.public_error().code, expected_code);
            assert_eq!(error.public_error().code, error.kind().code());
        }
    }

    #[test]
    fn preserves_source_for_operational_errors() {
        let error = AppError::transport("request failed", TestSource);
        let source = error
            .source()
            .expect("transport source should be preserved");

        assert!(source.downcast_ref::<TestSource>().is_some());
    }

    #[test]
    fn public_error_does_not_expose_internal_details() {
        let error = AppError::internal("database password=secret", TestSource);
        let public = error.public_error();

        assert_eq!(public.to_string(), "internal_error: internal server error");
        assert!(!public.to_string().contains("secret"));
        assert!(!format!("{public:?}").contains("secret"));
    }

    #[test]
    fn display_and_debug_keep_internal_details_for_diagnostics() {
        let error = AppError::process("executor failed", TestSource);

        assert_eq!(error.to_string(), "executor failed: source failure");
        assert!(format!("{error:?}").contains("executor failed"));
        assert!(format!("{error:?}").contains("TestSource"));
    }

    #[test]
    fn exposes_expected_kind_values() {
        assert_eq!(
            AppError::invalid_input("bad").kind(),
            ErrorKind::InvalidInput
        );
        assert_eq!(
            AppError::not_found("job", "job-1").kind(),
            ErrorKind::NotFound
        );
        assert_eq!(AppError::conflict("busy").kind(), ErrorKind::Conflict);
    }
}
