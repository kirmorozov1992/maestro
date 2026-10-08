use crate::domain::TransitionError;
use std::{error::Error, fmt};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum StateStoreError {
    LockPoisoned,
    NotFound {
        resource: &'static str,
        id: String,
    },
    AlreadyExists {
        resource: &'static str,
        id: String,
    },
    Conflict {
        resource: &'static str,
        id: String,
        reason: String,
    },
    InvariantViolation {
        context: &'static str,
    },
    Transition(TransitionError),
}

impl StateStoreError {
    pub(super) fn not_found(resource: &'static str, id: impl ToString) -> Self {
        Self::NotFound {
            resource,
            id: id.to_string(),
        }
    }

    pub(super) fn already_exists(resource: &'static str, id: impl ToString) -> Self {
        Self::AlreadyExists {
            resource,
            id: id.to_string(),
        }
    }

    pub(super) fn conflict(
        resource: &'static str,
        id: impl ToString,
        reason: impl Into<String>,
    ) -> Self {
        Self::Conflict {
            resource,
            id: id.to_string(),
            reason: reason.into(),
        }
    }

    pub(super) const fn invariant(context: &'static str) -> Self {
        Self::InvariantViolation { context }
    }
}

impl fmt::Display for StateStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LockPoisoned => formatter.write_str("state store mutex is poisoned"),
            Self::NotFound { resource, id } => write!(formatter, "{resource} `{id}` not found"),
            Self::AlreadyExists { resource, id } => {
                write!(formatter, "{resource} `{id}` already exists")
            }
            Self::Conflict {
                resource,
                id,
                reason,
            } => write!(formatter, "{resource} `{id}` conflict: {reason}"),
            Self::InvariantViolation { context } => {
                write!(formatter, "state store invariant violated: {context}")
            }
            Self::Transition(error) => error.fmt(formatter),
        }
    }
}

impl Error for StateStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Transition(error) => Some(error),
            Self::LockPoisoned
            | Self::NotFound { .. }
            | Self::AlreadyExists { .. }
            | Self::Conflict { .. }
            | Self::InvariantViolation { .. } => None,
        }
    }
}
