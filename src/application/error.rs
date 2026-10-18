use crate::{domain::TransitionError, error::AppError, state::error::StateStoreError};

pub(crate) fn map_store_error(error: StateStoreError) -> AppError {
    match error {
        StateStoreError::NotFound { resource, id } => AppError::not_found(resource, id),
        StateStoreError::Transition(error @ TransitionError::InconsistentState { .. }) => {
            AppError::internal("state transition encountered inconsistent state", error)
        }
        conflict @ (StateStoreError::AlreadyExists { .. }
        | StateStoreError::Conflict { .. }
        | StateStoreError::Transition(_)) => AppError::conflict(conflict.to_string()),
        internal @ (StateStoreError::LockPoisoned | StateStoreError::InvariantViolation { .. }) => {
            AppError::internal("state store operation failed", internal)
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        domain::{AllocationStatus, JobStatus, TransitionError},
        error::{AppError, ErrorKind},
        state::error::StateStoreError,
    };
    use std::error::Error;

    #[test]
    fn maps_store_not_found_to_not_found() {
        let error = super::map_store_error(StateStoreError::NotFound {
            resource: "job",
            id: "job-1".into(),
        });

        assert!(matches!(
            error,
            AppError::NotFound {
                resource: "job",
                ..
            }
        ));
        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn maps_store_conflicts_and_duplicates_to_conflict() {
        let errors = [
            StateStoreError::Conflict {
                resource: "job",
                id: "job-1".into(),
                reason: "busy".into(),
            },
            StateStoreError::AlreadyExists {
                resource: "agent",
                id: "agent-1".into(),
            },
            StateStoreError::Transition(TransitionError::InvalidTransition {
                job_status: JobStatus::Pending,
                event: "started",
            }),
        ];

        for source in errors {
            assert_eq!(super::map_store_error(source).kind(), ErrorKind::Conflict);
        }
    }

    #[test]
    fn maps_inconsistent_transition_to_internal_with_transition_source() {
        let error = super::map_store_error(StateStoreError::Transition(
            TransitionError::InconsistentState {
                job_status: JobStatus::Pending,
                allocation_status: AllocationStatus::Running,
            },
        ));

        assert_eq!(error.kind(), ErrorKind::Internal);
        assert!(
            Error::source(&error)
                .and_then(|source| source.downcast_ref::<TransitionError>())
                .is_some()
        );
        assert_eq!(error.public_error().code, "internal_error");
    }

    #[test]
    fn maps_poisoned_lock_and_invariant_to_internal_with_source() {
        let errors = [
            StateStoreError::LockPoisoned,
            StateStoreError::InvariantViolation {
                context: "test invariant",
            },
        ];

        for source in errors {
            let error = super::map_store_error(source);
            assert_eq!(error.kind(), ErrorKind::Internal);
            assert!(
                Error::source(&error)
                    .and_then(|source| source.downcast_ref::<StateStoreError>())
                    .is_some()
            );
            assert_eq!(error.public_error().code, "internal_error");
        }
    }
}
