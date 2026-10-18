use super::map_store_error;
use crate::{
    domain::{Agent, AgentId, AgentSnapshot, Timestamp},
    error::AppError,
    state::store::StateStore,
};

#[derive(Clone)]
pub(crate) struct AgentsService {
    store: StateStore,
}

#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "application services are connected to transport in M5"
    )
)]
impl AgentsService {
    pub(crate) fn new(store: StateStore) -> Self {
        Self { store }
    }

    pub(crate) fn register_agent(
        &self,
        id: AgentId,
        address: String,
        registered_at: Timestamp,
    ) -> Result<AgentSnapshot, AppError> {
        if address.trim().is_empty() {
            return Err(AppError::invalid_input("agent address cannot be empty"));
        }

        let agent = Agent::new(id, address, registered_at);
        let snapshot = AgentSnapshot::from(&agent);
        self.store.register_agent(agent).map_err(map_store_error)?;
        Ok(snapshot)
    }

    pub(crate) fn heartbeat(
        &self,
        id: AgentId,
        timestamp: Timestamp,
    ) -> Result<AgentSnapshot, AppError> {
        self.store
            .update_heartbeat(id, timestamp)
            .map_err(map_store_error)?;
        let agent = self.store.get_agent(id).map_err(map_store_error)?;
        Ok(AgentSnapshot::from(&agent))
    }
}

#[cfg(test)]
mod tests {
    use super::AgentsService;
    use crate::{
        domain::{Agent, AgentAvailability, AgentHealth, AgentId, Timestamp},
        error::ErrorKind,
        state::{error::StateStoreError, store::StateStore},
    };

    fn agent_id(value: u32) -> AgentId {
        format!("00000000-0000-4000-8001-{value:012x}")
            .parse()
            .expect("fixed UUID is valid")
    }

    fn service() -> AgentsService {
        AgentsService::new(StateStore::default())
    }

    #[test]
    fn register_agent_returns_healthy_idle_snapshot() {
        let service = service();
        let id = agent_id(1);
        let registered_at = Timestamp::from_unix_millis(100);

        let snapshot = service
            .register_agent(id, "worker.example:9000".into(), registered_at)
            .unwrap();

        assert_eq!(snapshot.id, id);
        assert_eq!(snapshot.address, "worker.example:9000");
        assert_eq!(snapshot.last_heartbeat_at, registered_at);
        assert_eq!(snapshot.health, AgentHealth::Healthy);
        assert_eq!(snapshot.availability, AgentAvailability::Idle);
    }

    #[test]
    fn register_agent_duplicate_id_returns_conflict_without_replacement() {
        let service = service();
        let id = agent_id(1);
        service
            .register_agent(
                id,
                "first.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();
        let original = Agent::new(
            id,
            "first.example:9000".into(),
            Timestamp::from_unix_millis(100),
        );

        let error = service
            .register_agent(
                id,
                "replacement.example:9000".into(),
                Timestamp::from_unix_millis(200),
            )
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(service.store.get_agent(id).unwrap(), original);
    }

    #[test]
    fn register_agent_rejects_blank_address_without_mutating_store() {
        for (suffix, address) in [(2, ""), (3, " \t\n")] {
            let service = service();
            let id = agent_id(suffix);

            let error = service
                .register_agent(id, address.into(), Timestamp::from_unix_millis(100))
                .unwrap_err();

            assert_eq!(error.kind(), ErrorKind::InvalidInput);
            assert!(matches!(
                service.store.get_agent(id),
                Err(StateStoreError::NotFound { .. })
            ));
        }
    }

    #[test]
    fn heartbeat_updates_known_agent_snapshot() {
        let service = service();
        let id = agent_id(1);
        service
            .register_agent(
                id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();

        let snapshot = service
            .heartbeat(id, Timestamp::from_unix_millis(200))
            .unwrap();

        assert_eq!(snapshot.id, id);
        assert_eq!(snapshot.last_heartbeat_at, Timestamp::from_unix_millis(200));
    }

    #[test]
    fn heartbeat_unknown_agent_returns_not_found() {
        let error = service()
            .heartbeat(agent_id(99), Timestamp::from_unix_millis(200))
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::NotFound);
    }

    #[test]
    fn heartbeat_stale_timestamp_returns_conflict_without_rewinding() {
        let service = service();
        let id = agent_id(1);
        service
            .register_agent(
                id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();
        service
            .heartbeat(id, Timestamp::from_unix_millis(200))
            .unwrap();

        let error = service
            .heartbeat(id, Timestamp::from_unix_millis(199))
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(
            service.store.get_agent(id).unwrap().last_heartbeat_at(),
            Timestamp::from_unix_millis(200)
        );
    }

    #[test]
    fn heartbeat_equal_timestamp_is_accepted() {
        let service = service();
        let id = agent_id(1);
        service
            .register_agent(
                id,
                "worker.example:9000".into(),
                Timestamp::from_unix_millis(100),
            )
            .unwrap();

        let snapshot = service
            .heartbeat(id, Timestamp::from_unix_millis(100))
            .unwrap();

        assert_eq!(snapshot.last_heartbeat_at, Timestamp::from_unix_millis(100));
    }
}
