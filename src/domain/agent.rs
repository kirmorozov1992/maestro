use crate::domain::{AgentId, Timestamp};
use serde::Serialize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Agent {
    id: AgentId,
    address: String,
    last_heartbeat_at: Timestamp,
    health: AgentHealth,
    availability: AgentAvailability,
}

impl Agent {
    pub fn new(id: AgentId, address: String, registered_at: Timestamp) -> Self {
        Self {
            id,
            address,
            last_heartbeat_at: registered_at,
            health: AgentHealth::Healthy,
            availability: AgentAvailability::Idle,
        }
    }

    pub fn id(&self) -> AgentId {
        self.id
    }

    pub fn address(&self) -> &str {
        &self.address
    }

    pub fn last_heartbeat_at(&self) -> Timestamp {
        self.last_heartbeat_at
    }

    pub fn health(&self) -> AgentHealth {
        self.health
    }

    pub fn availability(&self) -> AgentAvailability {
        self.availability
    }

    pub(crate) fn update_last_heartbeat_at(&mut self, timestamp: Timestamp) {
        self.last_heartbeat_at = timestamp;
    }

    pub(crate) fn set_availability(&mut self, availability: AgentAvailability) {
        self.availability = availability;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentHealth {
    Healthy,
    Unhealthy,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentAvailability {
    Idle,
    Busy,
}

#[cfg(test)]
mod tests {
    use super::{Agent, AgentAvailability, AgentHealth};
    use crate::domain::{AgentId, Timestamp};

    #[test]
    fn agent_starts_healthy_and_idle_at_registration_time() {
        let registered_at = Timestamp::from_unix_millis(1_742_000_000_456);
        let address = "worker.example:9000".to_owned();
        let agent = Agent::new(AgentId::new(), address.clone(), registered_at);

        assert_eq!(agent.address(), address);
        assert_eq!(agent.last_heartbeat_at(), registered_at);
        assert_eq!(agent.health(), AgentHealth::Healthy);
        assert_eq!(agent.availability(), AgentAvailability::Idle);
    }

    #[test]
    fn agent_state_enums_use_snake_case_serde_names() {
        assert_eq!(
            serde_json::to_string(&AgentHealth::Healthy).unwrap(),
            "\"healthy\""
        );
        assert_eq!(
            serde_json::to_string(&AgentHealth::Unhealthy).unwrap(),
            "\"unhealthy\""
        );
        assert_eq!(
            serde_json::to_string(&AgentAvailability::Idle).unwrap(),
            "\"idle\""
        );
        assert_eq!(
            serde_json::to_string(&AgentAvailability::Busy).unwrap(),
            "\"busy\""
        );
    }
}
