use crate::domain::{Agent, AgentAvailability, AgentHealth, AgentId};

#[cfg_attr(
    not(test),
    expect(dead_code, reason = "M4.3 scheduler tick will consume agent selection")
)]
pub(crate) fn select_agent(agents: &[Agent]) -> Option<AgentId> {
    agents
        .iter()
        .filter(|agent| {
            agent.health() == AgentHealth::Healthy
                && agent.availability() == AgentAvailability::Idle
        })
        .map(Agent::id)
        .min()
}

#[cfg(test)]
mod tests {
    use super::select_agent;
    use crate::domain::{Agent, AgentAvailability, AgentId, Timestamp};

    fn agent_id(suffix: u32) -> AgentId {
        format!("00000000-0000-4000-8001-{suffix:012x}")
            .parse()
            .expect("fixed UUID is valid")
    }

    fn agent(suffix: u32) -> Agent {
        Agent::new(
            agent_id(suffix),
            "worker.example:9000".into(),
            Timestamp::from_unix_millis(100),
        )
    }

    fn busy_agent(suffix: u32) -> Agent {
        let mut candidate = agent(suffix);
        candidate.set_availability(AgentAvailability::Busy);
        candidate
    }

    fn unhealthy_agent(suffix: u32) -> Agent {
        let mut candidate = agent(suffix);
        candidate.mark_unhealthy();
        candidate
    }

    #[test]
    fn select_agent_returns_none_for_empty_snapshot() {
        assert_eq!(select_agent(&[]), None);
    }

    #[test]
    fn select_agent_filters_candidates_and_is_independent_of_input_order() {
        let candidates = vec![agent(9), busy_agent(1), unhealthy_agent(2), agent(4)];

        assert_eq!(select_agent(&candidates), Some(agent_id(4)));
        assert_eq!(
            select_agent(&candidates.into_iter().rev().collect::<Vec<_>>()),
            Some(agent_id(4))
        );
    }

    #[test]
    fn select_agent_returns_none_when_no_candidate_is_healthy_and_idle() {
        let candidates = vec![busy_agent(1), unhealthy_agent(2)];

        assert_eq!(select_agent(&candidates), None);
    }
}
