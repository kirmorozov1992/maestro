use crate::domain::{AgentId, AllocationId, JobId, Timestamp};
use serde::{Deserialize, Deserializer, Serialize, de};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum JobEvent {
    Submitted {
        job_id: JobId,
        occurred_at: Timestamp,
    },
    Assigned {
        job_id: JobId,
        allocation_id: AllocationId,
        agent_id: AgentId,
        occurred_at: Timestamp,
    },
    Started {
        job_id: JobId,
        allocation_id: AllocationId,
        agent_id: AgentId,
        occurred_at: Timestamp,
    },
    Finished {
        job_id: JobId,
        allocation_id: AllocationId,
        agent_id: AgentId,
        occurred_at: Timestamp,
        result: ProcessResult,
    },
    Stopped {
        job_id: JobId,
        allocation_id: Option<AllocationId>,
        agent_id: Option<AgentId>,
        occurred_at: Timestamp,
        reason: StopReason,
    },
    Lost {
        job_id: JobId,
        allocation_id: AllocationId,
        agent_id: AgentId,
        occurred_at: Timestamp,
        reason: LostReason,
    },
}

impl<'de> Deserialize<'de> for JobEvent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(tag = "type", rename_all = "snake_case")]
        enum RawJobEvent {
            Submitted {
                job_id: JobId,
                occurred_at: Timestamp,
            },
            Assigned {
                job_id: JobId,
                allocation_id: AllocationId,
                agent_id: AgentId,
                occurred_at: Timestamp,
            },
            Started {
                job_id: JobId,
                allocation_id: AllocationId,
                agent_id: AgentId,
                occurred_at: Timestamp,
            },
            Finished {
                job_id: JobId,
                allocation_id: AllocationId,
                agent_id: AgentId,
                occurred_at: Timestamp,
                result: ProcessResult,
            },
            Stopped {
                job_id: JobId,
                allocation_id: Option<AllocationId>,
                agent_id: Option<AgentId>,
                occurred_at: Timestamp,
                reason: StopReason,
            },
            Lost {
                job_id: JobId,
                allocation_id: AllocationId,
                agent_id: AgentId,
                occurred_at: Timestamp,
                reason: LostReason,
            },
        }

        match RawJobEvent::deserialize(deserializer)? {
            RawJobEvent::Submitted {
                job_id,
                occurred_at,
            } => Ok(Self::Submitted {
                job_id,
                occurred_at,
            }),
            RawJobEvent::Assigned {
                job_id,
                allocation_id,
                agent_id,
                occurred_at,
            } => Ok(Self::Assigned {
                job_id,
                allocation_id,
                agent_id,
                occurred_at,
            }),
            RawJobEvent::Started {
                job_id,
                allocation_id,
                agent_id,
                occurred_at,
            } => Ok(Self::Started {
                job_id,
                allocation_id,
                agent_id,
                occurred_at,
            }),
            RawJobEvent::Finished {
                job_id,
                allocation_id,
                agent_id,
                occurred_at,
                result,
            } => Ok(Self::Finished {
                job_id,
                allocation_id,
                agent_id,
                occurred_at,
                result,
            }),
            RawJobEvent::Stopped {
                job_id,
                allocation_id: Some(allocation_id),
                agent_id: Some(agent_id),
                occurred_at,
                reason,
            } => Ok(Self::Stopped {
                job_id,
                allocation_id: Some(allocation_id),
                agent_id: Some(agent_id),
                occurred_at,
                reason,
            }),
            RawJobEvent::Stopped {
                job_id,
                allocation_id: None,
                agent_id: None,
                occurred_at,
                reason,
            } => Ok(Self::Stopped {
                job_id,
                allocation_id: None,
                agent_id: None,
                occurred_at,
                reason,
            }),
            RawJobEvent::Stopped { .. } => Err(de::Error::custom(
                "stopped event requires both allocation_id and agent_id or neither",
            )),
            RawJobEvent::Lost {
                job_id,
                allocation_id,
                agent_id,
                occurred_at,
                reason,
            } => Ok(Self::Lost {
                job_id,
                allocation_id,
                agent_id,
                occurred_at,
                reason,
            }),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProcessResult {
    Exited { exit_code: i32 },
    SpawnFailed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReason {
    UserRequested,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LostReason {
    HeartbeatExpired,
}

#[cfg(test)]
mod tests {
    use super::{JobEvent, LostReason, ProcessResult, StopReason};
    use crate::domain::{AgentId, AllocationId, JobId, Timestamp};
    use serde_json::{Value, json};

    fn fixture_ids() -> (JobId, AllocationId, AgentId) {
        (
            "550e8400-e29b-41d4-a716-446655440000".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440002".parse().unwrap(),
            "550e8400-e29b-41d4-a716-446655440001".parse().unwrap(),
        )
    }

    #[test]
    fn job_events_round_trip_with_stable_tags_and_fields() {
        let (job_id, allocation_id, agent_id) = fixture_ids();
        let occurred_at = Timestamp::from_unix_millis(1_742_000_000_123);
        let events = [
            (
                JobEvent::Submitted {
                    job_id,
                    occurred_at,
                },
                json!({
                    "type": "submitted",
                    "job_id": job_id.to_string(),
                    "occurred_at": 1_742_000_000_123_u64
                }),
            ),
            (
                JobEvent::Assigned {
                    job_id,
                    allocation_id,
                    agent_id,
                    occurred_at,
                },
                json!({
                    "type": "assigned",
                    "job_id": job_id.to_string(),
                    "allocation_id": allocation_id.to_string(),
                    "agent_id": agent_id.to_string(),
                    "occurred_at": 1_742_000_000_123_u64
                }),
            ),
            (
                JobEvent::Started {
                    job_id,
                    allocation_id,
                    agent_id,
                    occurred_at,
                },
                json!({
                    "type": "started",
                    "job_id": job_id.to_string(),
                    "allocation_id": allocation_id.to_string(),
                    "agent_id": agent_id.to_string(),
                    "occurred_at": 1_742_000_000_123_u64
                }),
            ),
            (
                JobEvent::Finished {
                    job_id,
                    allocation_id,
                    agent_id,
                    occurred_at,
                    result: ProcessResult::Exited { exit_code: 7 },
                },
                json!({
                    "type": "finished",
                    "job_id": job_id.to_string(),
                    "allocation_id": allocation_id.to_string(),
                    "agent_id": agent_id.to_string(),
                    "occurred_at": 1_742_000_000_123_u64,
                    "result": {"type": "exited", "exit_code": 7}
                }),
            ),
            (
                JobEvent::Stopped {
                    job_id,
                    allocation_id: Some(allocation_id),
                    agent_id: Some(agent_id),
                    occurred_at,
                    reason: StopReason::UserRequested,
                },
                json!({
                    "type": "stopped",
                    "job_id": job_id.to_string(),
                    "allocation_id": allocation_id.to_string(),
                    "agent_id": agent_id.to_string(),
                    "occurred_at": 1_742_000_000_123_u64,
                    "reason": "user_requested"
                }),
            ),
            (
                JobEvent::Lost {
                    job_id,
                    allocation_id,
                    agent_id,
                    occurred_at,
                    reason: LostReason::HeartbeatExpired,
                },
                json!({
                    "type": "lost",
                    "job_id": job_id.to_string(),
                    "allocation_id": allocation_id.to_string(),
                    "agent_id": agent_id.to_string(),
                    "occurred_at": 1_742_000_000_123_u64,
                    "reason": "heartbeat_expired"
                }),
            ),
        ];

        for (event, expected) in events {
            let serialized = serde_json::to_value(&event).unwrap();
            assert_eq!(serialized, expected);

            let decoded: JobEvent = serde_json::from_value(serialized).unwrap();
            assert_eq!(decoded, event);
        }

        let spawn_failed = ProcessResult::SpawnFailed;
        let encoded: Value = serde_json::to_value(spawn_failed).unwrap();
        assert_eq!(encoded, json!({"type": "spawn_failed"}));
        assert_eq!(
            serde_json::from_value::<ProcessResult>(encoded).unwrap(),
            spawn_failed
        );
    }

    #[test]
    fn stopped_event_requires_both_context_ids_or_neither() {
        let (job_id, allocation_id, agent_id) = fixture_ids();
        let common = json!({
            "type": "stopped",
            "job_id": job_id.to_string(),
            "occurred_at": 1_742_000_000_123_u64,
            "reason": "user_requested"
        });

        let mut neither = common.clone();
        neither["allocation_id"] = Value::Null;
        neither["agent_id"] = Value::Null;
        assert!(serde_json::from_value::<JobEvent>(neither).is_ok());

        let mut both = common.clone();
        both["allocation_id"] = json!(allocation_id.to_string());
        both["agent_id"] = json!(agent_id.to_string());
        assert!(serde_json::from_value::<JobEvent>(both).is_ok());

        let mut allocation_only = common.clone();
        allocation_only["allocation_id"] = json!(allocation_id.to_string());
        allocation_only["agent_id"] = Value::Null;
        assert!(serde_json::from_value::<JobEvent>(allocation_only).is_err());

        let mut agent_only = common;
        agent_only["allocation_id"] = Value::Null;
        agent_only["agent_id"] = json!(agent_id.to_string());
        assert!(serde_json::from_value::<JobEvent>(agent_only).is_err());
    }
}
