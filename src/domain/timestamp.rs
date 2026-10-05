use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    pub fn from_unix_millis(value: u64) -> Self {
        Self(value)
    }

    pub fn as_unix_millis(&self) -> u64 {
        self.0
    }
}

#[cfg(test)]
mod tests {
    use super::Timestamp;

    #[test]
    fn timestamp_round_trips_unix_millis_including_u64_max() {
        let timestamp = Timestamp::from_unix_millis(u64::MAX);

        assert_eq!(timestamp.as_unix_millis(), u64::MAX);

        let json = serde_json::to_string(&timestamp).expect("timestamp should serialize");
        assert_eq!(json, u64::MAX.to_string());

        let decoded: Timestamp = serde_json::from_str(&json).expect("timestamp should deserialize");
        assert_eq!(decoded, timestamp);
    }
}
