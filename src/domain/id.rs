use serde::{Deserialize, Serialize};
use std::{fmt, str::FromStr};
use uuid::Uuid;

macro_rules! define_id {
    ($id_type:ident) => {
        #[derive(
            Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
        )]
        #[serde(transparent)]
        pub struct $id_type(Uuid);

        impl $id_type {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl Default for $id_type {
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $id_type {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $id_type {
            type Err = uuid::Error;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Uuid::parse_str(value).map(Self)
            }
        }
    };
}

define_id!(JobId);
define_id!(AgentId);
define_id!(AllocationId);

#[cfg(test)]
mod tests {
    use super::{AgentId, AllocationId, JobId};
    use serde::{Serialize, de::DeserializeOwned};
    use std::{
        fmt::{Debug, Display},
        str::FromStr,
    };
    use uuid::Uuid;

    fn assert_id_contract<T>(new_id: fn() -> T)
    where
        T: Copy
            + Debug
            + Default
            + Eq
            + Display
            + FromStr<Err = uuid::Error>
            + Serialize
            + DeserializeOwned,
    {
        let id = new_id();
        let other_id = new_id();
        assert_ne!(id, other_id);

        let default_id = T::default();
        assert_eq!(
            Uuid::parse_str(&default_id.to_string())
                .unwrap()
                .get_version_num(),
            4
        );

        let text = id.to_string();
        let parsed = text.parse::<T>().expect("displayed ID should parse");
        assert_eq!(parsed, id);
        let fixture = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(fixture.parse::<T>().unwrap().to_string(), fixture);
        assert_eq!(Uuid::parse_str(&text).unwrap().get_version_num(), 4);

        let json = serde_json::to_string(&id).expect("ID should serialize");
        assert_eq!(json, format!("\"{text}\""));
        let decoded = serde_json::from_str::<T>(&json).expect("ID should deserialize");
        assert_eq!(decoded, id);
        assert!("not-an-id".parse::<T>().is_err());
    }

    #[test]
    fn job_id_generates_parses_displays_and_serializes() {
        assert_id_contract(JobId::new);
    }

    #[test]
    fn agent_id_generates_parses_displays_and_serializes() {
        assert_id_contract(AgentId::new);
    }

    #[test]
    fn allocation_id_generates_parses_displays_and_serializes() {
        assert_id_contract(AllocationId::new);
    }
}
