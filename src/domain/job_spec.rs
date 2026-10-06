use serde::{
    Deserialize, Deserializer, Serialize,
    de::{self, MapAccess, Visitor},
};
use std::{collections::BTreeMap, error::Error, fmt};

pub const MAX_COMMAND_BYTES: usize = 4 * 1024;
pub const MAX_ARGUMENTS_BYTES: usize = 64 * 1024;
pub const MAX_ENVIRONMENT_BYTES: usize = 64 * 1024;
pub const MAX_WORKING_DIR_BYTES: usize = 4 * 1024;

/// A user-provided command and its inputs, separate from job runtime state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct JobSpec {
    command: String,
    args: Vec<String>,
    env: Option<BTreeMap<String, String>>,
    working_dir: Option<String>,
}

impl JobSpec {
    pub fn new(
        command: impl Into<String>,
        args: Vec<String>,
        env: Option<BTreeMap<String, String>>,
        working_dir: Option<String>,
    ) -> Result<Self, JobSpecError> {
        let spec = Self {
            command: command.into(),
            args,
            env,
            working_dir,
        };
        spec.validate()?;
        Ok(spec)
    }

    pub fn command(&self) -> &str {
        &self.command
    }

    pub fn args(&self) -> &[String] {
        &self.args
    }

    pub fn env(&self) -> Option<&BTreeMap<String, String>> {
        self.env.as_ref()
    }

    pub fn working_dir(&self) -> Option<&str> {
        self.working_dir.as_deref()
    }

    fn validate(&self) -> Result<(), JobSpecError> {
        if self.command.trim().is_empty() {
            return Err(JobSpecError::EmptyCommand);
        }
        if self
            .working_dir
            .as_deref()
            .is_some_and(|working_dir| working_dir.trim().is_empty())
        {
            return Err(JobSpecError::EmptyWorkingDirectory);
        }
        ensure_within_limit(
            self.command.len(),
            MAX_COMMAND_BYTES,
            |actual_bytes, max_bytes| JobSpecError::CommandTooLong {
                actual_bytes,
                max_bytes,
            },
        )?;

        // Count one separator byte per argument so empty arguments also consume budget.
        let arguments_bytes = self
            .args
            .iter()
            .map(|argument| argument.len().saturating_add(1))
            .fold(0usize, usize::saturating_add);
        ensure_within_limit(
            arguments_bytes,
            MAX_ARGUMENTS_BYTES,
            |actual_bytes, max_bytes| JobSpecError::ArgumentsTooLong {
                actual_bytes,
                max_bytes,
            },
        )?;

        if let Some(env) = &self.env {
            // Count two separator bytes per pair in addition to key and value bytes.
            let environment_bytes = env
                .iter()
                .map(|(key, value)| key.len().saturating_add(value.len()).saturating_add(2))
                .fold(0usize, usize::saturating_add);
            ensure_within_limit(
                environment_bytes,
                MAX_ENVIRONMENT_BYTES,
                |actual_bytes, max_bytes| JobSpecError::EnvironmentTooLong {
                    actual_bytes,
                    max_bytes,
                },
            )?;
        }

        if let Some(working_dir) = &self.working_dir {
            ensure_within_limit(
                working_dir.len(),
                MAX_WORKING_DIR_BYTES,
                |actual_bytes, max_bytes| JobSpecError::WorkingDirectoryTooLong {
                    actual_bytes,
                    max_bytes,
                },
            )?;
        }

        Ok(())
    }
}

impl<'de> Deserialize<'de> for JobSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct JobSpecInput {
            command: String,
            #[serde(default)]
            args: Vec<String>,
            #[serde(default, deserialize_with = "deserialize_environment")]
            env: Option<BTreeMap<String, String>>,
            #[serde(default)]
            working_dir: Option<String>,
        }

        let input = JobSpecInput::deserialize(deserializer)?;
        Self::new(input.command, input.args, input.env, input.working_dir)
            .map_err(de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobSpecError {
    EmptyCommand,
    EmptyWorkingDirectory,
    CommandTooLong {
        actual_bytes: usize,
        max_bytes: usize,
    },
    ArgumentsTooLong {
        actual_bytes: usize,
        max_bytes: usize,
    },
    EnvironmentTooLong {
        actual_bytes: usize,
        max_bytes: usize,
    },
    WorkingDirectoryTooLong {
        actual_bytes: usize,
        max_bytes: usize,
    },
}

impl JobSpecError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::EmptyCommand => "empty_command",
            Self::EmptyWorkingDirectory => "empty_working_directory",
            Self::CommandTooLong { .. } => "command_too_long",
            Self::ArgumentsTooLong { .. } => "arguments_too_long",
            Self::EnvironmentTooLong { .. } => "environment_too_long",
            Self::WorkingDirectoryTooLong { .. } => "working_directory_too_long",
        }
    }
}

impl fmt::Display for JobSpecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCommand => formatter.write_str("command must not be empty"),
            Self::EmptyWorkingDirectory => {
                formatter.write_str("working directory must not be empty")
            }
            Self::CommandTooLong {
                actual_bytes,
                max_bytes,
            } => write!(
                formatter,
                "command uses {actual_bytes} bytes; limit is {max_bytes} bytes"
            ),
            Self::ArgumentsTooLong {
                actual_bytes,
                max_bytes,
            } => write!(
                formatter,
                "arguments use {actual_bytes} bytes; limit is {max_bytes} bytes"
            ),
            Self::EnvironmentTooLong {
                actual_bytes,
                max_bytes,
            } => write!(
                formatter,
                "environment uses {actual_bytes} bytes; limit is {max_bytes} bytes"
            ),
            Self::WorkingDirectoryTooLong {
                actual_bytes,
                max_bytes,
            } => write!(
                formatter,
                "working directory uses {actual_bytes} bytes; limit is {max_bytes} bytes"
            ),
        }
    }
}

impl Error for JobSpecError {}

fn ensure_within_limit(
    actual_bytes: usize,
    max_bytes: usize,
    make_error: fn(usize, usize) -> JobSpecError,
) -> Result<(), JobSpecError> {
    if actual_bytes <= max_bytes {
        Ok(())
    } else {
        Err(make_error(actual_bytes, max_bytes))
    }
}

struct UniqueEnvironment(BTreeMap<String, String>);

impl<'de> Deserialize<'de> for UniqueEnvironment {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EnvironmentVisitor;

        impl<'de> Visitor<'de> for EnvironmentVisitor {
            type Value = UniqueEnvironment;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an object with unique environment keys")
            }

            fn visit_map<A>(self, mut access: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut environment = BTreeMap::new();
                while let Some((key, value)) = access.next_entry::<String, String>()? {
                    if environment.insert(key, value).is_some() {
                        return Err(de::Error::custom("duplicate environment key"));
                    }
                }
                Ok(UniqueEnvironment(environment))
            }
        }

        deserializer.deserialize_map(EnvironmentVisitor)
    }
}

fn deserialize_environment<'de, D>(
    deserializer: D,
) -> Result<Option<BTreeMap<String, String>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<UniqueEnvironment>::deserialize(deserializer)
        .map(|environment| environment.map(|unique| unique.0))
}

#[cfg(test)]
mod tests {
    use super::{
        JobSpec, JobSpecError, MAX_ARGUMENTS_BYTES, MAX_COMMAND_BYTES, MAX_ENVIRONMENT_BYTES,
        MAX_WORKING_DIR_BYTES,
    };
    use std::collections::BTreeMap;

    #[test]
    fn preserves_argument_order_and_round_trips_user_fields() {
        let spec = JobSpec::new(
            "program",
            vec!["first".into(), "second".into()],
            Some(BTreeMap::from([("MODE".into(), "fast".into())])),
            Some("/work".into()),
        )
        .unwrap();

        assert_eq!(spec.command(), "program");
        assert_eq!(spec.args(), ["first", "second"]);
        assert_eq!(spec.env().unwrap().get("MODE").unwrap(), "fast");
        assert_eq!(spec.working_dir(), Some("/work"));

        let json = serde_json::to_string(&spec).unwrap();
        assert_eq!(
            json,
            r#"{"command":"program","args":["first","second"],"env":{"MODE":"fast"},"working_dir":"/work"}"#
        );
        let decoded: JobSpec = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, spec);
    }

    #[test]
    fn command_is_required_and_args_default_to_an_empty_list() {
        let missing_command = serde_json::from_str::<JobSpec>(r#"{"args":[]}"#);
        assert!(missing_command.is_err());

        let spec: JobSpec = serde_json::from_str(r#"{"command":"program"}"#).unwrap();
        assert!(spec.args().is_empty());
        assert!(spec.env().is_none());
        assert!(spec.working_dir().is_none());
    }

    #[test]
    fn rejects_empty_commands_in_constructor_and_deserialization() {
        let error = JobSpec::new(" \t\n", vec![], None, None).unwrap_err();
        assert_eq!(error, JobSpecError::EmptyCommand);
        assert_eq!(error.code(), "empty_command");

        let error = serde_json::from_str::<JobSpec>(r#"{"command":"  "}"#).unwrap_err();
        assert_eq!(error.to_string(), JobSpecError::EmptyCommand.to_string());
    }

    #[test]
    fn rejects_blank_working_directory_with_a_stable_code() {
        let error = JobSpec::new("program", vec![], None, Some(" \t".into())).unwrap_err();
        assert_eq!(error, JobSpecError::EmptyWorkingDirectory);
        assert_eq!(error.code(), "empty_working_directory");
    }

    #[test]
    fn job_spec_errors_have_stable_codes() {
        let errors = [
            (JobSpecError::EmptyCommand, "empty_command"),
            (
                JobSpecError::EmptyWorkingDirectory,
                "empty_working_directory",
            ),
            (
                JobSpecError::CommandTooLong {
                    actual_bytes: 2,
                    max_bytes: 1,
                },
                "command_too_long",
            ),
            (
                JobSpecError::ArgumentsTooLong {
                    actual_bytes: 2,
                    max_bytes: 1,
                },
                "arguments_too_long",
            ),
            (
                JobSpecError::EnvironmentTooLong {
                    actual_bytes: 2,
                    max_bytes: 1,
                },
                "environment_too_long",
            ),
            (
                JobSpecError::WorkingDirectoryTooLong {
                    actual_bytes: 2,
                    max_bytes: 1,
                },
                "working_directory_too_long",
            ),
        ];

        for (error, expected_code) in errors {
            assert_eq!(error.code(), expected_code);
        }
    }

    #[test]
    fn preserves_shell_metacharacters_as_literal_input() {
        let spec = JobSpec::new(
            "program; $HOME",
            vec!["$(touch marker)".into(), "two words".into()],
            None,
            None,
        )
        .unwrap();

        assert_eq!(spec.command(), "program; $HOME");
        assert_eq!(spec.args(), ["$(touch marker)", "two words"]);
    }

    #[test]
    fn rejects_duplicate_environment_keys() {
        let result = serde_json::from_str::<JobSpec>(
            r#"{"command":"program","env":{"MODE":"fast","MODE":"safe"}}"#,
        );

        assert!(result.is_err());
    }

    #[test]
    fn rejects_unknown_runtime_fields_and_oversized_deserialized_values() {
        let runtime_field =
            serde_json::from_str::<JobSpec>(r#"{"command":"program","status":"pending"}"#);
        assert!(runtime_field.is_err());

        let oversized_command =
            format!("{{\"command\":\"{}\"}}", "x".repeat(MAX_COMMAND_BYTES + 1));
        assert!(serde_json::from_str::<JobSpec>(&oversized_command).is_err());
    }

    #[test]
    fn enforces_utf8_byte_limits_including_exact_boundaries() {
        let at_limit = JobSpec::new(
            "x".repeat(MAX_COMMAND_BYTES),
            vec!["x".repeat(MAX_ARGUMENTS_BYTES - 1)],
            Some(BTreeMap::from([(
                "K".into(),
                "x".repeat(MAX_ENVIRONMENT_BYTES - 3),
            )])),
            Some("x".repeat(MAX_WORKING_DIR_BYTES)),
        );
        assert!(at_limit.is_ok());

        assert!(matches!(
            JobSpec::new("é".repeat(MAX_COMMAND_BYTES / 2 + 1), vec![], None, None),
            Err(JobSpecError::CommandTooLong { .. })
        ));
        assert!(matches!(
            JobSpec::new("program", vec!["x".repeat(MAX_ARGUMENTS_BYTES)], None, None),
            Err(JobSpecError::ArgumentsTooLong { .. })
        ));
        assert!(matches!(
            JobSpec::new(
                "program",
                vec![],
                Some(BTreeMap::from([(
                    "K".into(),
                    "x".repeat(MAX_ENVIRONMENT_BYTES - 2)
                )])),
                None
            ),
            Err(JobSpecError::EnvironmentTooLong { .. })
        ));
        assert!(matches!(
            JobSpec::new(
                "program",
                vec![],
                None,
                Some("x".repeat(MAX_WORKING_DIR_BYTES + 1))
            ),
            Err(JobSpecError::WorkingDirectoryTooLong { .. })
        ));
    }
}
