//! Configuration resolution and validation for CLI modes.

use std::{
    env,
    error::Error,
    ffi::{OsStr, OsString},
    fmt, fs, io,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

pub(crate) const ENV_SERVER_ADDR: &str = "MAESTRO_SERVER_ADDR";
pub(crate) const ENV_BIND_ADDR: &str = "MAESTRO_BIND_ADDR";
pub(crate) const ENV_HEARTBEAT_INTERVAL_SECS: &str = "MAESTRO_HEARTBEAT_INTERVAL_SECS";
pub(crate) const ENV_POLL_INTERVAL_SECS: &str = "MAESTRO_POLL_INTERVAL_SECS";
pub(crate) const ENV_LIVENESS_TIMEOUT_SECS: &str = "MAESTRO_LIVENESS_TIMEOUT_SECS";
pub(crate) const ENV_WORKING_DIR: &str = "MAESTRO_WORKING_DIR";

pub(crate) const DEFAULT_WORKING_DIR_TEXT: &str = ".";
pub(crate) const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 5;
pub(crate) const DEFAULT_POLL_INTERVAL_SECS: u64 = 1;
pub(crate) const DEFAULT_LIVENESS_TIMEOUT_SECS: u64 = 30;

pub(crate) const HELP_SERVER_ADDR: &str =
    "Control-plane address (default: 127.0.0.1:8080; env: MAESTRO_SERVER_ADDR)";
pub(crate) const HELP_BIND_ADDR: &str =
    "Bind address (default: 127.0.0.1:8080; env: MAESTRO_BIND_ADDR)";
pub(crate) const HELP_HEARTBEAT_INTERVAL: &str =
    "Heartbeat interval in seconds (default: 5; env: MAESTRO_HEARTBEAT_INTERVAL_SECS)";
pub(crate) const HELP_POLL_INTERVAL: &str =
    "Assignment polling interval in seconds (default: 1; env: MAESTRO_POLL_INTERVAL_SECS)";
pub(crate) const HELP_LIVENESS_TIMEOUT: &str =
    "Agent liveness timeout in seconds (default: 30; env: MAESTRO_LIVENESS_TIMEOUT_SECS)";
pub(crate) const HELP_WORKING_DIR: &str =
    "Agent working directory (default: .; env: MAESTRO_WORKING_DIR)";

const DEFAULT_SERVER_ADDR: SocketAddr =
    SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));
const DEFAULT_BIND_ADDR: SocketAddr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 8080));

#[derive(Debug, Default)]
pub(crate) struct Environment {
    server_addr: Option<OsString>,
    bind_addr: Option<OsString>,
    heartbeat_interval_secs: Option<OsString>,
    poll_interval_secs: Option<OsString>,
    liveness_timeout_secs: Option<OsString>,
    working_dir: Option<OsString>,
}

impl Environment {
    pub(crate) fn from_process() -> Self {
        Self {
            server_addr: env::var_os(ENV_SERVER_ADDR),
            bind_addr: env::var_os(ENV_BIND_ADDR),
            heartbeat_interval_secs: env::var_os(ENV_HEARTBEAT_INTERVAL_SECS),
            poll_interval_secs: env::var_os(ENV_POLL_INTERVAL_SECS),
            liveness_timeout_secs: env::var_os(ENV_LIVENESS_TIMEOUT_SECS),
            working_dir: env::var_os(ENV_WORKING_DIR),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ServerConfig {
    pub(crate) bind_addr: SocketAddr,
    pub(crate) liveness_timeout: Duration,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct AgentConfig {
    pub(crate) server_addr: SocketAddr,
    pub(crate) heartbeat_interval: Duration,
    pub(crate) poll_interval: Duration,
    pub(crate) working_dir: PathBuf,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ClientConfig {
    pub(crate) server_addr: SocketAddr,
}

#[derive(Debug)]
pub(crate) enum ConfigError {
    InvalidValue { setting: &'static str },
    ZeroDuration { setting: &'static str },
    WorkingDirectoryNotDirectory,
    WorkingDirectoryIo { source: io::Error },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidValue { setting } => write!(formatter, "invalid value for {setting}"),
            Self::ZeroDuration { setting } => {
                write!(formatter, "{setting} must be greater than zero")
            }
            Self::WorkingDirectoryNotDirectory => {
                formatter.write_str("agent working directory must be a directory")
            }
            Self::WorkingDirectoryIo { .. } => {
                formatter.write_str("agent working directory could not be created or accessed")
            }
        }
    }
}

impl Error for ConfigError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::WorkingDirectoryIo { source } => Some(source),
            Self::InvalidValue { .. }
            | Self::ZeroDuration { .. }
            | Self::WorkingDirectoryNotDirectory => None,
        }
    }
}

pub(crate) fn server(
    cli_bind_addr: Option<SocketAddr>,
    cli_liveness_timeout_secs: Option<u64>,
    environment: &Environment,
) -> Result<ServerConfig, ConfigError> {
    let bind_addr = resolve_value(
        cli_bind_addr,
        environment.bind_addr.as_deref(),
        DEFAULT_BIND_ADDR,
        ENV_BIND_ADDR,
    )?;
    let liveness_timeout_secs = resolve_value(
        cli_liveness_timeout_secs,
        environment.liveness_timeout_secs.as_deref(),
        DEFAULT_LIVENESS_TIMEOUT_SECS,
        ENV_LIVENESS_TIMEOUT_SECS,
    )?;

    Ok(ServerConfig {
        bind_addr,
        liveness_timeout: positive_duration(liveness_timeout_secs, ENV_LIVENESS_TIMEOUT_SECS)?,
    })
}

pub(crate) fn agent(
    cli_server_addr: Option<SocketAddr>,
    cli_heartbeat_interval_secs: Option<u64>,
    cli_poll_interval_secs: Option<u64>,
    cli_working_dir: Option<PathBuf>,
    environment: &Environment,
) -> Result<AgentConfig, ConfigError> {
    let server_addr = resolve_value(
        cli_server_addr,
        environment.server_addr.as_deref(),
        DEFAULT_SERVER_ADDR,
        ENV_SERVER_ADDR,
    )?;
    let heartbeat_interval_secs = resolve_value(
        cli_heartbeat_interval_secs,
        environment.heartbeat_interval_secs.as_deref(),
        DEFAULT_HEARTBEAT_INTERVAL_SECS,
        ENV_HEARTBEAT_INTERVAL_SECS,
    )?;
    let poll_interval_secs = resolve_value(
        cli_poll_interval_secs,
        environment.poll_interval_secs.as_deref(),
        DEFAULT_POLL_INTERVAL_SECS,
        ENV_POLL_INTERVAL_SECS,
    )?;
    let working_dir = cli_working_dir
        .or_else(|| environment.working_dir.as_deref().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from(DEFAULT_WORKING_DIR_TEXT));

    Ok(AgentConfig {
        server_addr,
        heartbeat_interval: positive_duration(
            heartbeat_interval_secs,
            ENV_HEARTBEAT_INTERVAL_SECS,
        )?,
        poll_interval: positive_duration(poll_interval_secs, ENV_POLL_INTERVAL_SECS)?,
        working_dir: ensure_working_directory(&working_dir)?,
    })
}

pub(crate) fn client(
    cli_server_addr: Option<SocketAddr>,
    environment: &Environment,
) -> Result<ClientConfig, ConfigError> {
    Ok(ClientConfig {
        server_addr: resolve_value(
            cli_server_addr,
            environment.server_addr.as_deref(),
            DEFAULT_SERVER_ADDR,
            ENV_SERVER_ADDR,
        )?,
    })
}

fn resolve_value<T>(
    cli_value: Option<T>,
    env_value: Option<&OsStr>,
    default: T,
    setting: &'static str,
) -> Result<T, ConfigError>
where
    T: FromStr,
{
    if let Some(value) = cli_value {
        return Ok(value);
    }

    let Some(value) = env_value else {
        return Ok(default);
    };
    let value = value
        .to_str()
        .ok_or(ConfigError::InvalidValue { setting })?;
    value
        .parse()
        .map_err(|_| ConfigError::InvalidValue { setting })
}

fn positive_duration(seconds: u64, setting: &'static str) -> Result<Duration, ConfigError> {
    if seconds == 0 {
        return Err(ConfigError::ZeroDuration { setting });
    }
    Ok(Duration::from_secs(seconds))
}

fn ensure_working_directory(path: &Path) -> Result<PathBuf, ConfigError> {
    match fs::metadata(path) {
        Ok(metadata) if !metadata.is_dir() => {
            return Err(ConfigError::WorkingDirectoryNotDirectory);
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path)
                .map_err(|source| ConfigError::WorkingDirectoryIo { source })?;
        }
        Err(source) => return Err(ConfigError::WorkingDirectoryIo { source }),
    }

    let metadata =
        fs::metadata(path).map_err(|source| ConfigError::WorkingDirectoryIo { source })?;
    if !metadata.is_dir() {
        return Err(ConfigError::WorkingDirectoryNotDirectory);
    }
    fs::canonicalize(path).map_err(|source| ConfigError::WorkingDirectoryIo { source })
}

#[cfg(test)]
mod tests {
    use super::{
        AgentConfig, ConfigError, DEFAULT_BIND_ADDR, DEFAULT_HEARTBEAT_INTERVAL_SECS,
        DEFAULT_LIVENESS_TIMEOUT_SECS, DEFAULT_POLL_INTERVAL_SECS, DEFAULT_SERVER_ADDR,
        DEFAULT_WORKING_DIR_TEXT, ENV_SERVER_ADDR, Environment, agent, client, server,
    };
    use std::{
        ffi::OsString,
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        path::PathBuf,
        time::Duration,
    };

    fn address(port: u16) -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, port))
    }

    #[test]
    fn resolves_defaults_for_all_modes() {
        let environment = Environment::default();
        let server_config = server(None, None, &environment).expect("defaults are valid");
        let client_config = client(None, &environment).expect("defaults are valid");
        let agent_config = agent(None, None, None, None, &environment).expect("defaults are valid");

        assert_eq!(server_config.bind_addr, DEFAULT_BIND_ADDR);
        assert_eq!(
            server_config.liveness_timeout,
            Duration::from_secs(DEFAULT_LIVENESS_TIMEOUT_SECS)
        );
        assert_eq!(client_config.server_addr, DEFAULT_SERVER_ADDR);
        assert_eq!(agent_config.server_addr, DEFAULT_SERVER_ADDR);
        assert_eq!(
            agent_config.heartbeat_interval,
            Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL_SECS)
        );
        assert_eq!(
            agent_config.poll_interval,
            Duration::from_secs(DEFAULT_POLL_INTERVAL_SECS)
        );
        assert_eq!(
            agent_config.working_dir,
            PathBuf::from(DEFAULT_WORKING_DIR_TEXT)
                .canonicalize()
                .expect("default directory should exist")
        );
    }

    #[test]
    fn environment_overrides_defaults_and_cli_overrides_environment() {
        let environment = Environment {
            server_addr: Some(OsString::from("127.0.0.1:9000")),
            bind_addr: Some(OsString::from("127.0.0.1:9001")),
            heartbeat_interval_secs: Some(OsString::from("7")),
            poll_interval_secs: Some(OsString::from("3")),
            liveness_timeout_secs: Some(OsString::from("21")),
            ..Environment::default()
        };

        let env_client = client(None, &environment).expect("environment value is valid");
        let cli_client = client(Some(address(9002)), &environment)
            .expect("CLI value should override environment");
        let server_config = server(None, None, &environment).expect("environment values are valid");
        let agent_config =
            agent(None, None, None, None, &environment).expect("environment values are valid");

        assert_eq!(env_client.server_addr, address(9000));
        assert_eq!(cli_client.server_addr, address(9002));
        assert_eq!(server_config.bind_addr, address(9001));
        assert_eq!(server_config.liveness_timeout, Duration::from_secs(21));
        assert_eq!(agent_config.server_addr, address(9000));
        assert_eq!(agent_config.heartbeat_interval, Duration::from_secs(7));
        assert_eq!(agent_config.poll_interval, Duration::from_secs(3));
    }

    #[test]
    fn invalid_environment_values_are_rejected_without_echoing_them() {
        let environment = Environment {
            server_addr: Some(OsString::from("secret-invalid-address")),
            ..Environment::default()
        };

        let error = client(None, &environment).expect_err("invalid address must be rejected");

        assert_eq!(
            error.to_string(),
            format!("invalid value for {ENV_SERVER_ADDR}")
        );
        assert!(!error.to_string().contains("secret-invalid-address"));
    }

    #[test]
    fn cli_values_ignore_invalid_environment_values() {
        let environment = Environment {
            server_addr: Some(OsString::from("not-an-address")),
            ..Environment::default()
        };

        let config = client(Some(address(9003)), &environment)
            .expect("valid CLI address should override invalid environment");

        assert_eq!(config.server_addr, address(9003));
    }

    #[test]
    fn all_intervals_must_be_positive() {
        let environment = Environment::default();

        assert!(matches!(
            server(None, Some(0), &environment),
            Err(ConfigError::ZeroDuration { .. })
        ));
        assert!(matches!(
            agent(None, Some(0), None, None, &environment),
            Err(ConfigError::ZeroDuration { .. })
        ));
        assert!(matches!(
            agent(None, None, Some(0), None, &environment),
            Err(ConfigError::ZeroDuration { .. })
        ));

        let environment = Environment {
            poll_interval_secs: Some(OsString::from("0")),
            ..Environment::default()
        };
        assert!(matches!(
            agent(None, None, None, None, &environment),
            Err(ConfigError::ZeroDuration { .. })
        ));
    }

    #[test]
    fn agent_config_contains_the_resolved_working_directory() {
        let environment = Environment::default();
        let path = std::env::temp_dir();
        let config: AgentConfig = agent(None, None, None, Some(path.clone()), &environment)
            .expect("system temporary directory should be valid");

        assert_eq!(
            config.working_dir,
            path.canonicalize().expect("path exists")
        );
    }

    #[test]
    fn environment_working_directory_is_used_when_cli_is_absent() {
        let expected = std::env::temp_dir();
        let environment = Environment {
            working_dir: Some(expected.as_os_str().to_owned()),
            ..Environment::default()
        };

        let config = agent(None, None, None, None, &environment)
            .expect("environment working directory should be valid");

        assert_eq!(
            config.working_dir,
            expected.canonicalize().expect("path exists")
        );
    }

    #[test]
    fn cli_working_directory_overrides_environment() {
        let expected = std::env::temp_dir();
        let environment = Environment {
            working_dir: Some(OsString::from("missing/environment/path")),
            ..Environment::default()
        };

        let config = agent(None, None, None, Some(expected.clone()), &environment)
            .expect("CLI working directory should override the environment");

        assert_eq!(
            config.working_dir,
            expected.canonicalize().expect("path exists")
        );
    }
}
