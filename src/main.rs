mod application;
mod config;
pub mod domain;
pub mod error;
mod scheduler;
mod scheduler_loop;
mod server;
pub(crate) mod state;
mod transport;
mod worker;

use clap::{Args, Parser, Subcommand, error::ErrorKind};
use std::{error::Error, fmt, net::SocketAddr, path::PathBuf, process::ExitCode};

#[derive(Debug, Parser)]
#[command(name = "maestro", about = "A small job orchestrator")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Server(ServerOptions),
    Agent(AgentOptions),
    Submit(ClientOptions),
    Status(ClientOptions),
    Stop(ClientOptions),
}

#[derive(Debug, Args)]
struct ServerOptions {
    #[arg(long, help = config::HELP_BIND_ADDR)]
    bind_addr: Option<SocketAddr>,
    #[arg(
        long,
        value_parser = clap::value_parser!(u64).range(1..),
        help = config::HELP_LIVENESS_TIMEOUT
    )]
    liveness_timeout_secs: Option<u64>,
    #[arg(
        long,
        value_parser = clap::value_parser!(u64).range(1..),
        help = config::HELP_ASSIGNMENT_TIMEOUT
    )]
    assignment_timeout_secs: Option<u64>,
    #[arg(
        long,
        value_parser = clap::value_parser!(u64).range(1..),
        help = config::HELP_SCHEDULER_INTERVAL
    )]
    scheduler_interval_secs: Option<u64>,
}

#[derive(Debug, Args)]
struct AgentOptions {
    #[arg(long, help = config::HELP_SERVER_ADDR)]
    server_addr: Option<SocketAddr>,
    #[arg(
        long,
        value_parser = clap::value_parser!(u64).range(1..),
        help = config::HELP_HEARTBEAT_INTERVAL
    )]
    heartbeat_interval_secs: Option<u64>,
    #[arg(
        long,
        value_parser = clap::value_parser!(u64).range(1..),
        help = config::HELP_POLL_INTERVAL
    )]
    poll_interval_secs: Option<u64>,
    #[arg(long, help = config::HELP_WORKING_DIR)]
    working_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ClientOptions {
    #[arg(long, help = config::HELP_SERVER_ADDR)]
    server_addr: Option<SocketAddr>,
}

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            let _ = error.print();
            return ExitCode::SUCCESS;
        }
        Err(error) => {
            let message = match error.kind() {
                ErrorKind::InvalidSubcommand => "unknown command; run `maestro --help` for usage",
                ErrorKind::UnknownArgument => {
                    "unknown option or argument; run `maestro --help` for usage"
                }
                ErrorKind::InvalidValue | ErrorKind::ValueValidation => {
                    "invalid configuration value; check the command help for valid values"
                }
                _ => "invalid command-line arguments; run `maestro --help` for usage",
            };
            eprintln!("{message}");
            return ExitCode::from(2);
        }
    };

    let environment = config::Environment::from_process();
    match launch(cli.command, &environment) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::from(2)
        }
    }
}

fn launch(command: Command, environment: &config::Environment) -> Result<(), LaunchError> {
    match command {
        Command::Server(options) => {
            server::run(config::server(
                options.bind_addr,
                options.liveness_timeout_secs,
                options.assignment_timeout_secs,
                options.scheduler_interval_secs,
                environment,
            )?)?;
        }
        Command::Agent(options) => {
            launch_mode(
                "agent",
                config::agent(
                    options.server_addr,
                    options.heartbeat_interval_secs,
                    options.poll_interval_secs,
                    options.working_dir,
                    environment,
                )?,
            );
        }
        Command::Submit(options) => {
            launch_mode("submit", config::client(options.server_addr, environment)?);
        }
        Command::Status(options) => {
            launch_mode("status", config::client(options.server_addr, environment)?);
        }
        Command::Stop(options) => {
            launch_mode("stop", config::client(options.server_addr, environment)?);
        }
    }
    Ok(())
}

#[derive(Debug)]
enum LaunchError {
    Configuration(config::ConfigError),
    Server(server::ServerError),
}

impl From<config::ConfigError> for LaunchError {
    fn from(error: config::ConfigError) -> Self {
        Self::Configuration(error)
    }
}

impl From<server::ServerError> for LaunchError {
    fn from(error: server::ServerError) -> Self {
        Self::Server(error)
    }
}

impl fmt::Display for LaunchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Configuration(error) => error.fmt(formatter),
            Self::Server(error) => error.fmt(formatter),
        }
    }
}

impl Error for LaunchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Configuration(error) => Some(error),
            Self::Server(error) => Some(error),
        }
    }
}

fn launch_mode(mode: &str, _configuration: impl std::fmt::Debug) {
    println!("maestro {mode} mode is not implemented yet");
}
