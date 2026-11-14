mod application;
mod config;
// M7.4 consumes the remaining stop method.
#[allow(dead_code)]
mod cli_client;
pub mod domain;
pub mod error;
mod scheduler;
mod scheduler_loop;
mod server;
pub(crate) mod state;
mod transport;
mod worker;

use clap::{Args, Parser, Subcommand, error::ErrorKind};
use std::{error::Error, fmt, io::Read, net::SocketAddr, path::PathBuf, process::ExitCode};

const MAX_SPEC_FILE_BYTES: u64 = 1024 * 1024;

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
    Submit(SubmitOptions),
    Status(StatusOptions),
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
    #[arg(long, help = config::HELP_AGENT_ADDRESS)]
    agent_address: Option<String>,
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

#[derive(Debug, Args)]
struct StatusOptions {
    #[command(flatten)]
    client: ClientOptions,
    #[arg(long, help = "Print the job response as JSON")]
    json: bool,
    #[arg(value_name = "JOB_ID")]
    job_id: Option<String>,
}

#[derive(Debug, Args)]
struct SubmitOptions {
    #[command(flatten)]
    client: ClientOptions,
    #[arg(long, value_name = "FILE", conflicts_with = "command_and_args")]
    spec: Option<PathBuf>,
    #[arg(long, help = "Print the created job ID as JSON")]
    json: bool,
    #[arg(
        value_name = "COMMAND",
        required_unless_present = "spec",
        num_args = 1..,
        conflicts_with = "spec",
        trailing_var_arg = true
    )]
    command_and_args: Vec<String>,
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
    launch_exit_code(launch(cli.command, &environment))
}

fn launch_exit_code(result: Result<(), LaunchError>) -> ExitCode {
    match result {
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
            worker::agent::run(config::agent(
                options.server_addr,
                options.agent_address,
                options.heartbeat_interval_secs,
                options.poll_interval_secs,
                options.working_dir,
                environment,
            )?)?;
        }
        Command::Submit(options) => {
            let client_config = config::client(options.client.server_addr, environment)?;
            run_submit(options, client_config)?;
        }
        Command::Status(options) => {
            let client_config = config::client(options.client.server_addr, environment)?;
            run_status(options, client_config)?;
        }
        Command::Stop(options) => {
            launch_mode("stop", config::client(options.server_addr, environment)?);
        }
    }
    Ok(())
}

fn run_submit(
    options: SubmitOptions,
    client_config: config::ClientConfig,
) -> Result<(), LaunchError> {
    let SubmitOptions {
        spec,
        json,
        command_and_args,
        ..
    } = options;
    let request = submit_request(spec, command_and_args)?;
    let client = cli_client::CliHttpClient::new(client_config.server_addr)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(SubmitError::RuntimeBuild)?;
    let response = runtime.block_on(client.create_job(&request))?;
    let job_id = response.job.id;

    if json {
        println!("{}", serde_json::json!({ "job_id": job_id }));
    } else {
        println!("job_id: {job_id}");
    }

    Ok(())
}

fn run_status(
    options: StatusOptions,
    client_config: config::ClientConfig,
) -> Result<(), LaunchError> {
    let job_id = options
        .job_id
        .as_deref()
        .map(str::parse::<domain::JobId>)
        .transpose()
        .map_err(|_| StatusError::InvalidJobId)?;
    let client = cli_client::CliHttpClient::new(client_config.server_addr)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(StatusError::RuntimeBuild)?;

    match job_id {
        Some(job_id) => {
            let response = runtime.block_on(client.get_job(job_id))?;
            if options.json {
                println!("{}", serialize_status_json(&response)?);
            } else {
                print_job_status(&response);
            }
        }
        None => {
            let response = runtime.block_on(client.list_jobs())?;
            if options.json {
                println!("{}", serialize_status_json(&response)?);
            } else {
                print_job_list(&response);
            }
        }
    }

    Ok(())
}

fn serialize_status_json(response: &impl serde::Serialize) -> Result<String, StatusError> {
    serde_json::to_string(response).map_err(StatusError::Json)
}

fn print_job_list(response: &transport::v1::JobListResponse) {
    if response.items.is_empty() {
        println!("no jobs found");
        return;
    }

    println!("jobs: {}", response.items.len());
    for (index, job) in response.items.iter().enumerate() {
        if index > 0 {
            println!();
        }
        print_job_status(job);
    }
}

fn print_job_status(response: &transport::v1::JobResponse) {
    let job = &response.job;
    println!("job_id: {}", job.id);
    println!("status: {}", job_status_label(job.status));
    println!("submitted_at_ms: {}", job.submitted_at.as_unix_millis());
    println!("updated_at_ms: {}", job.updated_at.as_unix_millis());
    print_timestamp("started_at_ms", job.started_at);
    print_timestamp("finished_at_ms", job.finished_at);

    match &response.allocation {
        Some(allocation) => {
            println!("allocation_id: {}", allocation.id);
            println!(
                "allocation_status: {}",
                allocation_status_label(allocation.status)
            );
            println!("agent_id: {}", allocation.agent_id);
            println!("attempt: {}", allocation.attempt);
            println!(
                "assigned_at_ms: {}",
                allocation.assigned_at.as_unix_millis()
            );
            print_timestamp("allocation_started_at_ms", allocation.started_at);
            print_timestamp("allocation_finished_at_ms", allocation.finished_at);
        }
        None => println!("allocation: none"),
    }
}

fn print_timestamp(label: &str, timestamp: Option<domain::Timestamp>) {
    match timestamp {
        Some(timestamp) => println!("{label}: {}", timestamp.as_unix_millis()),
        None => println!("{label}: -"),
    }
}

fn job_status_label(status: transport::v1::JobStatusV1) -> &'static str {
    match status {
        transport::v1::JobStatusV1::Pending => "pending",
        transport::v1::JobStatusV1::Assigned => "assigned",
        transport::v1::JobStatusV1::Running => "running",
        transport::v1::JobStatusV1::Succeeded => "succeeded",
        transport::v1::JobStatusV1::Failed => "failed",
        transport::v1::JobStatusV1::Stopped => "stopped",
        transport::v1::JobStatusV1::Lost => "lost",
    }
}

fn allocation_status_label(status: transport::v1::AllocationStatusV1) -> &'static str {
    match status {
        transport::v1::AllocationStatusV1::Assigned => "assigned",
        transport::v1::AllocationStatusV1::Running => "running",
        transport::v1::AllocationStatusV1::Succeeded => "succeeded",
        transport::v1::AllocationStatusV1::Failed => "failed",
        transport::v1::AllocationStatusV1::Stopped => "stopped",
        transport::v1::AllocationStatusV1::Lost => "lost",
    }
}

fn submit_request(
    spec_file: Option<PathBuf>,
    command_and_args: Vec<String>,
) -> Result<transport::v1::CreateJobRequest, SubmitError> {
    let spec = match spec_file {
        Some(path) => {
            if !command_and_args.is_empty() {
                return Err(SubmitError::ConflictingInput);
            }
            let metadata = std::fs::metadata(&path).map_err(SubmitError::SpecFileRead)?;
            if !metadata.is_file() {
                return Err(SubmitError::SpecFileNotRegular);
            }
            if metadata.len() > MAX_SPEC_FILE_BYTES {
                return Err(SubmitError::SpecFileTooLarge);
            }
            let file = std::fs::File::open(path).map_err(SubmitError::SpecFileRead)?;
            let mut contents = Vec::new();
            file.take(MAX_SPEC_FILE_BYTES + 1)
                .read_to_end(&mut contents)
                .map_err(SubmitError::SpecFileRead)?;
            if contents.len() as u64 > MAX_SPEC_FILE_BYTES {
                return Err(SubmitError::SpecFileTooLarge);
            }
            serde_json::from_slice(&contents).map_err(SubmitError::SpecFileParse)?
        }
        None => {
            let mut values = command_and_args.into_iter();
            let command = values.next().ok_or(SubmitError::MissingInput)?;
            let args = values.collect();
            domain::JobSpec::new(command, args, None, None).map_err(SubmitError::InvalidJobSpec)?
        }
    };

    Ok(transport::v1::CreateJobRequest::from(&spec))
}

#[derive(Debug)]
enum LaunchError {
    Agent(worker::agent::AgentRuntimeError),
    CliClient(cli_client::CliClientError),
    Configuration(config::ConfigError),
    Server(server::ServerError),
    Status(StatusError),
    Submit(SubmitError),
}

enum StatusError {
    InvalidJobId,
    Json(serde_json::Error),
    RuntimeBuild(std::io::Error),
}

impl fmt::Debug for StatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJobId => formatter.write_str("StatusError::InvalidJobId"),
            Self::Json(_) => formatter.write_str("StatusError::Json"),
            Self::RuntimeBuild(_) => formatter.write_str("StatusError::RuntimeBuild"),
        }
    }
}

impl fmt::Display for StatusError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidJobId => formatter.write_str("job ID must be a valid UUID"),
            Self::Json(_) => formatter.write_str("could not serialize status response"),
            Self::RuntimeBuild(_) => formatter.write_str("could not initialize CLI runtime"),
        }
    }
}

impl Error for StatusError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Json(source) => Some(source),
            Self::RuntimeBuild(source) => Some(source),
            Self::InvalidJobId => None,
        }
    }
}

enum SubmitError {
    ConflictingInput,
    InvalidJobSpec(domain::JobSpecError),
    MissingInput,
    RuntimeBuild(std::io::Error),
    SpecFileParse(serde_json::Error),
    SpecFileRead(std::io::Error),
    SpecFileNotRegular,
    SpecFileTooLarge,
}

impl fmt::Debug for SubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConflictingInput => formatter.write_str("SubmitError::ConflictingInput"),
            Self::InvalidJobSpec(error) => formatter
                .debug_tuple("SubmitError::InvalidJobSpec")
                .field(&error.code())
                .finish(),
            Self::MissingInput => formatter.write_str("SubmitError::MissingInput"),
            Self::RuntimeBuild(_) => formatter.write_str("SubmitError::RuntimeBuild"),
            Self::SpecFileParse(_) => formatter.write_str("SubmitError::SpecFileParse"),
            Self::SpecFileRead(_) => formatter.write_str("SubmitError::SpecFileRead"),
            Self::SpecFileNotRegular => formatter.write_str("SubmitError::SpecFileNotRegular"),
            Self::SpecFileTooLarge => formatter.write_str("SubmitError::SpecFileTooLarge"),
        }
    }
}

impl fmt::Display for SubmitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ConflictingInput => {
                formatter.write_str("provide either --spec or a command with arguments")
            }
            Self::InvalidJobSpec(error) => {
                write!(formatter, "invalid job specification: {error}")
            }
            Self::MissingInput => formatter.write_str("provide a command or --spec FILE"),
            Self::RuntimeBuild(_) => formatter.write_str("could not initialize CLI runtime"),
            Self::SpecFileParse(_) => formatter.write_str("job specification file is invalid"),
            Self::SpecFileRead(_) => formatter.write_str("could not read job specification file"),
            Self::SpecFileNotRegular => {
                formatter.write_str("job specification must be a regular file")
            }
            Self::SpecFileTooLarge => {
                formatter.write_str("job specification file exceeds the 1 MiB limit")
            }
        }
    }
}

impl Error for SubmitError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidJobSpec(source) => Some(source),
            Self::RuntimeBuild(source) | Self::SpecFileRead(source) => Some(source),
            Self::SpecFileParse(source) => Some(source),
            Self::ConflictingInput
            | Self::MissingInput
            | Self::SpecFileNotRegular
            | Self::SpecFileTooLarge => None,
        }
    }
}

impl From<worker::agent::AgentRuntimeError> for LaunchError {
    fn from(error: worker::agent::AgentRuntimeError) -> Self {
        Self::Agent(error)
    }
}

impl From<cli_client::CliClientError> for LaunchError {
    fn from(error: cli_client::CliClientError) -> Self {
        Self::CliClient(error)
    }
}

impl From<SubmitError> for LaunchError {
    fn from(error: SubmitError) -> Self {
        Self::Submit(error)
    }
}

impl From<StatusError> for LaunchError {
    fn from(error: StatusError) -> Self {
        Self::Status(error)
    }
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
            Self::Agent(error) => error.fmt(formatter),
            Self::CliClient(error) => error.fmt(formatter),
            Self::Configuration(error) => error.fmt(formatter),
            Self::Server(error) => error.fmt(formatter),
            Self::Status(error) => error.fmt(formatter),
            Self::Submit(error) => error.fmt(formatter),
        }
    }
}

impl Error for LaunchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Agent(error) => Some(error),
            Self::CliClient(error) => Some(error),
            Self::Configuration(error) => Some(error),
            Self::Server(error) => Some(error),
            Self::Status(error) => Some(error),
            Self::Submit(error) => Some(error),
        }
    }
}

fn launch_mode(mode: &str, _configuration: impl std::fmt::Debug) {
    println!("maestro {mode} mode is not implemented yet");
}

#[cfg(test)]
mod cli_exit_tests {
    use super::{LaunchError, launch_exit_code};
    use crate::cli_client::CliClientError;

    #[test]
    fn api_client_error_returns_nonzero_process_exit_code() {
        let error = LaunchError::CliClient(CliClientError::UnexpectedStatus {
            status: reqwest::StatusCode::ACCEPTED,
        });
        assert_eq!(
            launch_exit_code(Err(error)),
            std::process::ExitCode::from(2)
        );
    }
}
