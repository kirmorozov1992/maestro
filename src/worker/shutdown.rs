//! OS signal adapter. Install inside the Tokio runtime before network startup.
use std::{error::Error, fmt, io};

pub(crate) struct ShutdownError(io::Error);
impl ShutdownError {
    pub(super) fn new(source: io::Error) -> Self {
        Self(source)
    }
}
impl fmt::Display for ShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("agent shutdown signal listener failed")
    }
}
impl fmt::Debug for ShutdownError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShutdownError").finish_non_exhaustive()
    }
}
impl Error for ShutdownError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.0)
    }
}

pub(super) struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}
impl ShutdownSignals {
    pub(super) fn install() -> Result<Self, ShutdownError> {
        #[cfg(unix)]
        {
            use tokio::signal::unix::{SignalKind, signal};
            Ok(Self {
                interrupt: signal(SignalKind::interrupt()).map_err(ShutdownError::new)?,
                terminate: signal(SignalKind::terminate()).map_err(ShutdownError::new)?,
            })
        }
        #[cfg(not(unix))]
        {
            Ok(Self {})
        }
    }

    #[cfg(unix)]
    pub(super) async fn wait(mut self) -> Result<(), ShutdownError> {
        let (signal, received) = tokio::select! {
            received = self.interrupt.recv() => ("sigint", received),
            received = self.terminate.recv() => ("sigterm", received),
        };
        received.ok_or_else(|| {
            ShutdownError::new(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "shutdown signal stream closed",
            ))
        })?;
        tracing::info!(event = "shutdown_signal_received", signal);
        Ok(())
    }

    #[cfg(not(unix))]
    pub(super) async fn wait(self) -> Result<(), ShutdownError> {
        // Polled first by startup's biased select, before registration.
        tokio::signal::ctrl_c().await.map_err(ShutdownError::new)?;
        tracing::info!(event = "shutdown_signal_received", signal = "ctrl_c");
        Ok(())
    }
}
