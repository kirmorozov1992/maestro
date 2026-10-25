use crate::scheduler::SchedulerError;
use std::{
    error::Error,
    fmt, io,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

pub(crate) struct SchedulerLoop {
    interval: Duration,
    stopping: Arc<AtomicBool>,
    wake: Arc<(Mutex<()>, Condvar)>,
    worker: Option<JoinHandle<()>>,
}

#[derive(Debug)]
pub(crate) enum SchedulerLoopError {
    ZeroInterval,
    AlreadyRunning,
    ThreadSpawn(io::Error),
    WorkerPanicked,
}

impl SchedulerLoop {
    pub(crate) fn new(interval: Duration) -> Result<Self, SchedulerLoopError> {
        if interval.is_zero() {
            return Err(SchedulerLoopError::ZeroInterval);
        }
        Ok(Self {
            interval,
            stopping: Arc::new(AtomicBool::new(false)),
            wake: Arc::new((Mutex::new(()), Condvar::new())),
            worker: None,
        })
    }

    pub(crate) fn start<Tick, OnError>(
        &mut self,
        mut tick: Tick,
        mut on_error: OnError,
    ) -> Result<(), SchedulerLoopError>
    where
        Tick: FnMut() -> Result<(), SchedulerError> + Send + 'static,
        OnError: FnMut(SchedulerError) + Send + 'static,
    {
        if self.worker.is_some() {
            return Err(SchedulerLoopError::AlreadyRunning);
        }

        self.stopping.store(false, Ordering::Release);
        let stopping = Arc::clone(&self.stopping);
        let wake = Arc::clone(&self.wake);
        let interval = self.interval;
        self.worker = Some(
            thread::Builder::new()
                .name("maestro-scheduler".into())
                .spawn(move || {
                    while !stopping.load(Ordering::Acquire) {
                        if let Err(error) = tick() {
                            on_error(error);
                        }

                        let (wait_lock, condition) = &*wake;
                        let guard = wait_lock
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if stopping.load(Ordering::Acquire) {
                            break;
                        }
                        let (guard, _) = condition
                            .wait_timeout_while(guard, interval, |_| {
                                !stopping.load(Ordering::Acquire)
                            })
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        drop(guard);
                    }
                })
                .map_err(SchedulerLoopError::ThreadSpawn)?,
        );
        Ok(())
    }

    pub(crate) fn stop(&mut self) -> Result<(), SchedulerLoopError> {
        self.request_stop();
        let Some(worker) = self.worker.take() else {
            return Ok(());
        };
        worker
            .join()
            .map_err(|_| SchedulerLoopError::WorkerPanicked)
    }

    fn request_stop(&self) {
        self.stopping.store(true, Ordering::Release);
        let (wait_lock, condition) = &*self.wake;
        let _guard = wait_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        condition.notify_all();
    }
}

impl Drop for SchedulerLoop {
    fn drop(&mut self) {
        self.request_stop();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl fmt::Display for SchedulerLoopError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroInterval => formatter.write_str("scheduler interval must be positive"),
            Self::AlreadyRunning => formatter.write_str("scheduler loop is already running"),
            Self::ThreadSpawn(_) => formatter.write_str("scheduler loop thread could not start"),
            Self::WorkerPanicked => formatter.write_str("scheduler loop worker panicked"),
        }
    }
}

impl Error for SchedulerLoopError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ThreadSpawn(source) => Some(source),
            Self::ZeroInterval | Self::AlreadyRunning | Self::WorkerPanicked => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{SchedulerLoop, SchedulerLoopError};
    use crate::{scheduler::SchedulerError, state::error::StateStoreError};
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
            mpsc,
        },
        time::{Duration, Instant},
    };

    #[test]
    fn zero_interval_is_rejected() {
        assert!(matches!(
            SchedulerLoop::new(Duration::ZERO),
            Err(SchedulerLoopError::ZeroInterval)
        ));
    }

    #[test]
    fn second_start_on_one_controller_is_rejected() {
        let mut scheduler_loop = SchedulerLoop::new(Duration::from_secs(1)).unwrap();
        scheduler_loop
            .start(|| Ok::<(), SchedulerError>(()), |_| {})
            .unwrap();

        assert!(matches!(
            scheduler_loop.start(|| Ok::<(), SchedulerError>(()), |_| {}),
            Err(SchedulerLoopError::AlreadyRunning)
        ));
        scheduler_loop.stop().unwrap();
    }

    #[test]
    fn tick_error_is_reported_and_later_tick_still_runs() {
        let mut scheduler_loop = SchedulerLoop::new(Duration::from_millis(10)).unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let tick_calls = Arc::clone(&calls);
        let (error_sender, error_receiver) = mpsc::channel();
        let (success_sender, success_receiver) = mpsc::channel();

        scheduler_loop
            .start(
                move || {
                    if tick_calls.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(SchedulerError::StateStore(StateStoreError::LockPoisoned))
                    } else {
                        success_sender.send(()).unwrap();
                        Ok(())
                    }
                },
                move |error| error_sender.send(error).unwrap(),
            )
            .unwrap();

        assert_eq!(
            error_receiver.recv_timeout(Duration::from_secs(1)).unwrap(),
            SchedulerError::StateStore(StateStoreError::LockPoisoned)
        );
        success_receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap();
        scheduler_loop.stop().unwrap();
        assert!(calls.load(Ordering::SeqCst) >= 2);
    }

    #[test]
    fn stop_wakes_interval_wait_and_joins_worker() {
        let mut scheduler_loop = SchedulerLoop::new(Duration::from_secs(2)).unwrap();
        let (tick_sender, tick_receiver) = mpsc::channel();
        scheduler_loop
            .start(
                move || {
                    tick_sender.send(()).unwrap();
                    Ok::<(), SchedulerError>(())
                },
                |_| {},
            )
            .unwrap();
        tick_receiver.recv_timeout(Duration::from_secs(1)).unwrap();

        let stop_started_at = Instant::now();
        scheduler_loop.stop().unwrap();

        assert!(stop_started_at.elapsed() < Duration::from_secs(1));
    }
}
