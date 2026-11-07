//! Bounded binary stream capture. Never format or serialize the captured bytes.

use std::{fmt, io, sync::Arc};
use tokio::io::{AsyncRead, AsyncReadExt};

pub(super) const OUTPUT_LIMIT_BYTES: usize = 64 * 1024;
const READ_CHUNK_BYTES: usize = 8 * 1024;

pub(crate) struct CapturedOutput {
    bytes: Vec<u8>,
    truncated: bool,
    drain_timed_out: bool,
}

impl CapturedOutput {
    /// The collected prefix. It may be partial until wait/cleanup completes.
    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) fn is_truncated(&self) -> bool {
        self.truncated
    }

    pub(crate) fn drain_timed_out(&self) -> bool {
        self.drain_timed_out
    }
}

impl fmt::Debug for CapturedOutput {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CapturedOutput")
            .field("captured_bytes", &self.bytes.len())
            .field("truncated", &self.truncated)
            .field("drain_timed_out", &self.drain_timed_out)
            .finish()
    }
}

pub(super) struct BoundedCapture<Reader> {
    reader: Option<Reader>,
    output: CapturedOutput,
    error: Option<Arc<io::Error>>,
}

impl<Reader> BoundedCapture<Reader> {
    pub(super) fn new(reader: Option<Reader>) -> Self {
        Self {
            reader,
            output: CapturedOutput {
                bytes: Vec::with_capacity(OUTPUT_LIMIT_BYTES),
                truncated: false,
                drain_timed_out: false,
            },
            error: None,
        }
    }

    pub(super) fn output(&self) -> &CapturedOutput {
        &self.output
    }

    pub(super) fn error(&self) -> Option<&Arc<io::Error>> {
        self.error.as_ref()
    }

    pub(super) fn close_on_timeout(&mut self) {
        if self.reader.take().is_some() {
            self.output.truncated = true;
            self.output.drain_timed_out = true;
        }
    }
}

impl<Reader: AsyncRead + Unpin> BoundedCapture<Reader> {
    pub(super) async fn drain(&mut self) {
        let mut chunk = [0; READ_CHUNK_BYTES];
        while let Some(reader) = self.reader.as_mut() {
            match reader.read(&mut chunk).await {
                Ok(0) => {
                    self.reader = None;
                    break;
                }
                Ok(count) => {
                    let remaining = OUTPUT_LIMIT_BYTES - self.output.bytes.len();
                    let kept = remaining.min(count);
                    self.output.bytes.extend_from_slice(&chunk[..kept]);
                    self.output.truncated |= kept < count;
                    // Continue draining even when no capacity remains. Closing
                    // or ignoring this pipe could block the child or cause SIGPIPE.
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => {
                    self.error = Some(Arc::new(error));
                    self.output.truncated = true;
                    self.reader = None;
                    break;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedCapture, OUTPUT_LIMIT_BYTES};
    use crate::worker::executor::ExecutorError;
    use std::{
        error::Error,
        io,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };
    use tokio::io::{AsyncRead, ReadBuf};

    #[tokio::test]
    async fn keeps_raw_prefix_with_exact_and_independent_limit_boundaries() {
        for size in [
            0,
            OUTPUT_LIMIT_BYTES - 1,
            OUTPUT_LIMIT_BYTES,
            OUTPUT_LIMIT_BYTES + 1,
            OUTPUT_LIMIT_BYTES * 4,
        ] {
            let bytes: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            let mut capture = BoundedCapture::new(Some(bytes.as_slice()));
            capture.drain().await;
            assert_eq!(
                capture.output().bytes(),
                &bytes[..size.min(OUTPUT_LIMIT_BYTES)]
            );
            assert_eq!(capture.output().is_truncated(), size > OUTPUT_LIMIT_BYTES);
            assert!(!capture.output().drain_timed_out());
            assert!(capture.error().is_none());
            capture.drain().await;
            assert_eq!(capture.output().bytes().len(), size.min(OUTPUT_LIMIT_BYTES));
        }
    }

    struct ErrorReader {
        stage: usize,
        interrupted: bool,
    }

    impl AsyncRead for ErrorReader {
        fn poll_read(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
            buffer: &mut ReadBuf<'_>,
        ) -> Poll<io::Result<()>> {
            self.stage += 1;
            match self.stage {
                1 if self.interrupted => {
                    Poll::Ready(Err(io::Error::from(io::ErrorKind::Interrupted)))
                }
                1 | 2 if self.stage == 1 || self.interrupted => {
                    buffer.put_slice(b"private-output\xff\x00");
                    Poll::Ready(Ok(()))
                }
                _ if self.interrupted => Poll::Ready(Ok(())),
                _ => Poll::Ready(Err(io::Error::other("private-read-error"))),
            }
        }
    }

    #[tokio::test]
    async fn retries_interrupted_read_without_losing_binary_bytes() {
        let mut capture = BoundedCapture::new(Some(ErrorReader {
            stage: 0,
            interrupted: true,
        }));
        capture.drain().await;
        assert_eq!(capture.output().bytes(), b"private-output\xff\x00");
        assert!(!capture.output().is_truncated());
        assert!(capture.error().is_none());
    }

    #[tokio::test]
    async fn preserves_read_error_and_prefix_without_leaking_them_in_debug() {
        let mut capture = BoundedCapture::new(Some(ErrorReader {
            stage: 0,
            interrupted: false,
        }));
        capture.drain().await;
        assert_eq!(capture.output().bytes(), b"private-output\xff\x00");
        assert!(capture.output().is_truncated());
        assert!(!capture.output().drain_timed_out());
        let source = Arc::clone(capture.error().unwrap());
        capture.drain().await;
        assert!(Arc::ptr_eq(&source, capture.error().unwrap()));
        for error in [
            ExecutorError::Stdout(Arc::clone(&source)),
            ExecutorError::Stderr(source),
        ] {
            assert!(
                error
                    .source()
                    .unwrap()
                    .to_string()
                    .contains("private-read-error")
            );
            assert!(!format!("{error} {error:?} {:?}", capture.output()).contains("private"));
        }
    }

    #[tokio::test]
    async fn timeout_marks_only_an_open_stream_and_preserves_its_prefix() {
        let bytes = b"private-output";
        let mut completed = BoundedCapture::new(Some(bytes.as_slice()));
        completed.drain().await;
        completed.close_on_timeout();
        assert_eq!(completed.output().bytes(), bytes);
        assert!(!completed.output().is_truncated());
        assert!(!completed.output().drain_timed_out());
        let mut pending = BoundedCapture::new(Some(bytes.as_slice()));
        pending.close_on_timeout();
        assert!(pending.output().is_truncated());
        assert!(pending.output().drain_timed_out());
        pending.drain().await;
        assert!(pending.output().bytes().is_empty());
    }
}
