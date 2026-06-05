//! In-process transport helpers for tests (PLAN.md Phase 0).
//!
//! These wire two `Connection`s together over a `tokio` duplex pipe, using the
//! exact framing the real ssh transport uses, plus a fault injector that severs
//! a stream at a chosen byte offset to drive the drop/requeue tests in later
//! phases. (Public for now so integration tests can reach it; a `testing`
//! feature gate is a Phase 7 hardening item.)

use std::pin::Pin;
use std::sync::{Mutex, PoisonError};
use std::task::{Context, Poll};

use tokio::io::{duplex, AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

use crate::framing::Connection;
use crate::observability::{Event, EventSink, NodeState};

/// Collects the observability event stream so a test can assert what a run
/// emitted: the full sequence via [`events`](Self::events), or just the
/// node-state transitions via [`states`](Self::states).
#[derive(Debug, Default)]
pub struct EventRecorder {
    events: Mutex<Vec<Event>>,
}

impl EventRecorder {
    /// Every event emitted so far, in order.
    #[must_use]
    pub fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Just the node-state transitions, in order.
    #[must_use]
    pub fn states(&self) -> Vec<NodeState> {
        self.events()
            .into_iter()
            .filter_map(|event| match event {
                Event::Node { state, .. } => Some(state),
                _ => None,
            })
            .collect()
    }
}

impl EventSink for EventRecorder {
    fn emit(&self, event: Event) {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event);
    }
}

/// Create a connected pair of in-process connections over a `tokio` duplex pipe
/// of the given per-direction buffer size. Small buffers force fragmentation.
#[must_use]
pub fn connection_pair(max_buf: usize) -> (Connection<DuplexStream>, Connection<DuplexStream>) {
    let (a, b) = duplex(max_buf);
    (Connection::new(a), Connection::new(b))
}

/// How a severed read reports: a clean end-of-stream, or an error.
#[derive(Debug, Clone, Copy)]
enum OnCut {
    Eof,
    Error,
}

/// Wraps a byte stream and severs its read side after a fixed number of bytes.
/// Writes pass through. Used to simulate a host or link dropping mid-task.
#[derive(Debug)]
pub struct FaultInjector<S> {
    inner: S,
    read_budget: usize,
    on_cut: OnCut,
}

impl<S> FaultInjector<S> {
    /// Allow `bytes` more bytes to be read, then sever with a clean EOF.
    #[must_use]
    pub const fn cut_reads_after(inner: S, bytes: usize) -> Self {
        Self {
            inner,
            read_budget: bytes,
            on_cut: OnCut::Eof,
        }
    }

    /// Allow `bytes` more bytes to be read, then fail every read with an error.
    #[must_use]
    pub const fn error_reads_after(inner: S, bytes: usize) -> Self {
        Self {
            inner,
            read_budget: bytes,
            on_cut: OnCut::Error,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for FaultInjector<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();
        if this.read_budget == 0 {
            return match this.on_cut {
                OnCut::Eof => Poll::Ready(Ok(())),
                OnCut::Error => Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "injected read fault",
                ))),
            };
        }
        let max = this.read_budget.min(buf.remaining());
        let mut scratch = vec![0u8; max];
        let mut limited = ReadBuf::new(&mut scratch);
        match Pin::new(&mut this.inner).poll_read(cx, &mut limited) {
            Poll::Ready(Ok(())) => {
                let filled = limited.filled();
                buf.put_slice(filled);
                this.read_budget -= filled.len();
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for FaultInjector<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().inner).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// A value whose serialization always fails, for exercising encode-error paths.
#[derive(Debug)]
pub struct FailsToSerialize;

impl serde::Serialize for FailsToSerialize {
    fn serialize<S: serde::Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("serialization always fails"))
    }
}

#[cfg(test)]
mod tests {
    use super::{connection_pair, FaultInjector};
    use crate::framing::Connection;
    use crate::protocol::ToAgent;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn connection_pair_roundtrips() {
        let (mut tx, mut rx) = connection_pair(64);
        let msg = ToAgent::Shutdown;
        tx.send(&msg).await.unwrap();
        let got: ToAgent = rx.recv().await.unwrap().unwrap();
        assert_eq!(got, msg);
    }

    #[tokio::test]
    async fn fault_injector_passes_through_under_budget() {
        let (w, r) = tokio::io::duplex(1024);
        let mut tx = Connection::new(w);
        let mut rx = Connection::new(FaultInjector::cut_reads_after(r, 100_000));

        let msg = ToAgent::Assign {
            task_id: 5,
            payload: vec![9; 50],
        };
        tx.send(&msg).await.unwrap();
        let got: ToAgent = rx.recv().await.unwrap().unwrap();
        assert_eq!(got, msg);
    }

    #[tokio::test]
    async fn fault_injector_cut_mid_frame_prevents_delivery() {
        let (w, r) = tokio::io::duplex(1024);
        let mut tx = Connection::new(w);
        let mut rx = Connection::new(FaultInjector::cut_reads_after(r, 2));

        tx.send(&ToAgent::Hello {
            protocol_version: 1,
            fn_key: "x".to_string(),
        })
        .await
        .unwrap();

        let res: std::io::Result<Option<ToAgent>> = rx.recv().await;
        // A frame cut after 2 bytes (mid length-prefix) is a truncated-stream error.
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn error_mode_surfaces_an_error() {
        let (_w, r) = tokio::io::duplex(64);
        let mut rx = Connection::new(FaultInjector::error_reads_after(r, 0));
        let res: std::io::Result<Option<ToAgent>> = rx.recv().await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn write_side_passes_through() {
        let (w, mut r) = tokio::io::duplex(64);
        let mut fi = FaultInjector::cut_reads_after(w, 1_000);
        fi.write_all(b"hello").await.unwrap();
        fi.flush().await.unwrap();
        fi.shutdown().await.unwrap();

        let mut buf = [0u8; 5];
        r.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"hello");
    }

    #[test]
    fn debug_renders() {
        let (w, _r) = tokio::io::duplex(8);
        let fi = FaultInjector::cut_reads_after(w, 4);
        assert!(format!("{fi:?}").contains("FaultInjector"));
    }

    #[tokio::test]
    async fn read_pends_when_inner_has_no_data() {
        use futures::FutureExt;
        // Keep the writer alive (so it is not EOF) but write nothing: the inner
        // read returns Pending, exercising the pass-through arm of poll_read.
        let (_w, r) = tokio::io::duplex(64);
        let mut fi = FaultInjector::cut_reads_after(r, 100);
        let mut buf = [0u8; 4];
        assert!(fi.read(&mut buf).now_or_never().is_none());
    }
}
