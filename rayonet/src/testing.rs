//! In-process transport helpers for tests (PLAN.md Phase 0).
//!
//! These wire two `Connection`s together over a `tokio` duplex pipe, using the
//! exact framing the real ssh transport uses, plus a fault injector that severs
//! a stream at a chosen byte offset to drive the drop/requeue tests in later
//! phases. (Public for now so integration tests can reach it; a `testing`
//! feature gate is a Phase 7 hardening item.)

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{duplex, AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

use crate::framing::Connection;

/// Create a connected pair of in-process connections over a `tokio` duplex pipe
/// of the given per-direction buffer size. Small buffers force fragmentation.
#[must_use]
pub fn connection_pair(max_buf: usize) -> (Connection<DuplexStream>, Connection<DuplexStream>) {
    let (a, b) = duplex(max_buf);
    (Connection::new(a), Connection::new(b))
}

/// Wraps a byte stream and severs its read side after a fixed number of bytes,
/// after which reads report a clean end-of-stream. Writes pass through. Used to
/// simulate a host or link dropping mid-task.
#[derive(Debug)]
pub struct FaultInjector<S> {
    inner: S,
    read_budget: usize,
}

impl<S> FaultInjector<S> {
    /// Allow `bytes` more bytes to be read, then sever the read side (EOF).
    #[must_use]
    pub const fn cut_reads_after(inner: S, bytes: usize) -> Self {
        Self {
            inner,
            read_budget: bytes,
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
            // Severed: report a clean end-of-stream.
            return Poll::Ready(Ok(()));
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

#[cfg(test)]
mod tests {
    use super::{connection_pair, FaultInjector};
    use crate::framing::Connection;
    use crate::protocol::ToAgent;

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
        // A mid-frame cut must never yield the message; it is EOF or an error.
        assert!(!matches!(res, Ok(Some(_))));
    }
}
