//! Length-delimited framing over an async byte stream (DECISIONS.md decision 22).
//!
//! Messages are serde-encoded with postcard and framed with
//! `tokio_util::codec::LengthDelimitedCodec`. Framing is the only part of the
//! wire format that plain serde does not provide on a continuous byte stream.

use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use serde::{de::DeserializeOwned, Serialize};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

fn invalid_data(e: impl Into<Box<dyn std::error::Error + Send + Sync>>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}

/// A bidirectional, length-delimited, postcard-encoded message connection over
/// any async byte stream: an ssh stdio pair, a local subprocess pipe, or the
/// in-process duplex used by tests.
#[derive(Debug)]
pub struct Connection<S> {
    framed: Framed<S, LengthDelimitedCodec>,
}

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Wrap a byte stream.
    #[must_use]
    pub fn new(stream: S) -> Self {
        Self {
            framed: Framed::new(stream, LengthDelimitedCodec::new()),
        }
    }

    /// Serialize and send one message as a single length-delimited frame.
    ///
    /// # Errors
    /// Returns an error if `msg` fails to serialize or the stream write fails.
    pub async fn send<M: Serialize>(&mut self, msg: &M) -> std::io::Result<()> {
        let bytes = postcard::to_allocvec(msg).map_err(invalid_data)?;
        self.framed.send(Bytes::from(bytes)).await
    }

    /// Receive and decode one message. Returns `Ok(None)` at clean end-of-stream.
    ///
    /// # Errors
    /// Returns an error if the stream read fails or a frame fails to decode.
    pub async fn recv<M: DeserializeOwned>(&mut self) -> std::io::Result<Option<M>> {
        match self.framed.next().await {
            Some(Ok(frame)) => Ok(Some(postcard::from_bytes(&frame).map_err(invalid_data)?)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Connection;
    use crate::protocol::{ToAgent, PROTOCOL_VERSION};
    use bytes::{Bytes, BytesMut};
    use proptest::prelude::*;
    use tokio_util::codec::{Decoder, Encoder, LengthDelimitedCodec};

    #[tokio::test]
    async fn frame_roundtrips() {
        let (a, b) = tokio::io::duplex(1024);
        let mut ca = Connection::new(a);
        let mut cb = Connection::new(b);

        let msg = ToAgent::Hello {
            protocol_version: PROTOCOL_VERSION,
            fn_key: "my_crate::evolve".to_string(),
        };
        ca.send(&msg).await.unwrap();
        let got: ToAgent = cb.recv().await.unwrap().unwrap();
        assert_eq!(got, msg);
    }

    #[tokio::test]
    async fn frame_survives_a_one_byte_buffer() {
        // A 1-byte duplex forces the frame to be split across many reads, so the
        // codec must reassemble a frame from arbitrary stream fragments.
        let (a, b) = tokio::io::duplex(1);
        let mut ca = Connection::new(a);
        let mut cb = Connection::new(b);

        let payload: Vec<u8> = (0u8..=255).cycle().take(300).collect();
        let expected = ToAgent::Assign {
            task_id: 42,
            payload: payload.clone(),
        };
        let writer = tokio::spawn(async move {
            ca.send(&ToAgent::Assign {
                task_id: 42,
                payload,
            })
            .await
            .unwrap();
        });

        let got: ToAgent = cb.recv().await.unwrap().unwrap();
        writer.await.unwrap();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn multiple_frames_arrive_in_order() {
        let (a, b) = tokio::io::duplex(1024);
        let mut ca = Connection::new(a);
        let mut cb = Connection::new(b);

        let msgs = vec![
            ToAgent::Assign {
                task_id: 1,
                payload: vec![1],
            },
            ToAgent::Assign {
                task_id: 2,
                payload: vec![2, 2],
            },
            ToAgent::Shutdown,
        ];
        for m in &msgs {
            ca.send(m).await.unwrap();
        }
        for m in &msgs {
            let got: ToAgent = cb.recv().await.unwrap().unwrap();
            assert_eq!(&got, m);
        }
    }

    #[tokio::test]
    async fn recv_returns_none_at_eof() {
        let (a, b) = tokio::io::duplex(64);
        let ca = Connection::new(a);
        let mut cb = Connection::new(b);
        drop(ca); // close the writer end
        let got: Option<ToAgent> = cb.recv().await.unwrap();
        assert_eq!(got, None);
    }

    proptest! {
        /// An arbitrary payload, framed once, then fed to a fresh decoder in
        /// arbitrary-sized chunks, must reassemble into exactly that one frame.
        #[test]
        fn framing_reassembles_under_arbitrary_chunking(
            payload in proptest::collection::vec(any::<u8>(), 0..1000),
            chunk in 1usize..40,
        ) {
            let mut encoder = LengthDelimitedCodec::new();
            let mut framed = BytesMut::new();
            encoder.encode(Bytes::from(payload.clone()), &mut framed).unwrap();

            let mut decoder = LengthDelimitedCodec::new();
            let mut buf = BytesMut::new();
            let mut frames: Vec<Vec<u8>> = Vec::new();
            for piece in framed.chunks(chunk) {
                buf.extend_from_slice(piece);
                while let Some(frame) = decoder.decode(&mut buf).unwrap() {
                    frames.push(frame.to_vec());
                }
            }
            prop_assert_eq!(frames, vec![payload]);
        }
    }
}
