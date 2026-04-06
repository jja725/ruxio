use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;

use bytes::BytesMut;
use monoio::io::{AsyncReadRent, AsyncWriteRentExt};
use monoio::net::TcpStream;

use ruxio_cluster::ring::NodeId;
use ruxio_protocol::frame::{Frame, FrameReader, MessageType};
use ruxio_protocol::messages::{ErrorResponse, MetadataResponse};

use crate::error::{
    self, ConnectionFailedSnafu, DeserializationSnafu, FrameSnafu, ReadFailedSnafu, TimeoutSnafu,
    UnexpectedResponseSnafu, WriteFailedSnafu,
};
use crate::response::{MetadataResult, Response};

use snafu::{IntoError, ResultExt};

/// Receive buffer size for reading response frames.
const RECV_BUFFER_BYTES: usize = 128 * 1024;

/// A single TCP connection to a Ruxio server.
pub(crate) struct Connection {
    stream: TcpStream,
    reader: FrameReader,
    read_timeout: Duration,
    /// Reusable read buffer — avoids 128KB allocation per request.
    recv_buf: Vec<u8>,
}

impl Connection {
    /// Connect to a server with timeout.
    pub async fn connect(
        addr: &str,
        connect_timeout: Duration,
        read_timeout: Duration,
    ) -> error::Result<Self> {
        let stream = monoio::time::timeout(connect_timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| {
                TimeoutSnafu {
                    timeout: connect_timeout,
                }
                .build()
            })?
            .map_err(|e| {
                ConnectionFailedSnafu {
                    addr: addr.to_string(),
                }
                .into_error(e)
            })?;

        if let Err(e) = stream.set_nodelay(true) {
            tracing::debug!("set_nodelay failed: {e}");
        }

        Ok(Self {
            stream,
            reader: FrameReader::new(),
            read_timeout,
            recv_buf: vec![0u8; RECV_BUFFER_BYTES],
        })
    }

    /// Send a request frame and collect the full response.
    ///
    /// Reads DataChunk frames until a Done, Error, Redirect, or Metadata
    /// frame terminates the exchange.
    pub async fn send_request(&mut self, frame: Frame) -> error::Result<Response> {
        // Send — encode returns Bytes, write_all takes ownership (no .to_vec() copy)
        let encoded = frame.encode().to_vec();
        let (result, _) = self.stream.write_all(encoded).await;
        result.context(WriteFailedSnafu)?;

        // Collect response using connection-owned buffer
        let mut data = BytesMut::new();
        let mut buf = std::mem::take(&mut self.recv_buf);

        loop {
            let read_result = monoio::time::timeout(self.read_timeout, self.stream.read(buf)).await;

            let (result, returned_buf) = match read_result {
                Ok(inner) => inner,
                Err(_) => {
                    return Err(TimeoutSnafu {
                        timeout: self.read_timeout,
                    }
                    .build());
                }
            };
            buf = returned_buf;

            let n = result.context(ReadFailedSnafu)?;
            if n == 0 {
                return Err(ConnectionFailedSnafu {
                    addr: "server".to_string(),
                }
                .into_error(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server closed connection",
                )));
            }

            self.reader.feed(&buf[..n]);

            while let Some(resp) = self.reader.next_frame().context(FrameSnafu)? {
                match resp.msg_type {
                    MessageType::DataChunk => {
                        data.extend_from_slice(&resp.payload);
                    }
                    MessageType::Done => {
                        self.recv_buf = buf;
                        return Ok(Response::Data(data.freeze()));
                    }
                    MessageType::Error => {
                        self.recv_buf = buf;
                        let err: ErrorResponse =
                            serde_json::from_slice(&resp.payload).context(DeserializationSnafu)?;
                        return Ok(Response::Error {
                            error_code: err.error_code,
                            message: err.message,
                        });
                    }
                    MessageType::Metadata => {
                        self.recv_buf = buf;
                        let meta: MetadataResponse =
                            serde_json::from_slice(&resp.payload).context(DeserializationSnafu)?;
                        return Ok(Response::Metadata(MetadataResult {
                            uri: meta.uri,
                            file_size: meta.file_size,
                            footer_size: meta.footer_size,
                        }));
                    }
                    other => {
                        return Err(UnexpectedResponseSnafu { msg_type: other }.build());
                    }
                }
            }
        }
    }
}

/// Pool of connections to Ruxio servers, one per node.
///
/// Connections are lazily established and reconnected on failure.
/// Thread-local — not `Send`/`Sync`.
pub(crate) struct ConnectionPool {
    connections: RefCell<HashMap<NodeId, Option<Connection>>>,
    connect_timeout: Duration,
    read_timeout: Duration,
}

impl ConnectionPool {
    pub fn new(connect_timeout: Duration, read_timeout: Duration) -> Self {
        Self {
            connections: RefCell::new(HashMap::new()),
            connect_timeout,
            read_timeout,
        }
    }

    /// Send a request to the given node, establishing a connection if needed.
    pub async fn send_to(&self, node: &NodeId, frame: Frame) -> error::Result<Response> {
        // Get or create connection
        let needs_connect = {
            let conns = self.connections.borrow();
            conns.get(node).is_none_or(|c| c.is_none())
        };

        if needs_connect {
            let conn =
                Connection::connect(&node.0, self.connect_timeout, self.read_timeout).await?;
            self.connections
                .borrow_mut()
                .insert(node.clone(), Some(conn));
        }

        // Send request — take the connection out to get mutable access
        let mut conn = self
            .connections
            .borrow_mut()
            .get_mut(node)
            .and_then(|c| c.take())
            .ok_or_else(|| {
                ConnectionFailedSnafu {
                    addr: node.0.clone(),
                }
                .into_error(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "connection unavailable after connect",
                ))
            })?;

        let result = conn.send_request(frame).await;

        // Put it back if the request succeeded (connection still alive)
        if result.is_ok() {
            self.connections
                .borrow_mut()
                .insert(node.clone(), Some(conn));
        } else {
            // Connection is dead — will reconnect on next use
            self.connections.borrow_mut().insert(node.clone(), None);
        }

        result
    }

    /// Mark a connection as dead — will reconnect on next use.
    pub fn invalidate(&self, node: &NodeId) {
        self.connections.borrow_mut().insert(node.clone(), None);
    }
}
