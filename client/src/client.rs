use std::cell::Cell;
use std::rc::Rc;
use std::sync::mpsc;
use std::time::Duration;

use bytes::Bytes;

use ruxio_cluster::ring::NodeId;
use ruxio_cluster::service::MembershipEvent;
use ruxio_protocol::frame::{Frame, MessageType};
use ruxio_protocol::messages::{GetMetadataRequest, ReadRangeRequest, ScanRequest};
use ruxio_protocol::predicate::PredicateExpr;

use crate::config::{ClientConfig, MembershipConfig};
use crate::connection::ConnectionPool;
use crate::error::{self, UnexpectedResponseSnafu};
use crate::membership::ClientMembership;
use crate::response::{MetadataResult, Response};
use crate::routing::route_and_execute;

/// Client-side reader for a Ruxio distributed Parquet cache cluster.
///
/// Builds a consistent hash ring from the cluster membership and routes
/// each request directly to the owning server by file path. The ring
/// is refreshed automatically on a configurable interval and immediately
/// on receiving a Redirect (stale ring signal).
///
/// Each monoio worker thread should create its own `RuxioClient` instance.
/// The client is `!Send` (uses `Rc`/`RefCell` internally).
pub struct RuxioClient {
    membership: Rc<ClientMembership>,
    pool: Rc<ConnectionPool>,
    config: ClientConfig,
    next_request_id: Cell<u32>,
}

impl RuxioClient {
    /// Create a client with static membership (fixed list of server addresses).
    ///
    /// Panics if `config.membership` is not `MembershipConfig::Static`.
    pub fn with_static(config: ClientConfig) -> Self {
        let servers = match &config.membership {
            MembershipConfig::Static { servers } => servers.clone(),
            _ => panic!("with_static requires MembershipConfig::Static"),
        };

        let nodes: Vec<NodeId> = servers.iter().map(|s| NodeId(s.clone())).collect();
        let (_tx, rx) = mpsc::channel();

        let membership = Rc::new(ClientMembership::new(
            nodes,
            config.vnodes_per_node,
            rx,
            Duration::from_secs(config.membership_refresh_secs),
        ));
        let pool = Rc::new(ConnectionPool::new(
            config.connect_timeout,
            config.read_timeout,
        ));

        Self {
            membership,
            pool,
            config,
            next_request_id: Cell::new(1),
        }
    }

    /// Create a client with etcd-based discovery.
    ///
    /// Discovers servers from etcd and watches for membership changes.
    /// The client does NOT register itself — it is a passive observer.
    /// The hash ring refreshes automatically via the event channel.
    pub fn with_etcd(
        initial_members: Vec<NodeId>,
        event_rx: mpsc::Receiver<MembershipEvent>,
        config: ClientConfig,
    ) -> Self {
        let membership = Rc::new(ClientMembership::new(
            initial_members,
            config.vnodes_per_node,
            event_rx,
            Duration::from_secs(config.membership_refresh_secs),
        ));
        let pool = Rc::new(ConnectionPool::new(
            config.connect_timeout,
            config.read_timeout,
        ));

        Self {
            membership,
            pool,
            config,
            next_request_id: Cell::new(1),
        }
    }

    /// Read a byte range from a cached Parquet file.
    ///
    /// The URI is hashed to determine the owning server. On redirect
    /// (stale ring), the ring is refreshed and the request retried.
    pub async fn read_range(&self, uri: &str, offset: u64, length: u64) -> error::Result<Bytes> {
        let req = ReadRangeRequest {
            uri: uri.to_string(),
            offset,
            length,
        };
        let frame = Frame::new_json_unchecked(MessageType::ReadRange, self.next_id(), &req);

        match route_and_execute(
            uri,
            frame,
            &self.membership,
            &self.pool,
            self.config.max_retries,
        )
        .await?
        {
            Response::Data(data) => Ok(data),
            Response::Error {
                error_code,
                message,
            } => Err(crate::error::ServerSnafu {
                error_code,
                message,
            }
            .build()),
            other => Err(UnexpectedResponseSnafu {
                msg_type: other.msg_type(),
            }
            .build()),
        }
    }

    /// Get file metadata (Parquet footer info).
    pub async fn get_metadata(&self, uri: &str) -> error::Result<MetadataResult> {
        let req = GetMetadataRequest {
            uri: uri.to_string(),
        };
        let frame = Frame::new_json_unchecked(MessageType::GetMetadata, self.next_id(), &req);

        match route_and_execute(
            uri,
            frame,
            &self.membership,
            &self.pool,
            self.config.max_retries,
        )
        .await?
        {
            Response::Metadata(meta) => Ok(meta),
            Response::Error {
                error_code,
                message,
            } => Err(crate::error::ServerSnafu {
                error_code,
                message,
            }
            .build()),
            other => Err(UnexpectedResponseSnafu {
                msg_type: other.msg_type(),
            }
            .build()),
        }
    }

    /// Scan with predicate pushdown.
    ///
    /// Returns matching data from the server. The predicate is evaluated
    /// against Parquet row group statistics on the server side.
    pub async fn scan(
        &self,
        uri: &str,
        predicate: Option<PredicateExpr>,
        projection: Option<Vec<String>>,
    ) -> error::Result<Bytes> {
        let req = ScanRequest {
            uri: uri.to_string(),
            predicate,
            projection,
        };
        let frame = Frame::new_json_unchecked(MessageType::Scan, self.next_id(), &req);

        match route_and_execute(
            uri,
            frame,
            &self.membership,
            &self.pool,
            self.config.max_retries,
        )
        .await?
        {
            Response::Data(data) => Ok(data),
            other => Err(UnexpectedResponseSnafu {
                msg_type: other.msg_type(),
            }
            .build()),
        }
    }

    fn next_id(&self) -> u32 {
        let id = self.next_request_id.get();
        self.next_request_id.set(id.wrapping_add(1));
        id
    }
}
