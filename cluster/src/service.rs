use crate::ring::NodeId;

/// Events emitted by the membership service when cluster state changes.
#[derive(Debug, Clone)]
pub enum MembershipEvent {
    /// A new node joined the cluster.
    Joined(NodeId),
    /// A node left the cluster (or its lease expired).
    Left(NodeId),
    /// Full membership snapshot (used for reconciliation on reconnect).
    Snapshot(Vec<NodeId>),
}

/// Errors from membership operations.
#[derive(Debug, thiserror::Error)]
pub enum MembershipError {
    #[error("connection failed: {0}")]
    ConnectionFailed(String),
    #[error("lease expired")]
    LeaseExpired,
    #[error("timeout")]
    Timeout,
    #[error("{0}")]
    Internal(String),
}

/// Extensible membership service trait.
///
/// Implementations manage how nodes discover each other and track liveness.
/// Current implementations: `StaticMembership`, `EtcdMembership`.
/// Future: `RaftMembership`.
///
/// Methods are synchronous (not async) because:
/// - `join`/`leave` are called from the main thread before monoio starts
/// - The etcd impl runs blocking tokio calls on a background thread
/// - This avoids `Send` bound issues with monoio's `!Send` futures
pub trait MembershipService: Send + Sync {
    /// Register this node with the cluster.
    fn join(&self) -> Result<(), MembershipError>;

    /// Deregister this node from the cluster.
    fn leave(&self) -> Result<(), MembershipError>;

    /// Get all currently live (heartbeat-active) members.
    fn get_live_members(&self) -> Result<Vec<NodeId>, MembershipError>;

    /// Get all registered members, including those that may have failed.
    /// Default: same as `get_live_members()`.
    fn get_all_members(&self) -> Result<Vec<NodeId>, MembershipError> {
        self.get_live_members()
    }

    /// Subscribe to membership change events.
    /// Returns a receiver; events are pushed from a background thread.
    /// Multiple calls return independent receivers.
    fn subscribe(&self) -> std::sync::mpsc::Receiver<MembershipEvent>;

    /// This node's identity.
    fn self_id(&self) -> &NodeId;
}
